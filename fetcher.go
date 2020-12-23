package crawler

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"golang.org/x/time/rate"
)

var (
	ErrAlreadyCrawledRecently = errors.New("error: already crawled recently")
)

type Fetcher struct {
	store           *Store
	limitDuration   time.Duration
	timeoutDuration time.Duration
	minInterval     time.Duration
	domainLimiter   map[string]*rate.Limiter
	mu              sync.Mutex
}

func NewFetcher(store *Store, timeoutDuration time.Duration, limitDuration time.Duration, minInterval time.Duration) (*Fetcher, error) {
	m := make(map[string]*rate.Limiter)
	return &Fetcher{
		domainLimiter:   m,
		store:           store,
		limitDuration:   limitDuration,
		timeoutDuration: timeoutDuration,
		minInterval:     minInterval,
	}, nil
}

func (f *Fetcher) getDomainLimiter(domain string) *rate.Limiter {
	f.mu.Lock()
	defer f.mu.Unlock()
	limiter, exists := f.domainLimiter[domain]
	if !exists {
		limiter = rate.NewLimiter(rate.Every(f.limitDuration), 1)
		f.domainLimiter[domain] = limiter
	}
	return limiter
}

func (f *Fetcher) Fetch(urlstr string) error {
	u, err := url.Parse(urlstr)
	if err != nil {
		return err
	}
	cu, closer, err := f.store.Get(urlstr)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if err == nil {
		defer closer.Close()
		crawlTime := time.Unix(cu.CrawlTs(), 0)
		minTime := crawlTime.Add(f.minInterval)
		if !time.Now().After(minTime) {
			return ErrAlreadyCrawledRecently
		}
	}
	limiter := f.getDomainLimiter(u.Hostname())
	ctx, cancel := context.WithTimeout(context.Background(), f.timeoutDuration)
	defer cancel()
	err = limiter.Wait(ctx)
	if err != nil {
		return err
	}
	resp, err := http.Get(urlstr)
	if err != nil {
		return err
	}
	return f.store.Save(urlstr, resp)
}

func (f *Fetcher) FetchConcurrentlyWait(urlChannel <-chan string, concurrency int) {
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for u := range urlChannel {
				err := f.Fetch(u)
				if err != nil {
					log.Printf("Error while fetching url %q, %s", u, err)
				}
			}
		}()
	}

	wg.Wait()
}

func (f *Fetcher) FetchConcurrently(urlChannel <-chan string, concurrency int) chan bool {
	c := make(chan bool, 1)
	go func() {
		f.FetchConcurrentlyWait(urlChannel, concurrency)
		c <- true
	}()
	return c
}
