package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	ErrAlreadyCrawledRecently = errors.New("error: already crawled recently")
)

// Interface that defines what can be `fetched`. The url to be fetched is
// returned by `Url()` method. Before the actual fetching is performed, the
// `Validate()` method is called. Fetching only proceeds if that method returns
// a `nil` error. Finally, `HandleResponse()` is the callback when crawling is
// successful.
type Fetchable interface {
	Url() string
	Validate() error
	HandleResponse(*http.Response) error
}

type Fetcher struct {
	limitDuration   time.Duration
	timeoutDuration time.Duration
	minInterval     time.Duration
	domainLimiter   map[string]*rate.Limiter
	mu              sync.Mutex
}

func NewFetcher(timeoutDuration time.Duration, limitDuration time.Duration, minInterval time.Duration) (*Fetcher, error) {
	m := make(map[string]*rate.Limiter)
	return &Fetcher{
		domainLimiter:   m,
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

func (f *Fetcher) Fetch(furl Fetchable) error {
	err := furl.Validate()
	if err != nil {
		return err
	}
	u, err := url.Parse(furl.Url())
	if err != nil {
		return err
	}
	limiter := f.getDomainLimiter(u.Hostname())
	ctx, cancel := context.WithTimeout(context.Background(), f.timeoutDuration)
	defer cancel()
	err = limiter.Wait(ctx)
	if err != nil {
		return err
	}
	resp, err := http.Get(furl.Url())
	if err != nil {
		return err
	}
	return furl.HandleResponse(resp)
}

func (f *Fetcher) FetchConcurrentlyWait(urlChannel <-chan Fetchable, concurrency int) {
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

func (f *Fetcher) FetchConcurrently(urlChannel <-chan Fetchable, concurrency int) chan bool {
	c := make(chan bool, 1)
	go func() {
		f.FetchConcurrentlyWait(urlChannel, concurrency)
		c <- true
	}()
	return c
}