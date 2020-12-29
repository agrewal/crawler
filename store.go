package fetcher

import (
	"io"
	"sync"
	"time"

	"github.com/agrewal/fetcher/crawled_url"
	"github.com/cockroachdb/pebble"
	flatbuffers "github.com/google/flatbuffers/go"
	log "github.com/sirupsen/logrus"
)

var builderPool = sync.Pool{
	New: func() interface{} {
		return flatbuffers.NewBuilder(0)
	},
}

type Store interface {
	Get(key string) (*crawled_url.CrawledUrl, io.Closer, error)
	Set(key string, body []byte) error
}

type storeWrappedFetchable struct {
	Fetchable
	store Store
}

func NewStoreWrappedFetchable(fetchable Fetchable, store Store) *storeWrappedFetchable {
	return &storeWrappedFetchable{fetchable, store}
}

func (sf *storeWrappedFetchable) HandleResponseBody(body []byte) error {
	builder := builderPool.Get().(*flatbuffers.Builder)
	defer builderPool.Put(builder)
	builder.Reset()
	body_offset := builder.CreateByteString(body)
	id_offset := builder.CreateString(sf.Id())
	crawled_url.CrawledUrlStart(builder)
	crawled_url.CrawledUrlAddCrawlTs(builder, time.Now().Unix())
	crawled_url.CrawledUrlAddCrawlId(builder, id_offset)
	crawled_url.CrawledUrlAddBody(builder, body_offset)
	cu := crawled_url.CrawledUrlEnd(builder)
	builder.Finish(cu)
	buf := builder.FinishedBytes()
	err := sf.store.Set(sf.Url(), buf)
	if err != nil {
		return err
	}
	return sf.Fetchable.HandleResponseBody(body)
}

type StoreBackedFetcher struct {
	store       Store
	fetcher     *Fetcher
	minInterval time.Duration
}

type StoringFetchable interface {
	Fetchable

	// Force a fetch from the URL, instead of getting from the store
	ForceFetch() bool
}

func NewStoreBackedFetcher(store Store, fetcher *Fetcher, minInterval time.Duration) *StoreBackedFetcher {
	return &StoreBackedFetcher{
		store,
		fetcher,
		minInterval,
	}
}

func (sbf *StoreBackedFetcher) Fetch(furl StoringFetchable) error {
	log.WithField("id", furl.Id()).Debug("Getting http request")
	req, err := furl.Request()
	if err != nil {
		return err
	}
	u := req.URL.String()
	l := log.WithFields(log.Fields{
		"id":  furl.Id(),
		"url": u,
	})
	if !furl.ForceFetch() {
		l.Debug("Fetching from the store")
		cu, closer, err := sbf.store.Get(u)
		if closer != nil {
			defer closer.Close()
		}
		if err != nil && err != pebble.ErrNotFound {
			return err
		}
		if cu != nil {
			l.Debug("Found in storage")
			crawlTime := time.Unix(cu.CrawlTs(), 0)
			minTime := crawlTime.Add(sbf.minInterval)
			if !time.Now().After(minTime) {
				l.Debug("minInterval has not elapsed, returning crawled data from store")
				return furl.HandleResponseBody(cu.Body())
			}
		}
	} else {
		l.Debug("Skipping store because ForceFetch is set")
	}
	l.Debug("Calling underlying fetcher")
	return sbf.fetcher.Fetch(NewStoreWrappedFetchable(furl, sbf.store))
}

// Starts `concurrency` goroutines to fetch content from `urlChannel` in
// parallel. The goroutines end when the `urlChannel` is closed. This method
// waits until all the launched goroutines are complete.
//
// Note: Please ensure you call `close()` on the `urlChannel`, or else this
// method will never return
func (sbf *StoreBackedFetcher) FetchConcurrentlyWait(urlChannel <-chan StoringFetchable, concurrency int) {
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for u := range urlChannel {
				err := sbf.Fetch(u)
				if err != nil {
					log.WithField("id", u.Id()).WithError(err).Error("Did not fetch")
				}
			}
		}()
	}
	wg.Wait()
}

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(dirname string) (*PebbleStore, error) {
	db, err := pebble.Open(dirname, nil)
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db}, nil
}

func (s *PebbleStore) Close() {
	s.db.Close()
}

func (s *PebbleStore) Get(key string) (*crawled_url.CrawledUrl, io.Closer, error) {
	data, closer, err := s.db.Get([]byte(key))
	if err != nil {
		return nil, closer, err
	}
	cu := crawled_url.GetRootAsCrawledUrl(data, 0)
	return cu, closer, nil
}

func (s *PebbleStore) Set(key string, body []byte) error {
	return s.db.Set([]byte(key), body, nil)
}
