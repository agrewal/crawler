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

type StoringFetchable struct {
	Fetchable
	store Store
}

func NewStoringFetchable(fetchable Fetchable, store Store) *StoringFetchable {
	return &StoringFetchable{fetchable, store}
}

func (sf *StoringFetchable) HandleResponseBody(body []byte) error {
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

func NewStoreBackedFetcher(store Store, fetcher *Fetcher, minInterval time.Duration) *StoreBackedFetcher {
	return &StoreBackedFetcher{
		store,
		fetcher,
		minInterval,
	}
}

func (sbf *StoreBackedFetcher) Fetch(furl Fetchable) error {
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
	l.Debug("Fetching from the store")
	cu, closer, err := sbf.store.Get(u)
	if err != nil {
		return err
	}
	defer closer.Close()
	if cu != nil {
		l.Debug("Found in storage")
		crawlTime := time.Unix(cu.CrawlTs(), 0)
		minTime := crawlTime.Add(sbf.minInterval)
		if !time.Now().After(minTime) {
			l.Debug("minInterval has not elapsed, returning crawled data from store")
			return furl.HandleResponseBody(cu.Body())
		}
	}
	l.Debug("Calling underlying fetcher")
	return sbf.fetcher.Fetch(NewStoringFetchable(furl, sbf.store))
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
