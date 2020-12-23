package crawler

import (
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/agrewal/crawler/crawled_url"
	"github.com/cockroachdb/pebble"
	flatbuffers "github.com/google/flatbuffers/go"
)

var builderPool = sync.Pool{
	New: func() interface{} {
		return flatbuffers.NewBuilder(0)
	},
}

type Store struct {
	db *pebble.DB
}

func NewStore(dirname string) (*Store, error) {
	db, err := pebble.Open(dirname, nil)
	if err != nil {
		return nil, err
	}
	return &Store{db}, nil
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) Save(url string, resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	builder := builderPool.Get().(*flatbuffers.Builder)
	builder.Reset()
	body_offset := builder.CreateByteString(body)
	crawled_url.CrawledUrlStart(builder)
	crawled_url.CrawledUrlAddCrawlTs(builder, time.Now().Unix())
	crawled_url.CrawledUrlAddStatusCode(builder, int32(resp.StatusCode))
	crawled_url.CrawledUrlAddBody(builder, body_offset)
	cu := crawled_url.CrawledUrlEnd(builder)
	builder.Finish(cu)
	buf := builder.FinishedBytes()
	builderPool.Put(builder)
	return s.db.Set([]byte(url), buf, nil)
}

func (s *Store) Get(url string) (*crawled_url.CrawledUrl, io.Closer, error) {
	data, closer, err := s.db.Get([]byte(url))
	if err != nil {
		return nil, closer, err
	}
	cu := crawled_url.GetRootAsCrawledUrl(data, 0)
	return cu, closer, nil
}

func (s *Store) Exists(url string) (bool, error) {
	_, closer, err := s.db.Get([]byte(url))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	return true, nil
}

func (s *Store) LastCrawlTs(url string) (int64, error) {
	cu, closer, err := s.Get(url)
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	return cu.CrawlTs(), nil
}

type StoringFetchable struct {
	urlStr      string
	store       *Store
	minInterval time.Duration
}

func NewStoringFetchable(urlStr string, store *Store, minInterval time.Duration) *StoringFetchable {
	return &StoringFetchable{
		urlStr:      urlStr,
		store:       store,
		minInterval: minInterval,
	}
}

func (s *StoringFetchable) Url() string {
	return s.urlStr
}

func (s *StoringFetchable) Validate() error {
	cu, closer, err := s.store.Get(s.Url())
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if err == nil {
		defer closer.Close()
		crawlTime := time.Unix(cu.CrawlTs(), 0)
		minTime := crawlTime.Add(s.minInterval)
		if !time.Now().After(minTime) {
			return ErrAlreadyCrawledRecently
		}
	}
	return nil
}

func (s *StoringFetchable) HandleResponse(resp *http.Response) error {
	return s.store.Save(s.Url(), resp)
}
