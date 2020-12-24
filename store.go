package crawler

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/agrewal/crawler/crawled_url"
	"github.com/agrewal/crawler/fetcher"
	"github.com/cockroachdb/pebble"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/segmentio/ksuid"
)

var builderPool = sync.Pool{
	New: func() interface{} {
		return flatbuffers.NewBuilder(0)
	},
}

var (
	ErrAlreadyCrawledRecently = errors.New("error: already crawled recently")
)

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
	id          string
}

func NewStoringFetchable(urlStr string, store *Store, minInterval time.Duration) *StoringFetchable {
	return &StoringFetchable{
		urlStr:      urlStr,
		store:       store,
		minInterval: minInterval,
		id:          ksuid.New().String(),
	}
}

func (s *StoringFetchable) Id() string {
	return s.id
}

func (s *StoringFetchable) Request() (*http.Request, error) {
	return http.NewRequest("GET", s.urlStr, nil)
}

func (s *StoringFetchable) Validate() error {
	cu, closer, err := s.store.Get(s.urlStr)
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
	return s.store.Save(s.urlStr, resp)
}

func ReaderToStoringFetchable(ctx context.Context, reader io.Reader, channelBufferSize int, store *Store, minInterval time.Duration) <-chan fetcher.Fetchable {
	strChannel := make(chan fetcher.Fetchable, channelBufferSize)
	makeFetchable := func(u string) *StoringFetchable {
		return NewStoringFetchable(u, store, minInterval)
	}
	go func() {
		scanner := bufio.NewScanner(reader)
		defer close(strChannel)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			case strChannel <- makeFetchable(scanner.Text()):
				continue
			}
		}
	}()
	return strChannel
}
