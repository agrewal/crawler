package fetcher

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// Interface that defines what can be `fetched`.
type Fetchable interface {
	// Unique identifier for this fetchable item. This is useful in logging.
	Id() string

	// Url that this is trying to fetch. It can also be determined from
	// Request. Keeping it here to avoid repetition in the codebase.
	Url() string

	// Build a request.
	Request() (*http.Request, error)

	// Callback to handle the http response body corresponding to the request.
	// This can be used for example, to store data into the store, or to parse
	// the results in some way
	HandleResponseBody([]byte) error
}

// Wrapping a `string` (holding a url) into a `Fetchable`
type StringFetchable string

// Return the string as the Id
func (sf StringFetchable) Id() string {
	return string(sf)
}

// Return the string as the Url
func (sf StringFetchable) Url() string {
	return string(sf)
}

// Returns a GET request to the Url
func (sf StringFetchable) Request() (*http.Request, error) {
	return http.NewRequest("GET", sf.Url(), nil)
}

// Always returns nil.
func (sf StringFetchable) HandleResponseBody(body []byte) error {
	return nil
}

// Interface
type FetchI interface {
	Fetch(Fetchable) error
}

// Fetcher struct used to download
type Fetcher struct {
	options *Options

	// Limiter instance per domain.
	domainLimiter map[string]*rate.Limiter

	// Mutex to guard against simultaneous access
	mu sync.Mutex
}

type Options struct {
	// http client to use. This allows callers to specify more details like
	// timeouts
	client *http.Client

	// The amount of time between each request to a given domain. Used with
	// rate.Every
	limitDuration time.Duration

	// Burst capacity
	burst int

	// Amount of time to wait for the rate limiter.
	timeoutDuration time.Duration
}

// Default options to be used with a `Fetcher` instance
var DefaultOptions = &Options{
	client:          http.DefaultClient,
	limitDuration:   5 * time.Second,
	timeoutDuration: 1 * time.Minute,
	burst:           1,
}

// Returns a new `Fetcher` instance.
func NewFetcher() *Fetcher {
	return NewFetcherWithOptions(DefaultOptions)
}

// Returns a `Fetcher` with specified options. If any fields of the option are
// equal to the zero value, we use the value from `DefaultOptions` instead.
// This allows a caller to specify only the changed options
func NewFetcherWithOptions(options *Options) *Fetcher {
	opts := &Options{}
	if options.client != nil {
		opts.client = options.client
	} else {
		opts.client = DefaultOptions.client
	}

	if options.limitDuration != 0 {
		opts.limitDuration = options.limitDuration
	} else {
		opts.limitDuration = DefaultOptions.limitDuration
	}

	if options.timeoutDuration != 0 {
		opts.timeoutDuration = options.timeoutDuration
	} else {
		opts.timeoutDuration = DefaultOptions.timeoutDuration
	}

	if options.burst != 0 {
		opts.burst = options.burst
	} else {
		opts.burst = DefaultOptions.burst
	}

	return &Fetcher{
		options:       opts,
		domainLimiter: make(map[string]*rate.Limiter),
	}
}

// Internal function to get a limiter for a given domain. If this is the first
// time a domain is being crawled, it creates a new limiter
func (f *Fetcher) getDomainLimiter(domain string) *rate.Limiter {
	f.mu.Lock()
	defer f.mu.Unlock()
	limiter, exists := f.domainLimiter[domain]
	if !exists {
		limiter = rate.NewLimiter(rate.Every(f.options.limitDuration), f.options.burst)
		f.domainLimiter[domain] = limiter
	}
	return limiter
}

// Performs the actual fetch of a given `Fetchable`. The steps it follows are:
//   1. Build the request by calling `Request()`
//   2. Validate the request by calling `Validate()`
//   3. Wait until the rate limit allows the domain to be crawled, or
//   options.timeoutDuration is exceeded
//   4. Actually make the http request with the supplied client, calling
//   `HandleResponse()` on the output
func (f *Fetcher) Fetch(furl Fetchable) error {
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
	l.Debug("Waiting for rate limiter")
	limiter := f.getDomainLimiter(req.URL.Hostname())
	ctx, cancel := context.WithTimeout(context.Background(), f.options.timeoutDuration)
	defer cancel()
	err = limiter.Wait(ctx)
	if err != nil {
		return err
	}
	l.Debug("Beginning http Get")
	resp, err := f.options.client.Do(req)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	l.Debugf("Finished reading body (len: %d bytes)", len(body))
	return furl.HandleResponseBody(body)
}

// Starts `concurrency` goroutines to fetch content from `urlChannel` in
// parallel. The goroutines end when the `urlChannel` is closed. This method
// waits until all the launched goroutines are complete.
//
// Note: Please ensure you call `close()` on the `urlChannel`, or else this
// method will never return
func (f *Fetcher) FetchConcurrentlyWait(urlChannel <-chan Fetchable, concurrency int) {
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for u := range urlChannel {
				err := f.Fetch(u)
				if err != nil {
					log.WithField("id", u.Id()).WithError(err).Error("Did not fetch")
				}
			}
		}()
	}
	wg.Wait()
}
