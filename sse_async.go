package sse

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	httpModule "go.k6.io/k6/js/modules/k6/http"
	"go.k6.io/k6/js/promises"
	"go.k6.io/k6/metrics"
)

// AsyncClient represents an asynchronous SSE client
type AsyncClient struct {
	ctx        context.Context
	cancel     context.CancelFunc
	httpClient *http.Client
	url        string
	request    *http.Request
	response   *http.Response

	// Event handling
	eventChan chan Event
	doneChan  chan struct{}

	// State management
	mu       sync.RWMutex
	isOpen   bool
	isClosed bool
	lastErr  error

	// k6 integration
	vu             modules.VU
	tagsAndMeta    *metrics.TagsAndMeta
	samplesOutput  chan<- metrics.SampleContainer
	builtinMetrics *metrics.BuiltinMetrics
	sseMetrics     *sseMetrics
}

// OpenAsync opens an SSE connection asynchronously
// It returns the client immediately, while the connection happens in the background.
func (s *sse) OpenAsync(url string, paramsObj sobek.Value) (*AsyncClient, error) {
	state := s.vu.State()
	if state == nil {
		return nil, common.NewInitContextError("using sse in the init context is not supported")
	}

	// 1. Parse Options
	params, err := parseAsyncOptions(s.vu, url, paramsObj)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(s.vu.Context())

	// 2. Configure HTTP Client
	// Overriding the NextProtos to avoid talking http2 for SSE if needed
	var tlsConfig *tls.Config
	if state.TLSConfig != nil {
		tlsConfig = state.TLSConfig.Clone()
		tlsConfig.NextProtos = []string{"http/1.1"}
	}

	// Implementation of Connection Pooling via Transport configuration
	transport := &http.Transport{
		DialContext:         state.Dialer.DialContext,
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives: state.Options.NoConnectionReuse.ValueOrZero() ||
			state.Options.NoVUConnectionReuse.ValueOrZero(),
	}

	httpClient := &http.Client{
		Transport: transport,
	}

	if params.Jar != nil {
		httpClient.Jar = params.Jar
	}

	// 3. Initialize Client
	client := &AsyncClient{
		ctx:            ctx,
		cancel:         cancel,
		httpClient:     httpClient,
		url:            url,
		eventChan:      make(chan Event, 100), // Buffer slightly to handle bursts
		doneChan:       make(chan struct{}),
		isOpen:         false,
		isClosed:       false,
		vu:             s.vu,
		tagsAndMeta:    params.tagsAndMeta,
		samplesOutput:  state.Samples,
		builtinMetrics: state.BuiltinMetrics,
		sseMetrics:     s.metrics,
	}

	// Start Connection in Background with retry logic
	go client.connectWithRetry(params)

	return client, nil
}

func (c *AsyncClient) connectWithRetry(opts *asyncOptions) {
	defer close(c.doneChan)

	var bodyBytes []byte
	if opts.Body != "" {
		bodyBytes = []byte(opts.Body)
	}

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		// Check for context cancellation before starting an attempt
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Exponential Backoff with Jitter logic
		if attempt > 0 {
			// Calculate delay: base * 2^(attempt-1)
			backoffFactor := math.Pow(2, float64(attempt-1))
			delay := time.Duration(float64(opts.BaseDelay) * backoffFactor)
			if delay > opts.MaxDelay {
				delay = opts.MaxDelay
			}

			// Add 20% jitter
			jitter := time.Duration(rand.Float64() * 0.2 * float64(delay))
			sleepTime := delay + jitter

			select {
			case <-time.After(sleepTime):
			case <-c.ctx.Done():
				return
			}
		}

		// Re-create request for each attempt
		req, err := http.NewRequestWithContext(c.ctx, opts.Method, c.url, bytes.NewReader(bodyBytes))
		if err != nil {
			c.setError(fmt.Errorf("request creation failed: %w", err))
			return
		}

		// Apply headers
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Connection", "keep-alive")
		for k, v := range opts.Headers {
			req.Header.Set(k, v)
		}

		// Trace for IP tags
		trace := &httptrace.ClientTrace{
			GotConn: func(connInfo httptrace.GotConnInfo) {
				if c.vu.State().Options.SystemTags.Has(metrics.TagIP) {
					if ip, _, err2 := net.SplitHostPort(connInfo.Conn.RemoteAddr().String()); err2 == nil {
						c.tagsAndMeta.SetSystemTagOrMeta(metrics.TagIP, ip)
					}
				}
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
		c.request = req

		// Metrics and Request
		start := time.Now()
		resp, err := c.httpClient.Do(req)
		end := time.Now()

		if c.samplesOutput != nil {
			metrics.PushIfNotDone(c.ctx, c.samplesOutput, metrics.ConnectedSamples{
				Samples: []metrics.Sample{
					{
						TimeSeries: metrics.TimeSeries{
							Metric: c.builtinMetrics.HTTPReqSending,
							Tags:   c.tagsAndMeta.Tags,
						},
						Time:     start,
						Metadata: c.tagsAndMeta.Metadata,
						Value:    metrics.D(end.Sub(start)),
					},
				},
				Tags: c.tagsAndMeta.Tags,
				Time: start,
			})
		}

		if err != nil {
			// Network error
			if attempt == opts.MaxRetries {
				c.setError(fmt.Errorf("connection failed after %d retries: %v", attempt, err))
				return
			}
			continue
		}

		// Update Status Tag
		if c.vu.State().Options.SystemTags.Has(metrics.TagStatus) {
			c.tagsAndMeta.SetSystemTagOrMeta(metrics.TagStatus, strconv.Itoa(resp.StatusCode))
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			if attempt == opts.MaxRetries {
				c.setError(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
				return
			}
			continue
		}

		// Handshake successful
		c.handleSuccessfulConnect(resp, start)
		return
	}
}

func (c *AsyncClient) handleSuccessfulConnect(resp *http.Response, start time.Time) {
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		resp.Body.Close()
		c.setError(fmt.Errorf("unexpected content-type: %s", contentType))
		return
	}

	c.mu.Lock()
	c.response = resp
	c.isOpen = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.isOpen = false
		if c.response != nil && c.response.Body != nil {
			// Drain body to allow connection reuse if possible
			_, _ = io.Copy(io.Discard, c.response.Body)
			c.response.Body.Close()
		}
		c.mu.Unlock()

		// Push final request metrics
		finish := time.Now()
		if c.samplesOutput != nil {
			metrics.PushIfNotDone(c.ctx, c.samplesOutput, metrics.ConnectedSamples{
				Samples: []metrics.Sample{
					{
						TimeSeries: metrics.TimeSeries{
							Metric: c.builtinMetrics.HTTPReqs,
							Tags:   c.tagsAndMeta.Tags,
						},
						Time:     finish,
						Metadata: c.tagsAndMeta.Metadata,
						Value:    1,
					},
					{
						TimeSeries: metrics.TimeSeries{
							Metric: c.builtinMetrics.HTTPReqDuration,
							Tags:   c.tagsAndMeta.Tags,
						},
						Time:     finish,
						Metadata: c.tagsAndMeta.Metadata,
						Value:    metrics.D(finish.Sub(start)),
					},
				},
				Tags: c.tagsAndMeta.Tags,
				Time: finish,
			})
		}
	}()

	c.readEvents()
}

// Wraps SSE in a channel, follow the SSE format described in:
// https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
func (c *AsyncClient) readEvents() {
	if c.response == nil {
		return
	}

	reader := bufio.NewReader(c.response.Body)
	ev := Event{}
	var buf bytes.Buffer

	for {
		// Check for cancellation
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				c.setError(fmt.Errorf("read error: %w", err))
			}
			return
		}

		switch {
		// id of event
		case hasPrefix(line, "id: "):
			ev.ID = stripPrefix(line, 4)
		case hasPrefix(line, "id:"):
			ev.ID = stripPrefix(line, 3)

		// Comment
		case hasPrefix(line, ": "):
			ev.Comment = stripPrefix(line, 2)
		case hasPrefix(line, ":"):
			ev.Comment = stripPrefix(line, 1)

		// name of event
		case hasPrefix(line, "event: "):
			ev.Name = stripPrefix(line, 7)
		case hasPrefix(line, "event:"):
			ev.Name = stripPrefix(line, 6)

		// event data
		case hasPrefix(line, "data: "):
			buf.Write(line[6:])
		case hasPrefix(line, "data:"):
			buf.Write(line[5:])

		case hasPrefix(line, "retry:"):
			// ignore

		// end of event
		case isLineEnd(line):
			// Trailing newlines are removed.
			ev.Data = strings.TrimRightFunc(buf.String(), func(r rune) bool {
				return r == '\r' || r == '\n'
			})

			// Only emit if we have data or an event name (ignore keep-alives)
			if ev.Data != "" || ev.Name != "" {
				// Metric: sse_events_received
				if c.samplesOutput != nil && c.sseMetrics != nil {
					metrics.PushIfNotDone(c.ctx, c.samplesOutput, metrics.Sample{
						TimeSeries: metrics.TimeSeries{
							Metric: c.sseMetrics.SSEEventReceived,
							Tags:   c.tagsAndMeta.Tags,
						},
						Time:     time.Now(),
						Metadata: c.tagsAndMeta.Metadata,
						Value:    1,
					})
				}

				// Send to channel, respecting context
				select {
				case c.eventChan <- ev:
					buf.Reset()
					ev = Event{}
				case <-c.ctx.Done():
					return
				}
			} else {
				// Reset buffer even if we didn't emit (e.g. empty keep-alive)
				buf.Reset()
				ev = Event{}
			}

		default:
			// ignore unknown
		}
	}
}

// PollEvents retrieves events that have been received (non-blocking)
func (c *AsyncClient) PollEvents() []Event {
	c.mu.RLock()
	if c.isClosed {
		c.mu.RUnlock()
		return []Event{}
	}
	c.mu.RUnlock()

	events := make([]Event, 0)
	for {
		select {
		case event := <-c.eventChan:
			events = append(events, event)
		default:
			return events
		}
	}
}

// WaitForEvent waits for an event matching the predicate (blocking)
func (c *AsyncClient) WaitForEvent(predicateFn sobek.Value, timeoutMs int) (Event, error) {
	c.mu.RLock()
	if c.isClosed {
		c.mu.RUnlock()
		return Event{}, fmt.Errorf("client is closed")
	}
	c.mu.RUnlock()

	rt := c.vu.Runtime()

	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case event := <-c.eventChan:
			// No predicate = return first event
			if predicateFn == nil || sobek.IsUndefined(predicateFn) || sobek.IsNull(predicateFn) {
				return event, nil
			}

			callable, isCallable := sobek.AssertFunction(predicateFn)
			if !isCallable {
				return Event{}, fmt.Errorf("predicate is not a function")
			}

			result, err := callable(sobek.Undefined(), rt.ToValue(event))
			if err != nil {
				return Event{}, fmt.Errorf("predicate error: %w", err)
			}

			if result.ToBoolean() {
				return event, nil
			}

		case <-timer.C:
			return Event{}, fmt.Errorf("timeout waiting for event")

		case <-c.doneChan:
			if err := c.GetError(); err != nil {
				return Event{}, err
			}
			return Event{}, fmt.Errorf("connection closed")

		case <-c.ctx.Done():
			return Event{}, fmt.Errorf("context cancelled")
		}
	}
}

// WaitForEventAsync returns a Promise that resolves when the event is received
func (c *AsyncClient) WaitForEventAsync(predicateFn sobek.Value, timeoutMs int) *sobek.Promise {
	promise, resolve, reject := promises.New(c.vu)

	callbackChan := make(chan func(func() error), 1)

	// Initial registration
	callbackChan <- c.vu.RegisterCallback()

	go func() {
		// Quick check before waiting
		c.mu.RLock()
		if c.isClosed {
			c.mu.RUnlock()
			cb := <-callbackChan
			cb(func() error {
				reject(fmt.Errorf("client is closed"))
				return nil
			})
			return
		}
		c.mu.RUnlock()

		timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
		defer timer.Stop()

		for {
			select {
			case event := <-c.eventChan:
				// Get the *current* callback handle
				cb := <-callbackChan

				// We use this bool to signal the Go loop to exit
				shouldExit := false

				cb(func() error {
					// --- We are now on the JS Thread ---

					// 1. Check Predicate
					match := false
					if predicateFn == nil || sobek.IsUndefined(predicateFn) || sobek.IsNull(predicateFn) {
						match = true
					} else {
						rt := c.vu.Runtime()
						callable, isCallable := sobek.AssertFunction(predicateFn)
						if !isCallable {
							reject(fmt.Errorf("predicate is not a function"))
							shouldExit = true
							return nil
						}

						result, err := callable(sobek.Undefined(), rt.ToValue(event))
						if err != nil {
							reject(fmt.Errorf("predicate error: %w", err))
							shouldExit = true
							return nil
						}
						match = result.ToBoolean()
					}

					if match {
						resolve(event)
						shouldExit = true
					} else {
						// 2. Recycle Callback
						// If we didn't match, we need to listen for the next event.
						// We are on the JS thread, so we can register a NEW callback.
						newCb := c.vu.RegisterCallback()
						// Send it back to the Go loop for the next iteration
						callbackChan <- newCb
					}
					return nil
				})

				if shouldExit {
					return
				}

			case <-timer.C:
				cb := <-callbackChan
				cb(func() error {
					reject(fmt.Errorf("timeout waiting for event"))
					return nil
				})
				return

			case <-c.doneChan:
				cb := <-callbackChan
				cb(func() error {
					if err := c.GetError(); err != nil {
						reject(err)
					} else {
						reject(fmt.Errorf("connection closed"))
					}
					return nil
				})
				return

			case <-c.ctx.Done():
				cb := <-callbackChan
				cb(func() error {
					reject(fmt.Errorf("context cancelled"))
					return nil
				})
				return
			}
		}
	}()

	return promise
}

func (c *AsyncClient) IsOpen() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isOpen
}

func (c *AsyncClient) GetError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastErr
}

func (c *AsyncClient) Close() error {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return nil
	}
	c.isClosed = true
	c.mu.Unlock()

	c.cancel()

	select {
	case <-c.doneChan:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for connection to close")
	}
}

func (c *AsyncClient) GetStatus() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.response != nil {
		return c.response.StatusCode
	}
	return 0
}

func (c *AsyncClient) setError(err error) {
	c.mu.Lock()
	if c.lastErr == nil {
		c.lastErr = err
	}
	c.mu.Unlock()
}

// Helpers for options parsing

type asyncOptions struct {
	Method      string
	Body        string
	Headers     map[string]string
	Timeout     time.Duration
	Jar         http.CookieJar
	tagsAndMeta *metrics.TagsAndMeta
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func parseAsyncOptions(vu modules.VU, url string, paramsObj sobek.Value) (*asyncOptions, error) {
	state := vu.State()
	rt := vu.Runtime()

	// Default options
	opts := &asyncOptions{
		Method:     "GET",
		Headers:    make(map[string]string),
		Timeout:    60 * time.Second,
		MaxRetries: 0,
		BaseDelay:  1000 * time.Millisecond,
		MaxDelay:   30000 * time.Millisecond,
	}

	// Capture initial tags
	tagsAndMeta := state.Tags.GetCurrentValues()
	tagsAndMeta.SetSystemTagOrMetaIfEnabled(state.Options.SystemTags, metrics.TagURL, url)
	opts.tagsAndMeta = &tagsAndMeta

	// Set default User-Agent
	opts.Headers["User-Agent"] = state.Options.UserAgent.String

	// If no params object, return defaults
	if paramsObj == nil || sobek.IsUndefined(paramsObj) || sobek.IsNull(paramsObj) {
		return opts, nil
	}

	paramsMap := paramsObj.ToObject(rt)
	if paramsMap == nil {
		return opts, nil
	}

	// Parse Method
	if method := paramsMap.Get("method"); method != nil && !sobek.IsUndefined(method) {
		opts.Method = strings.ToUpper(method.String())
	}

	// Parse Body
	if body := paramsMap.Get("body"); body != nil && !sobek.IsUndefined(body) {
		opts.Body = body.String()
	}

	// Parse Headers
	if headers := paramsMap.Get("headers"); headers != nil && !sobek.IsUndefined(headers) {
		headersObj := headers.ToObject(rt)
		if headersObj != nil {
			for _, key := range headersObj.Keys() {
				opts.Headers[key] = headersObj.Get(key).String()
			}
		}
	}

	// Parse Tags
	if tags := paramsMap.Get("tags"); tags != nil && !sobek.IsUndefined(tags) {
		if err := common.ApplyCustomUserTags(rt, opts.tagsAndMeta, tags); err != nil {
			return nil, fmt.Errorf("invalid metric tags: %w", err)
		}
	}

	// Parse Timeout
	if timeout := paramsMap.Get("timeout"); timeout != nil && !sobek.IsUndefined(timeout) {
		if t := timeout.ToInteger(); t > 0 {
			opts.Timeout = time.Duration(t) * time.Millisecond
		} else if tStr := timeout.ToString().String(); tStr != "" {
			d, err := time.ParseDuration(tStr)
			if err == nil {
				opts.Timeout = d
			}
		}
	}

	// Retry Options
	if val := paramsMap.Get("maxRetries"); val != nil && !sobek.IsUndefined(val) {
		opts.MaxRetries = int(val.ToInteger())
	}
	if val := paramsMap.Get("baseDelay"); val != nil && !sobek.IsUndefined(val) {
		if t := val.ToInteger(); t > 0 {
			opts.BaseDelay = time.Duration(t) * time.Millisecond
		}
	}
	if val := paramsMap.Get("maxDelay"); val != nil && !sobek.IsUndefined(val) {
		if t := val.ToInteger(); t > 0 {
			opts.MaxDelay = time.Duration(t) * time.Millisecond
		}
	}

	if jarValue := paramsMap.Get("jar"); jarValue != nil && !sobek.IsUndefined(jarValue) {
		// Try to unwrap k6 http.CookieJar
		if exported := jarValue.Export(); exported != nil {
			if k6Jar, ok := exported.(*httpModule.CookieJar); ok {
				opts.Jar = k6Jar.Jar
			} else if stdJar, ok := exported.(http.CookieJar); ok {
				opts.Jar = stdJar
			}
		}
	}

	// If no custom jar, use the VU's default jar
	if opts.Jar == nil {
		opts.Jar = state.CookieJar
	}

	return opts, nil
}