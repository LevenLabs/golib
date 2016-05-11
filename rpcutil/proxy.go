package rpcutil

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"

	"golang.org/x/net/context"
)

// HTTPProxy implements an http reverse proxy. It is obstensibly a simple
// wrapper around httputil.ReverseProxy, except that it handles a number of
// common cases that we generally want
//
// To use, first change the URL on an incoming request to the new destination,
// as well as any other changes to the request which are wished to be made. Then
// call ServeHTTP on the HTTPProxy with that edited request and its
// ResponseWriter.
//
// Features implemented:
// * Disable built-in httputil.ReverseProxy logger
// * Automatically adding X-Forwarded-For
type HTTPProxy struct {
	once sync.Once
	rp   *httputil.ReverseProxy
}

func (h *HTTPProxy) init() {
	h.once.Do(func() {
		h.rp = &httputil.ReverseProxy{
			Director: func(r *http.Request) {},
			// This is unfortunately the only way to keep the proxy from using
			// its own log format
			ErrorLog: log.New(ioutil.Discard, "", 0),
		}
	})
}

func (h *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ServeHTTPCtx(context.Background(), w, r)
}

// ServeHTTPCtx will do the proxying of the given request and write the response
// to the ResponseWriter. ctx can be used to cancel the request mid-way.
func (h *HTTPProxy) ServeHTTPCtx(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h.init()
	// We only do the cancellation logic if the Cancel channel hasn't been set
	// on the Request already. If it has, then some other process is liable to
	// close it also, which would cause a panic
	if r.Cancel == nil {
		cancelCh := make(chan struct{})
		r.Cancel = cancelCh
		doneCh := make(chan struct{})
		defer close(doneCh) // so no matter what the go-routine exits
		go func() {
			select {
			case <-ctx.Done():
			case <-doneCh:
			}
			close(cancelCh)
		}()
	}
	AddProxyXForwardedFor(r, r)
	h.rp.ServeHTTP(w, r)
}

// BufferedResponseWriter is a wrapper around a real ResponseWriter which
// actually writes all data to a buffer instead of the ResponseWriter. It also
// catches calls to WriteHeader. Once writing is done, GetBody can be called to
// get the buffered body, and SetBody can be called to set a new body to be
// used. When ActuallyWrite is called all headers will be written to the
// ResponseWriter (with corrected Content-Length if a new body was given)
// followed by the body.
//
// BufferedResponseWriter will transparently handle un-gzipping and re-gzipping
// the response body when it sees Content-Encoding: gzip.
type BufferedResponseWriter struct {
	http.ResponseWriter
	buffer  *bytes.Buffer
	newBody io.Reader

	// The response code which was or will be written to the ResponseWriter
	Code int
}

// NewBufferedResponseWriter returns an initialized BufferedResponseWriter,
// which will catch writes going to the given ResponseWriter
func NewBufferedResponseWriter(rw http.ResponseWriter) *BufferedResponseWriter {
	return &BufferedResponseWriter{
		ResponseWriter: rw,
		buffer:         new(bytes.Buffer),
	}
}

// WriteHeader catches the call to WriteHeader and doesn't actually do anything,
// except store the given code for later use when ActuallyWrite is called
func (brw *BufferedResponseWriter) WriteHeader(code int) {
	brw.Code = code
}

func (brw *BufferedResponseWriter) Write(b []byte) (int, error) {
	if brw.Code == 0 {
		brw.WriteHeader(200)
	}
	return brw.buffer.Write(b)
}

// Returns a buffer which will produce the original raw bytes from the
// response body. May be called multiple times, will return independent buffers
// each time. The buffers should NEVER be written to
func (brw *BufferedResponseWriter) originalBodyRaw() *bytes.Buffer {
	// We take the bytes in the existing buffer, and make a new buffer around
	// those bytes. This way we can read from the new buffer without draining
	// the original buffer
	return bytes.NewBuffer(brw.buffer.Bytes())
}

// GetBody returns a ReadCloser which gives the body of the response which was
// buffered. If the response was compressed the body will be transparently
// decompressed. Close must be called in order to clean up resources. GetBody
// may be called multiple times, each time it will return an independent
// ReadCloser for the original response body.
func (brw *BufferedResponseWriter) GetBody() (io.ReadCloser, error) {
	body := brw.originalBodyRaw()
	// TODO we need to support deflate and sdch
	if brw.Header().Get("Content-Encoding") == "gzip" {
		gzR, err := gzip.NewReader(body)
		if err != nil {
			return nil, err
		}
		return gzR, nil
	}
	return ioutil.NopCloser(body), nil
}

// SetBody sets the io.Reader from which the new body of the buffered response
// should be read from. This io.Reader will be drained once ActuallyWrite is
// called. If this is never called the original body will be used.
//
// Note: there is no need to send in compressed data here, the new response body
// will be automatically compressed in the same way the original was.
func (brw *BufferedResponseWriter) SetBody(in io.Reader) {
	brw.newBody = in
}

func (brw *BufferedResponseWriter) bodyBuf() (*bytes.Buffer, error) {
	body := brw.originalBodyRaw()
	if brw.newBody == nil {
		return body, nil
	}

	// using body.Len() here is just a guess for how big we think the new body
	// will be
	bodyBuf := bytes.NewBuffer(make([]byte, 0, body.Len()))
	var dst io.Writer = bodyBuf

	if brw.Header().Get("Content-Encoding") == "gzip" {
		gzW := gzip.NewWriter(bodyBuf)
		defer gzW.Close()
		dst = gzW

	} else if bodyBuf2, ok := brw.newBody.(*bytes.Buffer); ok {
		// shortcut, we don't actually have to do anything if the body we've
		// been given is already a buffer and we don't have to gzip it
		return bodyBuf2, nil
	}

	// dst will be some wrapper around bodyBuf, or bodyBuf itself
	if _, err := io.Copy(dst, brw.newBody); err != nil {
		return nil, err
	}
	return bodyBuf, nil
}

// ActuallyWrite takes all the buffered data and actually writes to the wrapped
// ResponseWriter. Returns the number of bytes written as the body
func (brw *BufferedResponseWriter) ActuallyWrite() (int64, error) {
	bodyBuf, err := brw.bodyBuf()
	if err != nil {
		return 0, err
	}

	rw := brw.ResponseWriter
	rw.Header().Set("Content-Length", strconv.Itoa(bodyBuf.Len()))
	rw.WriteHeader(brw.Code)
	return io.Copy(rw, bodyBuf)
}

type brwMarshalled struct {
	Code   int
	Header map[string][]string
	Body   []byte
}

// MarshalBinary returns a binary form of the BufferedResponseWriter. Can only
// be called after a response has been buffered.
func (brw *BufferedResponseWriter) MarshalBinary() ([]byte, error) {
	body, err := ioutil.ReadAll(brw.originalBodyRaw())
	if err != nil {
		return nil, err
	}

	bm := brwMarshalled{
		Code:   brw.Code,
		Header: brw.Header(),
		Body:   body,
	}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(&bm); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary takes in a byte slice produced by calling MarshalBinary on
// another BufferedResponseWriter, and makes this one identical to it.
func (brw *BufferedResponseWriter) UnmarshalBinary(b []byte) error {
	var bm brwMarshalled
	if err := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&bm); err != nil {
		return err
	}

	brw.Code = bm.Code

	// If there's any pre-set headers in the ResponseWriter, get rid of them
	h := brw.Header()
	for k := range h {
		delete(h, k)
	}

	// Now copy in the headers we want
	for k, vv := range bm.Header {
		for _, v := range vv {
			h.Add(k, v)
		}
	}

	// Copy in the body
	io.Copy(brw.buffer, bytes.NewBuffer(bm.Body))
	return nil
}
