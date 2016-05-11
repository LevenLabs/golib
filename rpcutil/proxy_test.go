package rpcutil

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bodyBufCopy(t *T, body io.Reader) *bytes.Buffer {
	cp := new(bytes.Buffer)
	_, err := io.Copy(cp, body)
	require.Nil(t, err)
	return cp
}

func TestBufferedResponseWriter(t *T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Foo", "foo")
		w.WriteHeader(201)
		fmt.Fprint(w, "foo")
	})
	rw := httptest.NewRecorder()
	brw := NewBufferedResponseWriter(rw)
	h.ServeHTTP(brw, nil)

	bodyBuf, err := brw.GetBody()
	bodyBufB, err := ioutil.ReadAll(bodyBuf)
	assert.Nil(t, err)

	bodyBuf2, err := brw.GetBody()
	assert.Nil(t, err)
	bodyBufCp := bodyBufCopy(t, bodyBuf2)

	// Make sure brw caught writes, and the recorder didn't get any of them
	assert.Equal(t, []byte("foo"), bodyBufB)
	assert.Equal(t, "foo", brw.Header().Get("X-Foo"))
	assert.Equal(t, 201, brw.Code)
	assert.NotEqual(t, 201, rw.Code)
	assert.Equal(t, 0, rw.Body.Len())

	bodyBufCp.WriteString("bar")
	brw.SetBody(bodyBufCp)

	written, err := brw.ActuallyWrite()
	assert.Nil(t, err)
	assert.Equal(t, int64(6), written)

	assert.Equal(t, 201, rw.Code)
	assert.Equal(t, []byte("foobar"), rw.Body.Bytes())
	assert.Equal(t, "foo", rw.HeaderMap.Get("X-Foo"))
	assert.Equal(t, "6", rw.HeaderMap.Get("Content-Length"))
}

func gzipped(s string) []byte {
	gbuf := new(bytes.Buffer)
	gw := gzip.NewWriter(gbuf)
	gw.Write([]byte(s))
	gw.Close()
	return gbuf.Bytes()
}

func gzipHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Foo", "foo")
	w.Header().Set("Content-Encoding", "gzip")
	w.WriteHeader(201)
	w.Write(gzipped("foo"))
}

func TestBufferedResponseWriterGZip(t *T) {
	rw := httptest.NewRecorder()
	brw := NewBufferedResponseWriter(rw)
	gzipHandler(brw, nil)

	bodyBuf, err := brw.GetBody()
	assert.Nil(t, err)
	bodyBufCp := bodyBufCopy(t, bodyBuf)

	// Make sure brw caught writes, and decompressed the body, and the recorder
	// didn't get any changes
	assert.Equal(t, []byte("foo"), bodyBufCp.Bytes())
	assert.Equal(t, "foo", brw.Header().Get("X-Foo"))
	assert.Equal(t, 201, brw.Code)
	assert.NotEqual(t, 201, rw.Code)
	assert.Equal(t, 0, rw.Body.Len())

	bodyBufCp.WriteString("bar")
	brw.SetBody(bodyBufCp)
	expected := gzipped("foobar")

	written, err := brw.ActuallyWrite()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(expected)), written)

	assert.Equal(t, 201, rw.Code)
	assert.Equal(t, expected, rw.Body.Bytes())
	assert.Equal(t, "foo", rw.HeaderMap.Get("X-Foo"))
	assert.Equal(t, strconv.Itoa(len(expected)), rw.HeaderMap.Get("Content-Length"))
}

func TestBufferedResponseWriterMarshalBinary(t *T) {
	rw := httptest.NewRecorder()
	brw := NewBufferedResponseWriter(rw)
	gzipHandler(brw, nil)
	expected := gzipped("foo")

	b, err := brw.MarshalBinary()
	require.Nil(t, err)

	written, err := brw.ActuallyWrite()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(expected)), written)

	assert.Equal(t, 201, rw.Code)
	assert.Equal(t, expected, rw.Body.Bytes())
	assert.Equal(t, "foo", rw.HeaderMap.Get("X-Foo"))
	assert.Equal(t, strconv.Itoa(len(expected)), rw.HeaderMap.Get("Content-Length"))

	// Now use the marshalled binary to make a new brw, and try ActuallyWrite
	// with that

	rw2 := httptest.NewRecorder()
	brw2 := NewBufferedResponseWriter(rw2)
	err = brw2.UnmarshalBinary(b)
	require.Nil(t, err)

	written, err = brw2.ActuallyWrite()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(expected)), written)

	assert.Equal(t, 201, rw2.Code)
	assert.Equal(t, expected, rw2.Body.Bytes())
	assert.Equal(t, "foo", rw2.HeaderMap.Get("X-Foo"))
	assert.Equal(t, strconv.Itoa(len(expected)), rw2.HeaderMap.Get("Content-Length"))
}
