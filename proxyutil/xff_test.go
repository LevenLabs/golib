package proxyutil

import (
	"net/http"
	"strings"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertXFF(t *T, addrExpect, addrIn string, forwards ...string) {
	r, err := http.NewRequest("GET", "/", nil)
	require.Nil(t, err)
	r.RemoteAddr = addrIn
	if len(forwards) > 0 {
		r.Header.Set("X-Forwarded-For", strings.Join(forwards, ", "))
	}
	out := RequestIP(r)
	assert.Equal(t, addrExpect, out, "addrIn: %q forwards: %v", addrIn, forwards)
}

func TestXFF(t *T) {
	// Some basic sanity checks
	assertXFF(t, "8.8.8.8", "8.8.8.8:2000")
	assertXFF(t, "::ffff:8.8.8.8", "[::ffff:8.8.8.8]:2000")

	// IPv4
	assertXFF(t, "8.8.8.8", "1.1.1.1:2000",
		"8.8.8.8")
	assertXFF(t, "1.1.1.1", "1.1.1.1:2000",
		"127.0.0.1")
	assertXFF(t, "1.1.1.1", "1.1.1.1:2000",
		"127.0.0.1", "192.168.1.1")
	assertXFF(t, "8.8.8.8", "1.1.1.1:2000",
		"127.0.0.1", "192.168.1.1", "8.8.8.8")
	assertXFF(t, "8.8.8.8", "1.1.1.1:2000",
		"127.0.0.1", "192.168.1.1", "8.8.8.8", "9.9.9.9")
	assertXFF(t, "192.168.1.1", "127.0.0.1:2000",
		"127.0.0.1", "192.168.1.1", "8.8.8.8")

	// IPv6
	assertXFF(t, "1::1", "1.1.1.1:2000",
		"1::1")
	assertXFF(t, "8.8.8.8", "1.1.1.1:2000",
		"::ffff:8.8.8.8")
	assertXFF(t, "1.1.1.1", "1.1.1.1:2000",
		"fd00::1")
	assertXFF(t, "1.1.1.1", "1.1.1.1:2000",
		"fd00::1", "::1")
	assertXFF(t, "1::1", "1.1.1.1:2000",
		"fd00::1", "::1", "1::1")
	assertXFF(t, "1::1", "1.1.1.1:2000",
		"fd00::1", "::1", "1::1", "2::2")
	assertXFF(t, "fd00::1", "[::1]:2000",
		"::1", "fd00::1", "1::1", "2::2")
}

func TestAddXForwardedFor(t *T) {
	xffCases := []struct {
		xffs     []string
		expected string
		addr     string
	}{
		{xffs: []string{}, expected: "", addr: "127.0.0.1:6666"},
		{xffs: []string{}, expected: "1.1.1.1", addr: "1.1.1.1:6666"},
		{xffs: []string{"1.1.1.1"}, expected: "1.1.1.1", addr: "127.0.0.1:6666"},
		{xffs: []string{"1.1.1.1, 2.2.2.2"}, expected: "1.1.1.1, 2.2.2.2", addr: "127.0.0.1:6666"},
		{xffs: []string{"1.1.1.1, 2.2.2.2", "3.3.3.3"}, expected: "1.1.1.1, 2.2.2.2, 3.3.3.3", addr: "127.0.0.1:6666"},
	}
	for _, xffCase := range xffCases {
		r, err := http.NewRequest("GET", "/", nil)
		require.Nil(t, err)
		r.RemoteAddr = xffCase.addr

		for _, xff := range xffCase.xffs {
			r.Header.Add("X-Forwarded-For", xff)
		}

		out, err := http.NewRequest("GET", "/", nil)
		require.Nil(t, err)
		AddXForwardedFor(out, r)
		assert.Equal(
			t, xffCase.expected, out.Header.Get("X-Forwarded-For"),
			"input headers: %v", xffCase.xffs,
		)
	}
}
