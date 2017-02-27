package proxyutil

import (
	"net"
	"net/http"
	"strings"
)

func mustGetCIDRNetwork(cidr string) *net.IPNet {
	_, n, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	return n
}

var internalCIDRs4 = []*net.IPNet{
	mustGetCIDRNetwork("10.0.0.0/8"),
	mustGetCIDRNetwork("172.16.0.0/12"),
	mustGetCIDRNetwork("192.168.0.0/16"),
	mustGetCIDRNetwork("169.254.0.0/16"),
}

var internalCIDRs6 = []*net.IPNet{
	mustGetCIDRNetwork("fd00::/8"),
}

func ipIsPrivate(ip net.IP) bool {
	cidrs := internalCIDRs4
	if ip.To4() == nil {
		cidrs = internalCIDRs6
	}
	for _, cidr := range cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// RequestIP returns the string form of the original requester's IP address for
// the given request, taking into account X-Forwarded-For if applicable.
// If the request was from a loopback address, then we will take the first
// non-loopback X-Forwarded-For address. This is under the assumption that
// your golang server is running behind a reverse proxy.
func RequestIP(r *http.Request) string {
	reqIP, _, _ := net.SplitHostPort(r.RemoteAddr)

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return reqIP
	}

	origIP := net.ParseIP(reqIP)
	ips := strings.Split(xff, ",")
	for i := range ips {
		ip := net.ParseIP(strings.TrimSpace(ips[i]))
		if ip == nil || ip.IsLoopback() {
			continue
		}
		// accept any XFF private or not if the request's IP is a loopback IP
		// because if you're running golang behind a local reverse proxy, the
		// request's IP will always be loopback
		if ipIsPrivate(ip) && !origIP.IsLoopback() {
			continue
		}
		reqIP = ip.String()
		break
	}

	return reqIP
}

func addXForwardedForHelper(dst *http.Request, existing []string, ip string) {
	ipp := net.ParseIP(ip)
	if ipp != nil && !ipp.IsLoopback() {
		existing = append(existing, ip)
	}
	if len(existing) > 0 {
		dst.Header.Set("X-Forwarded-For", strings.Join(existing, ", "))
	}
}

// AddXForwardedFor populates the X-Forwarded-For header on dst to convey that
// src is being proxied by this server. dst and src may be the same pointer.
func AddXForwardedFor(dst, src *http.Request) {
	rIP, _, _ := net.SplitHostPort(src.RemoteAddr)
	xff, _ := src.Header["X-Forwarded-For"]
	addXForwardedForHelper(dst, xff, rIP)
}

// AddXForwardedForIP populates the X-Forwarded-For header on dst to convey that
// ip is making the request.
func AddXForwardedForIP(dst *http.Request, ip string) {
	xff, _ := dst.Header["X-Forwarded-For"]
	addXForwardedForHelper(dst, xff, ip)
}
