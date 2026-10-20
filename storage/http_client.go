package storage

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Tuned HTTP transport for bulk transfers.
//
// Go's default transport keeps only 2 idle connections per host, so with dozens of parallel part
// requests connections are constantly closed and re-dialled (a TCP + TLS handshake per request).
// Like the AWS CLI / rclone, keep a large idle pool, use bigger I/O buffers and stay on HTTP/1.1:
// one TCP connection per part keeps every stream's full congestion window, where HTTP/2 would
// multiplex all parts onto a single connection.
const (
	transportIOBufferSize = 256 << 10
	maxIdleConnsPerHost   = 1024
	// rateLimitChunk caps a single throttled read/write so the limiter stays smooth.
	rateLimitChunk = 32 << 10
)

var bandwidthLimiter atomic.Pointer[tokenBucket]

// SetBandwidthLimit caps the total transfer rate of this process in bytes per second, across all
// connections and both directions (0 = unlimited). Must be called before the first request.
func SetBandwidthLimit(bytesPerSec int64) {
	if bytesPerSec <= 0 {
		bandwidthLimiter.Store(nil)
		return
	}
	bandwidthLimiter.Store(newTokenBucket(float64(bytesPerSec)))
}

func newHTTPClient(noVerifySSL bool) *http.Client {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if host, _, _ := net.SplitHostPort(addr); host == "169.254.169.254" || host == "fd00:ec2::254" {
				// EC2 instance metadata (credential chain fallback). The SDK only applies its 1 s timeout
				// with the default client; keep it, or non-EC2 machines stall for minutes without keys.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if b := bandwidthLimiter.Load(); b != nil {
				return &throttledConn{Conn: conn, bucket: b}, nil
			}
			return conn, nil
		},
		MaxIdleConns:          0, // no global cap
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: time.Second,
		WriteBufferSize:       transportIOBufferSize,
		ReadBufferSize:        transportIOBufferSize,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: noVerifySSL, //nolint:gosec // explicit opt-in via --no-verify-ssl
		},
		// Non-nil empty map disables HTTP/2 (see above).
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	return &http.Client{Transport: transport}
}

// tokenBucket is a reservation-based token bucket (the model used by golang.org/x/time/rate and
// rclone's --bwlimit): callers reserve n bytes and sleep until the bucket has produced them.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // bytes per second
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64) *tokenBucket {
	burst := rate / 10 // 100 ms worth of traffic
	if burst < 2*rateLimitChunk {
		burst = 2 * rateLimitChunk
	}
	return &tokenBucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

func (b *tokenBucket) wait(n int) {
	b.mu.Lock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	b.tokens -= float64(n)
	var delay time.Duration
	if b.tokens < 0 {
		delay = time.Duration(-b.tokens / b.rate * float64(time.Second))
	}
	b.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
}

type throttledConn struct {
	net.Conn
	bucket *tokenBucket
}

func (c *throttledConn) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := written + rateLimitChunk
		if end > len(p) {
			end = len(p)
		}
		c.bucket.wait(end - written)
		n, err := c.Conn.Write(p[written:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (c *throttledConn) Read(p []byte) (int, error) {
	if len(p) > rateLimitChunk {
		p = p[:rateLimitChunk]
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.bucket.wait(n)
	}
	return n, err
}
