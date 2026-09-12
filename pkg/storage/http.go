package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// S3Credentials configure Signature Version 4 authentication for an
// S3-compatible endpoint whose URL already includes the bucket path.
type S3Credentials struct {
	AccessKey, SecretKey, SessionToken, Region string
}

// NewS3 creates an S3-compatible store with path-style object URLs.
func NewS3(endpoint string, credentials S3Credentials, client *http.Client) (*HTTP, error) {
	if credentials.AccessKey == "" || credentials.SecretKey == "" || credentials.Region == "" {
		return nil, fmt.Errorf("S3 access key, secret key, and region are required")
	}
	baseTransport := http.DefaultTransport
	if client == nil {
		client = &http.Client{}
	}
	if client.Transport != nil {
		baseTransport = client.Transport
	}
	copy := *client
	copy.Transport = &s3SigningTransport{credentials: credentials, base: baseTransport, now: time.Now}
	return NewHTTP(endpoint, &copy)
}

type s3SigningTransport struct {
	credentials S3Credentials
	base        http.RoundTripper
	now         func() time.Time
}

func (t *s3SigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	payloadHash := version(body)
	now := t.now().UTC()
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + req.URL.Host + "\n" + "x-amz-content-sha256:" + payloadHash + "\n" + "x-amz-date:" + stamp + "\n"
	if t.credentials.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", t.credentials.SessionToken)
		canonicalHeaders += "x-amz-security-token:" + t.credentials.SessionToken + "\n"
		signedHeaders += ";x-amz-security-token"
	}
	canonical := req.Method + "\n" + s3CanonicalURI(req.URL.Path) + "\n" + req.URL.Query().Encode() + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash
	scope := day + "/" + t.credentials.Region + "/s3/aws4_request"
	canonicalSum := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(canonicalSum[:])
	dateKey := hmacSHA256([]byte("AWS4"+t.credentials.SecretKey), day)
	regionKey := hmacSHA256(dateKey, t.credentials.Region)
	serviceKey := hmacSHA256(regionKey, "s3")
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+t.credentials.AccessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
	return t.base.RoundTrip(req)
}

func s3CanonicalURI(path string) string {
	var encoded strings.Builder
	for i := 0; i < len(path); i++ {
		b := path[i]
		if b == '/' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-._~", rune(b)) {
			encoded.WriteByte(b)
			continue
		}
		fmt.Fprintf(&encoded, "%%%02X", b)
	}
	return encoded.String()
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

// HTTP stores objects through the S3-compatible object HTTP operations. The
// supplied client must add any authentication required by the endpoint.
type HTTP struct {
	base   *url.URL
	client *http.Client
}

const defaultRequestTimeout = 30 * time.Second

func NewHTTP(endpoint string, client *http.Client) (*HTTP, error) {
	base, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil {
		return nil, err
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("object-store endpoint must be an absolute URL")
	}
	if client == nil {
		client = http.DefaultClient
	}
	clientCopy := *client
	if clientCopy.Timeout <= 0 {
		clientCopy.Timeout = defaultRequestTimeout
	}
	return &HTTP{base: base, client: &clientCopy}, nil
}

func (s *HTTP) objectURL(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("invalid storage key %q", key)
	}
	u := *s.base
	u.Path += "/" + strings.Join(strings.Split(key, "/"), "/")
	return u.String(), nil
}

func (s *HTTP) request(ctx context.Context, method, key string, body []byte, condition, value string) (*http.Response, error) {
	u, err := s.objectURL(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if condition != "" {
		req.Header.Set(condition, value)
	}
	return s.client.Do(req)
}

func (s *HTTP) PutImmutable(ctx context.Context, key string, body []byte) error {
	resp, err := s.request(ctx, http.MethodPut, key, body, "If-None-Match", "*")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed || resp.StatusCode == http.StatusConflict {
		return ErrConflict
	}
	return statusError(resp)
}

func (s *HTTP) Get(ctx context.Context, key string) (Object, error) {
	return s.GetBounded(ctx, key, -1)
}

// GetBounded reads at most maxBytes plus one byte, so an oversized response
// is rejected without first allocating its complete body. A negative limit
// preserves the Store.Get behavior for small metadata objects.
func (s *HTTP) GetBounded(ctx context.Context, key string, maxBytes int64) (Object, error) {
	resp, err := s.request(ctx, http.MethodGet, key, nil, "", "")
	if err != nil {
		return Object{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return Object{}, ErrNotFound
	}
	if err := statusError(resp); err != nil {
		return Object{}, err
	}
	reader := io.Reader(resp.Body)
	if maxBytes >= 0 {
		if resp.ContentLength > maxBytes {
			return Object{}, fmt.Errorf("%w: content length %d exceeds %d", ErrObjectTooLarge, resp.ContentLength, maxBytes)
		}
		reader = io.LimitReader(resp.Body, maxBytes+1)
	}
	body, err := io.ReadAll(reader)
	if err == nil && maxBytes >= 0 && int64(len(body)) > maxBytes {
		return Object{}, fmt.Errorf("%w: body exceeds %d bytes", ErrObjectTooLarge, maxBytes)
	}
	return Object{Bytes: body, Version: resp.Header.Get("ETag")}, err
}

func (s *HTTP) CompareAndSwap(ctx context.Context, key, oldVersion string, body []byte) (string, error) {
	header, value := "If-Match", oldVersion
	if oldVersion == "" {
		header, value = "If-None-Match", "*"
	}
	resp, err := s.request(ctx, http.MethodPut, key, body, header, value)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed || resp.StatusCode == http.StatusConflict {
		return "", ErrConflict
	}
	if err := statusError(resp); err != nil {
		return "", err
	}
	return resp.Header.Get("ETag"), nil
}

func (s *HTTP) Delete(ctx context.Context, key string) error {
	resp, err := s.request(ctx, http.MethodDelete, key, nil, "", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return statusError(resp)
}

func statusError(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		return fmt.Errorf("object store returned %s", resp.Status)
	}
	return fmt.Errorf("object store returned %s: %s", resp.Status, detail)
}
