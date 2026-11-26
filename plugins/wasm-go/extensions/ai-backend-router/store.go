package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/decisions"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
)

// tickPeriodMs is how often a VM checks shared data for a newer snapshot and,
// if it holds the lease, whether a pull is due. Must be a multiple of 100.
const tickPeriodMs = 1000

// sourceConfig describes the decision service.
type sourceConfig struct {
	serviceName string
	servicePort int64
	host        string
	path        string
	intervalMs  int64
	timeoutMs   int64
	maxStaleMs  int64
}

func (s *sourceConfig) key() string {
	return fmt.Sprintf("ai-backend-router/%s:%d%s", s.serviceName, s.servicePort, s.path)
}

// httpGetter is the part of wrapper.ClusterClient the store uses.
type httpGetter interface {
	Get(rawURL string, headers [][2]string, cb wrapper.ResponseCallback, timeoutMillisecond ...uint32) error
}

// decisionStore holds the snapshot that requests route by.
//
// Without a source it only ever serves the static snapshot. With a source,
// one VM per gateway process (the lease holder) pulls the document and writes
// it to shared data; every VM, including the holder, adopts a new snapshot
// from shared data on its next tick. A wasm VM is single-threaded, so the
// store needs no locking.
type decisionStore struct {
	current *decisions.Snapshot
	source  *sourceConfig
	client  httpGetter
	metrics *pluginMetrics
	now     func() time.Time

	holderID      string
	adoptedCas    uint32
	lastPullStart int64
	pulling       bool
	startedMs     int64
	lastStaleLog  int64
	lastFailLog   int64
}

func newDecisionStore(static *decisions.Snapshot, source *sourceConfig, metrics *pluginMetrics) *decisionStore {
	store := &decisionStore{current: static, source: source, metrics: metrics, now: time.Now}
	store.startedMs = store.now().UnixMilli()
	if source != nil {
		store.holderID = randomID()
		store.client = wrapper.NewClusterClient(wrapper.FQDNCluster{FQDN: source.serviceName, Port: source.servicePort, Host: source.host})
	}
	return store
}

func (s *decisionStore) snapshot() *decisions.Snapshot {
	return s.current
}

type pullMeta struct {
	FetchedAtMs int64  `json:"fetchedAtMs"`
	ETag        string `json:"etag,omitempty"`
}

type pullLease struct {
	Holder string `json:"holder"`
	AtMs   int64  `json:"atMs"`
}

func (s *decisionStore) docKey() string   { return s.source.key() + "/doc" }
func (s *decisionStore) metaKey() string  { return s.source.key() + "/meta" }
func (s *decisionStore) leaseKey() string { return s.source.key() + "/lease" }

// tick runs on every VM.
func (s *decisionStore) tick() {
	if s.source == nil {
		return
	}
	now := s.now().UnixMilli()
	s.adopt()
	s.reportAge(now)
	if !s.pulling && now-s.lastPullStart >= s.source.intervalMs && s.holdLease(now) {
		s.pull(now)
	}
}

// adopt switches to the document in shared data if it changed since the last
// adoption. The holder validated it before writing; it is validated again
// here so a corrupt entry can never replace a good snapshot.
func (s *decisionStore) adopt() {
	raw, cas, err := proxywasm.GetSharedData(s.docKey())
	if err != nil || len(raw) == 0 || cas == s.adoptedCas {
		return
	}
	snapshot, err := decisions.Parse(raw)
	if err != nil {
		log.Errorf("[ai-backend-router] shared decisions document is invalid, keeping version %q: %v", s.current.Version, err)
		s.adoptedCas = cas
		return
	}
	if snapshot.Version != s.current.Version {
		log.Infof("[ai-backend-router] decisions version %q -> %q", s.current.Version, snapshot.Version)
	}
	s.current = snapshot
	s.adoptedCas = cas
}

func (s *decisionStore) reportAge(now int64) {
	fetchedAt := s.startedMs
	if raw, _, err := proxywasm.GetSharedData(s.metaKey()); err == nil && len(raw) > 0 {
		var meta pullMeta
		if json.Unmarshal(raw, &meta) == nil && meta.FetchedAtMs > 0 {
			fetchedAt = meta.FetchedAtMs
		}
	}
	age := now - fetchedAt
	s.metrics.decisionsAge(age)
	if age > s.source.maxStaleMs && now-s.lastStaleLog >= 60_000 {
		s.lastStaleLog = now
		log.Errorf("[ai-backend-router] decisions are stale: last successful pull %d ms ago (maxStaleMs %d); still routing by version %q", age, s.source.maxStaleMs, s.current.Version)
	}
}

// holdLease acquires or renews the per-process pull lease. A holder that
// stops renewing (VM gone) loses the lease after a few intervals.
func (s *decisionStore) holdLease(now int64) bool {
	raw, cas, err := proxywasm.GetSharedData(s.leaseKey())
	if err != nil && !errors.Is(err, types.ErrorStatusNotFound) {
		return false
	}
	if err == nil && len(raw) > 0 {
		var lease pullLease
		if json.Unmarshal(raw, &lease) == nil && lease.Holder != s.holderID && now-lease.AtMs < s.leaseExpiryMs() {
			return false
		}
	}
	value, _ := json.Marshal(pullLease{Holder: s.holderID, AtMs: now})
	return proxywasm.SetSharedData(s.leaseKey(), value, cas) == nil
}

func (s *decisionStore) leaseExpiryMs() int64 {
	return max(3*s.source.intervalMs+s.source.timeoutMs, 10_000)
}

func (s *decisionStore) pull(now int64) {
	s.lastPullStart = now
	headers := [][2]string{{"accept", "application/json"}}
	if raw, _, err := proxywasm.GetSharedData(s.metaKey()); err == nil && len(raw) > 0 {
		var meta pullMeta
		if json.Unmarshal(raw, &meta) == nil && meta.ETag != "" {
			if docRaw, _, docErr := proxywasm.GetSharedData(s.docKey()); docErr == nil && len(docRaw) > 0 {
				headers = append(headers, [2]string{"if-none-match", meta.ETag})
			}
		}
	}
	s.pulling = true
	err := s.client.Get(s.source.path, headers, func(status int, responseHeaders http.Header, body []byte) {
		s.pulling = false
		s.onPullResponse(status, responseHeaders, body)
	}, uint32(s.source.timeoutMs))
	if err != nil {
		s.pulling = false
		s.pullFailed(fmt.Sprintf("dispatch failed: %v", err))
	}
}

func (s *decisionStore) onPullResponse(status int, responseHeaders http.Header, body []byte) {
	now := s.now().UnixMilli()
	switch {
	case status == http.StatusNotModified:
		s.writeMeta(now, responseHeaders.Get("etag"))
	case status == http.StatusOK:
		snapshot, err := decisions.Parse(body)
		if err != nil {
			s.pullFailed(fmt.Sprintf("invalid document: %v", err))
			return
		}
		if err := putSharedData(s.docKey(), body); err != nil {
			s.pullFailed(fmt.Sprintf("cannot store document: %v", err))
			return
		}
		s.writeMeta(now, responseHeaders.Get("etag"))
		log.Debugf("[ai-backend-router] pulled decisions version %q", snapshot.Version)
		s.adopt()
	default:
		s.pullFailed("status " + strconv.Itoa(status))
	}
}

func (s *decisionStore) writeMeta(now int64, etag string) {
	value, _ := json.Marshal(pullMeta{FetchedAtMs: now, ETag: etag})
	if err := putSharedData(s.metaKey(), value); err != nil {
		log.Warnf("[ai-backend-router] cannot store pull metadata: %v", err)
	}
}

func (s *decisionStore) pullFailed(reason string) {
	s.metrics.pullFailure()
	now := s.now().UnixMilli()
	if now-s.lastFailLog >= 10_000 {
		s.lastFailLog = now
		log.Warnf("[ai-backend-router] decisions pull from %s failed (%s); keeping version %q", s.source.key(), reason, s.current.Version)
	}
}

// putSharedData overwrites a key. It writes with the key's current cas
// rather than relying on cas 0 meaning "unconditional", and retries once if
// another VM wrote in between.
func putSharedData(key string, value []byte) error {
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		_, cas, getErr := proxywasm.GetSharedData(key)
		if getErr != nil && !errors.Is(getErr, types.ErrorStatusNotFound) {
			return getErr
		}
		if err = proxywasm.SetSharedData(key, value, cas); err == nil {
			return nil
		}
	}
	return err
}

func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
