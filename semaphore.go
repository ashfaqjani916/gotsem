package gotsem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Semaphore struct {
	rdb        redis.UniversalClient
	keyPrefix  string
	slotTTL    time.Duration
	defaultMax int
	planFn     func(ctx context.Context, projectID string) int
	// local cache so planFn isn't called on every request
	planCache sync.Map // projectID → planCacheEntry
}

type AcquireResult struct {
	Acquired bool
	SlotID   string
	Active   int // current active slots after this operation
	Max      int // max limit allowed for this project
}

type planCacheEntry struct {
	limit int
	exp   time.Time
}

func NewGotsem(rdb redis.UniversalClient, keyPrefix string, slotTTL time.Duration, defaultMax int, planFn func(ctx context.Context, projectID string) int) *Semaphore {
	return &Semaphore{
		rdb:        rdb,
		keyPrefix:  keyPrefix,
		slotTTL:    slotTTL,
		defaultMax: defaultMax,
		planFn:     planFn,
	}
}

func (s *Semaphore) TryAcquire(ctx context.Context, projectID string) AcquireResult {
	// max :=
	max := s.limitFor(ctx, projectID)
	key := s.keyPrefix + projectID

	now := time.Now().UnixMilli()
	expiry := now + s.slotTTL.Milliseconds()
	keyTTL := s.slotTTL.Milliseconds() + 10_000 // 10s buffer

	slotID := newSlotID()

	luaAcquire, err := LoadAquire()
	if err != nil {
		panic(err)
	}

	result, err := luaAcquire.Run(ctx, s.rdb,
		[]string{key},
		now, expiry, slotID, max, keyTTL,
	).Int64Slice()

	if err != nil {
		// Redis down → fail-open, return a sentinel slotID so Release is a no-op
		return AcquireResult{Acquired: true, SlotID: "failopen", Active: 0, Max: max}
	}

	return AcquireResult{
		Acquired: result[0] == 1,
		SlotID:   slotID,
		Active:   int(result[1]),
		Max:      max,
	}
}

func (s *Semaphore) limitFor(ctx context.Context, projectID string) int {
	if s.planFn == nil {
		return s.defaultMax
	}
	if v, ok := s.planCache.Load(projectID); ok {
		e := v.(planCacheEntry)
		if time.Now().Before(e.exp) {
			return e.limit
		}
	}
	limit := s.planFn(ctx, projectID)
	if limit <= 0 {
		limit = s.defaultMax
	}
	s.planCache.Store(projectID, planCacheEntry{limit, time.Now().Add(time.Minute)})
	return limit
}

func newSlotID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "slot_" + hex.EncodeToString(b)
}
