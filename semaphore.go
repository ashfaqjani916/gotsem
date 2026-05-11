package gotsem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Semaphore struct {
	rdb        redis.UniversalClient
	keyPrefix  string
	slotTTL    time.Duration
	defaultMax int
	planFn     func(ctx context.Context, ID string) (limit int, err error)
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

func NewGotsem(rdb redis.UniversalClient, keyPrefix string, slotTTL time.Duration, defaultMax int, planFn func(ctx context.Context, ID string) (limit int, err error)) *Semaphore {
	return &Semaphore{
		rdb:        rdb,
		keyPrefix:  keyPrefix,
		slotTTL:    slotTTL,
		defaultMax: defaultMax,
		planFn:     planFn,
	}
}

func (s *Semaphore) TryAcquire(ctx context.Context, ID string) AcquireResult {
	// max :=
	max := s.limitFor(ctx, ID)
	key := s.keyPrefix + ID

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

func (s *Semaphore) limitFor(ctx context.Context, ID string) int {
	if s.planFn == nil {
		return s.defaultMax
	}
	if v, ok := s.planCache.Load(ID); ok {
		e := v.(planCacheEntry)
		if time.Now().Before(e.exp) {
			return e.limit
		}
	}
	limit, err := s.planFn(ctx, ID)
	if err != nil {
		fmt.Errorf("Failed to get limit")
	}
	if limit <= 0 {
		limit = s.defaultMax
	}
	s.planCache.Store(ID, planCacheEntry{limit, time.Now().Add(time.Minute)})
	return limit
}

// Release frees the slot. Always call via defer.
// Uses context.WithoutCancel so it fires even if the request context was cancelled
// (client disconnected, timeout, etc).
func (s *Semaphore) Release(ctx context.Context, ID, slotID string) {
	if slotID == "failopen" {
		return // Redis was down on acquire, nothing to release
	}
	key := s.keyPrefix + ID
	now := time.Now().UnixMilli()

	luaRelease, err := LoadRelease()
	if err != nil {
		panic(err)
	}
	// Detach from request context — must succeed even if client disconnected
	luaRelease.Run(context.WithoutCancel(ctx), s.rdb, []string{key}, slotID, now)
}

// ActiveCount returns the number of currently held slots for a project.
// Useful for metrics/dashboards.
func (s *Semaphore) ActiveCount(ctx context.Context, ID string) int {
	key := s.keyPrefix + ID
	now := time.Now().UnixMilli()
	s.rdb.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(now, 10))
	count, _ := s.rdb.ZCard(ctx, key).Result()
	return int(count)
}

// ForceRelease drops all active slots for the given ID unconditionally.
// Use for admin/emergency drain — e.g. when a deploy is stuck or slots leaked.
func (s *Semaphore) ForceRelease(ctx context.Context, ID string) {
	key := s.keyPrefix + ID
	s.rdb.Del(context.WithoutCancel(ctx), key)
}

func newSlotID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "slot_" + hex.EncodeToString(b)
}
