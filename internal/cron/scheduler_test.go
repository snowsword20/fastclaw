package cron

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/bus"
)

// mockStore implements StoreInterface for testing
type mockStore struct {
	mu   sync.Mutex
	jobs []StoreJob

	locked  map[string]bool
	deleted map[string]bool
	updated map[string]time.Time // jobID → nextRun

	// agentChannels maps "agentID|channelType" → list of account_ids
	// that ListAgentChannels should report as enabled. Populated by tests
	// via setAgentChannels.
	agentChannels map[string][]string
	// rekeyed records jobID → newAccountID for UpdateCronJobAccountByID
	// calls, so a test can assert the self-heal persisted the rewrite.
	rekeyed map[string]string
}

func newMockStore() *mockStore {
	return &mockStore{
		locked:        make(map[string]bool),
		deleted:       make(map[string]bool),
		updated:       make(map[string]time.Time),
		agentChannels: make(map[string][]string),
		rekeyed:       make(map[string]string),
	}
}

func (m *mockStore) addJob(j StoreJob) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = append(m.jobs, j)
}

func (m *mockStore) GetDueCronJobs(ctx context.Context, now time.Time) ([]StoreJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return jobs whose "next_run" would be <= now
	// For simplicity, return all non-deleted jobs (the scheduler handles timing)
	var out []StoreJob
	for _, j := range m.jobs {
		if !m.deleted[j.ID] {
			out = append(out, j)
		}
	}
	return out, nil
}

func (m *mockStore) LockCronJob(ctx context.Context, jobID, instanceID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locked[jobID] {
		return false, nil
	}
	m.locked[jobID] = true
	return true, nil
}

func (m *mockStore) UpdateCronJobRun(ctx context.Context, jobID string, lastRun, nextRun time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updated[jobID] = nextRun
	m.locked[jobID] = false
	return nil
}

func (m *mockStore) DeleteCronJob(ctx context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted[jobID] = true
	return nil
}

func (m *mockStore) GetNextDueTime(ctx context.Context) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var earliest time.Time
	for _, j := range m.jobs {
		if !m.deleted[j.ID] {
			if next, ok := m.updated[j.ID]; ok {
				if earliest.IsZero() || next.Before(earliest) {
					earliest = next
				}
			}
		}
	}
	return earliest, nil
}

func (m *mockStore) IncrementCronJobFailure(ctx context.Context, jobID string) (int, error) {
	return 1, nil
}

func (m *mockStore) ListAgentChannels(ctx context.Context, agentID, channelType string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.agentChannels[agentID+"|"+channelType], nil
}

func (m *mockStore) UpdateCronJobAccountByID(ctx context.Context, jobID, newAccountID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rekeyed[jobID] = newAccountID
	return nil
}

// setAgentChannels configures which account_ids ListAgentChannels reports
// as enabled for the given (agentID, channelType) pair.
func (m *mockStore) setAgentChannels(agentID, channelType string, accounts ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.agentChannels[agentID+"|"+channelType] = accounts
}

func (m *mockStore) isDeleted(jobID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleted[jobID]
}

func (m *mockStore) getNextRun(jobID string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.updated[jobID]
	return t, ok
}

func TestProcessDueJobs_Once(t *testing.T) {
	mb := bus.New()
	go func() {
		for range mb.Inbound {
			// drain
		}
	}()

	store := newMockStore()
	store.addJob(StoreJob{
		ID:      "once-1",
		Name:    "test once",
		Type:    "once",
		Message: "remind me",
		Channel: "web",
	})

	s := &Scheduler{
		bus:        mb,
		store:      store,
		instanceID: "test",
	}

	ctx := context.Background()
	s.processDueJobs(ctx)

	if !store.isDeleted("once-1") {
		t.Error("once job should be deleted after firing")
	}
}

func TestProcessDueJobs_Interval(t *testing.T) {
	mb := bus.New()
	go func() {
		for range mb.Inbound {
		}
	}()

	store := newMockStore()
	store.addJob(StoreJob{
		ID:       "interval-1",
		Name:     "test interval",
		Type:     "interval",
		Schedule: "30m",
		Message:  "check something",
		Channel:  "web",
	})

	s := &Scheduler{
		bus:        mb,
		store:      store,
		instanceID: "test",
	}

	ctx := context.Background()
	s.processDueJobs(ctx)

	nextRun, ok := store.getNextRun("interval-1")
	if !ok {
		t.Fatal("interval job should have nextRun set")
	}
	expected := time.Now().Add(30 * time.Minute)
	diff := nextRun.Sub(expected)
	if diff < -time.Minute || diff > time.Minute {
		t.Errorf("interval nextRun should be ~30m from now, got diff=%v", diff)
	}
}

func TestProcessDueJobs_IntervalWithEveryPrefix(t *testing.T) {
	mb := bus.New()
	go func() {
		for range mb.Inbound {
		}
	}()

	store := newMockStore()
	store.addJob(StoreJob{
		ID:       "interval-2",
		Name:     "every prefix",
		Type:     "interval",
		Schedule: "every 5m",
		Message:  "check",
		Channel:  "web",
	})

	s := &Scheduler{
		bus:        mb,
		store:      store,
		instanceID: "test",
	}

	ctx := context.Background()
	s.processDueJobs(ctx)

	nextRun, ok := store.getNextRun("interval-2")
	if !ok {
		t.Fatal("interval job with 'every' prefix should have nextRun set")
	}
	expected := time.Now().Add(5 * time.Minute)
	diff := nextRun.Sub(expected)
	if diff < -time.Minute || diff > time.Minute {
		t.Errorf("interval nextRun should be ~5m from now, got diff=%v", diff)
	}
}

func TestProcessDueJobs_Cron(t *testing.T) {
	mb := bus.New()
	go func() {
		for range mb.Inbound {
		}
	}()

	store := newMockStore()
	store.addJob(StoreJob{
		ID:       "cron-1",
		Name:     "daily 9am",
		Type:     "cron",
		Schedule: "0 9 * * *",
		Message:  "morning",
		Channel:  "web",
	})

	s := &Scheduler{
		bus:        mb,
		store:      store,
		instanceID: "test",
	}

	ctx := context.Background()
	s.processDueJobs(ctx)

	nextRun, ok := store.getNextRun("cron-1")
	if !ok {
		t.Fatal("cron job should have nextRun set")
	}
	if nextRun.Before(time.Now()) {
		t.Error("cron nextRun should be in the future")
	}
	if nextRun.Hour() != 9 || nextRun.Minute() != 0 {
		t.Errorf("cron nextRun should be at 9:00, got %v", nextRun)
	}
}

func TestNextCronOccurrence(t *testing.T) {
	// Test "every 2 minutes" cron
	now := time.Date(2026, 5, 6, 10, 3, 0, 0, time.UTC)
	next := nextCronOccurrence("*/2 * * * *", now)
	if next.Minute() != 4 {
		t.Errorf("expected minute=4, got %d (time=%v)", next.Minute(), next)
	}

	// Test "daily at 9:00"
	now = time.Date(2026, 5, 6, 9, 1, 0, 0, time.UTC)
	next = nextCronOccurrence("0 9 * * *", now)
	if next.Day() != 7 || next.Hour() != 9 {
		t.Errorf("expected next day 9:00, got %v", next)
	}
}

func TestNotifyJobCreated(t *testing.T) {
	// Drain any existing notification
	select {
	case <-globalNotify:
	default:
	}

	NotifyJobCreated()

	select {
	case <-globalNotify:
		// good
	default:
		t.Error("globalNotify should have a pending notification")
	}

	// Second call should not block
	NotifyJobCreated()
	NotifyJobCreated() // should not panic or block
}

// mockChannelChecker implements ChannelChecker. live maps
// "channel:accountID" → registered. A key present means Has returns true.
type mockChannelChecker struct {
	live map[string]bool
}

func (c *mockChannelChecker) Has(channel, accountID string) bool {
	return c.live[channel+":"+accountID]
}

// TestSchedulerSelfHealOnRebind covers the rebind scenario: a cron job
// was created under an account_id that's since been replaced (the bot was
// rebound and minted a fresh account_id). The pre-flight Has() on the old
// account_id fails, so the scheduler should look up a still-live bot under
// the same channel type, fire against it, and persist the re-key — instead
// of bumping the failure counter toward auto-delete.
func TestSchedulerSelfHealOnRebind(t *testing.T) {
	mb := bus.New()
	// Capture the fired inbound so we can assert the account_id it carries.
	var fired *bus.InboundMessage
	var fireMu sync.Mutex
	go func() {
		for msg := range mb.Inbound {
			fireMu.Lock()
			f := msg
			fired = &f
			fireMu.Unlock()
		}
	}()

	store := newMockStore()
	// Job frozen with the OLD account_id that no longer resolves.
	store.addJob(StoreJob{
		ID:        "daily-1",
		Name:      "alarm",
		Type:      "cron",
		Schedule:  "0 7 * * *",
		Message:   "wake up",
		Channel:   "wechat",
		AccountID: "OLD_bot@im.bot",
		AgentID:   "agt_1",
		ChatID:    "openid-1",
	})
	// The agent now has a live bot under a NEW account_id.
	store.setAgentChannels("agt_1", "wechat", "NEW_bot@im.bot")

	// Channel registry: only the NEW bot is registered; the OLD one was
	// torn down by the rebind.
	checker := &mockChannelChecker{live: map[string]bool{
		"wechat:NEW_bot@im.bot": true,
	}}

	s := &Scheduler{
		bus:        mb,
		store:      store,
		channels:   checker,
		instanceID: "test",
	}

	s.processDueJobs(context.Background())

	// Give the drain goroutine a moment to record the fired message.
	deadline := time.Now().Add(time.Second)
	for {
		fireMu.Lock()
		done := fired != nil
		fireMu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	fireMu.Lock()
	defer fireMu.Unlock()
	if fired == nil {
		t.Fatal("expected the job to fire after self-heal, but no inbound was emitted")
	}
	if fired.AccountID != "NEW_bot@im.bot" {
		t.Errorf("fired inbound should carry the healed account_id; got %q", fired.AccountID)
	}
	// The re-key should have been persisted so the next tick skips the fallback.
	if got := store.rekeyed["daily-1"]; got != "NEW_bot@im.bot" {
		t.Errorf("expected job account_id persisted as NEW_bot@im.bot, got %q", got)
	}
	// And it must NOT have been counted as a failure (no auto-delete path).
	if store.isDeleted("daily-1") {
		t.Error("self-healed job should not be deleted")
	}
}

// TestSchedulerNoSelfHealWhenNoLiveBot confirms that when there is no live
// bot under the channel type at all, the scheduler falls back to the
// failure-counter / auto-delete path rather than firing blindly.
func TestSchedulerNoSelfHealWhenNoLiveBot(t *testing.T) {
	mb := bus.New()
	go func() {
		for range mb.Inbound {
		}
	}()

	store := newMockStore()
	store.addJob(StoreJob{
		ID:        "daily-2",
		Name:      "alarm",
		Type:      "cron",
		Schedule:  "0 7 * * *",
		Message:   "wake up",
		Channel:   "wechat",
		AccountID: "DEAD_bot@im.bot",
		AgentID:   "agt_2",
		ChatID:    "openid-2",
	})
	// No live bots reported for this agent/channel.
	store.setAgentChannels("agt_2", "wechat")

	checker := &mockChannelChecker{live: map[string]bool{}}

	s := &Scheduler{
		bus:        mb,
		store:      store,
		channels:   checker,
		instanceID: "test",
	}

	s.processDueJobs(context.Background())

	// IncrementCronJobFailure returns 1 (below threshold of 3), so the job
	// is skipped-but-kept, not deleted.
	if store.isDeleted("daily-2") {
		t.Error("job should not be deleted on first miss (below threshold)")
	}
	if got := store.rekeyed["daily-2"]; got != "" {
		t.Errorf("no self-heal expected when no live bot exists; got rekey %q", got)
	}
}
