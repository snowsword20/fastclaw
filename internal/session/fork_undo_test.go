package session

import (
	"fmt"
	"strings"
	"testing"

	"github.com/fastclaw-ai/fastclaw/internal/provider"
)

// fork_undo_test.go 固化 2026-11 ⑤ 的两个能力：
//   - 快照栈：多档 /undo，栈深上限，标签按新→旧出；
//   - ForkSession / SwitchBackSession：平行时间线 + activeOverride 回切。

func msg(role, content string) provider.Message {
	return provider.Message{Role: role, Content: content, Timestamp: 1}
}

func TestSnapshotStack_MultiLevelUndo(t *testing.T) {
	m := NewManager(t.TempDir())
	s := m.Get("web", "", "chat-fork-test", "")

	s.Append(msg("user", "q1"))
	s.SnapshotLabeled("turn: q1")
	s.Append(msg("assistant", "a1"))

	s.Append(msg("user", "q2"))
	s.SnapshotLabeled("turn: q2")
	s.Append(msg("assistant", "a2"))

	if got := len(s.SnapshotLabels()); got != 2 {
		t.Fatalf("want 2 snapshot labels, got %d: %v", got, s.SnapshotLabels())
	}
	// 最新的档排最前（标签格式是 "<label> (<n> msgs, <time>)"，比前缀）
	if labels := s.SnapshotLabels(); !strings.HasPrefix(labels[0], "turn: q2") {
		t.Fatalf("newest label should be first, got %v", labels)
	}

	// 第一次 undo：回到 "turn: q2" 快照 —— 那一刻 q2 已入列，
	// 状态 = [q1, a1, q2]（3 条），只少了 a2
	if label, ok := s.Undo(); !ok || label != "turn: q2" {
		t.Fatalf("first undo = (%q,%v), want (turn: q2,true)", label, ok)
	}
	if got := len(s.GetMessages()); got != 3 {
		t.Fatalf("after first undo want 3 msgs, got %d", got)
	}
	// 第二次 undo：回到 "turn: q1" 快照 —— 那一刻只有 [q1]
	if label, ok := s.Undo(); !ok || label != "turn: q1" {
		t.Fatalf("second undo = (%q,%v), want (turn: q1,true)", label, ok)
	}
	if got := len(s.GetMessages()); got != 1 {
		t.Fatalf("after second undo want 1 msg, got %d", got)
	}
	// 栈空
	if _, ok := s.Undo(); ok {
		t.Fatal("third undo should fail on empty stack")
	}
	if s.HasSnapshot() {
		t.Fatal("HasSnapshot should be false after draining the stack")
	}
}

func TestSnapshotStack_CappedAtLimit(t *testing.T) {
	m := NewManager(t.TempDir())
	s := m.Get("web", "", "chat-cap-test", "")
	for i := 0; i < undoSnapshotLimit+3; i++ {
		s.SnapshotLabeled("snap")
	}
	if got := len(s.SnapshotLabels()); got != undoSnapshotLimit {
		t.Fatalf("stack should be capped at %d, got %d", undoSnapshotLimit, got)
	}
}

func TestForkSession_BranchAndBack(t *testing.T) {
	m := NewManager(t.TempDir())
	const chatID = "chat-fork-branch"
	s := m.Get("web", "", chatID, "")
	s.Append(msg("user", "plan A please"))
	s.Append(msg("assistant", "doing plan A"))
	s.Append(msg("user", "hmm"))

	// 全量分叉
	forkKey, err := m.ForkSession("web", "", chatID, 0)
	if err != nil {
		t.Fatalf("ForkSession: %v", err)
	}
	if forkKey == "" || forkKey == s.SessionKey() {
		t.Fatalf("fork key should be new, got %q", forkKey)
	}

	// Get 现在解析到分叉（activeOverride），且带全部历史
	fork := m.Get("web", "", chatID, "")
	if fork.SessionKey() != forkKey {
		t.Fatalf("Get should resolve to fork %q, got %q", forkKey, fork.SessionKey())
	}
	if got := len(fork.GetMessages()); got != 3 {
		t.Fatalf("fork should carry 3 copied messages, got %d", got)
	}
	// 分叉里继续写，不影响原线
	fork.Append(msg("assistant", "doing plan B"))
	if got := len(s.GetMessages()); got != 3 {
		t.Fatalf("original line must stay untouched, got %d msgs", got)
	}

	// 带 keepLast 的二级分叉
	fork2Key, err := m.ForkSession("web", "", chatID, 1)
	if err != nil {
		t.Fatalf("ForkSession keepLast: %v", err)
	}
	fork2 := m.Get("web", "", chatID, "")
	if fork2.SessionKey() != fork2Key {
		t.Fatalf("second fork should be active, got %q", fork2.SessionKey())
	}
	if got := len(fork2.GetMessages()); got != 1 {
		t.Fatalf("keepLast=1 fork should carry 1 message, got %d", got)
	}

	// /fork back：先回第一条分叉，再回原线
	backKey, ok := m.SwitchBackSession("web", "", chatID)
	if !ok || backKey != forkKey {
		t.Fatalf("first back = (%q,%v), want (%q,true)", backKey, ok, forkKey)
	}
	if got := m.Get("web", "", chatID, "").SessionKey(); got != forkKey {
		t.Fatalf("after first back Get should resolve to fork, got %q", got)
	}
	backKey2, ok := m.SwitchBackSession("web", "", chatID)
	if !ok || backKey2 != s.SessionKey() {
		t.Fatalf("second back = (%q,%v), want original (%q,true)", backKey2, ok, s.SessionKey())
	}
	if got := m.Get("web", "", chatID, "").SessionKey(); got != s.SessionKey() {
		t.Fatalf("after second back Get should resolve to original, got %q", got)
	}
	// 栈空后 back 失败
	if _, ok := m.SwitchBackSession("web", "", chatID); ok {
		t.Fatal("back on empty stack should fail")
	}
}

func TestForkSessionKeepFirst_BranchFromMiddle(t *testing.T) {
	m := NewManager(t.TempDir())
	const chatID = "chat-keepfirst"
	s := m.Get("web", "", chatID, "")
	// 5 条消息：user/assistant 交替
	for i := 0; i < 5; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		s.Append(msg(role, fmt.Sprintf("m%d", i)))
	}

	// 保留前 3 条分叉（"从第 2 条回答分叉"）
	forkKey, err := m.ForkSessionKeepFirst("web", "", chatID, 3)
	if err != nil {
		t.Fatalf("ForkSessionKeepFirst: %v", err)
	}
	fork := m.Get("web", "", chatID, "")
	if fork.SessionKey() != forkKey {
		t.Fatalf("Get should resolve to fork, got %q", fork.SessionKey())
	}
	if got := len(fork.GetMessages()); got != 3 {
		t.Fatalf("fork should keep first 3 messages, got %d", got)
	}
	if last := fork.GetMessages()[2].Content; last != "m2" {
		t.Fatalf("fork last message should be m2, got %q", last)
	}
	// 原线不受影响
	if got := len(s.GetMessages()); got != 5 {
		t.Fatalf("original line must stay 5 msgs, got %d", got)
	}
	// 越界 keepFirst = 全量。先切回原线（此刻活跃线是上面的分叉，
	// 直接分叉会对着 3 条的分支做）
	if _, ok := m.SwitchBackSession("web", "", chatID); !ok {
		t.Fatal("switch back should succeed")
	}
	full, err := m.ForkSessionKeepFirst("web", "", chatID, 99)
	if err != nil {
		t.Fatalf("ForkSessionKeepFirst overflow: %v", err)
	}
	if got := len(m.GetByKey(full).GetMessages()); got != 5 {
		t.Fatalf("overflow keepFirst should copy all 5, got %d", got)
	}
}

func TestOpenNewSession_ClearsForkOverride(t *testing.T) {
	m := NewManager(t.TempDir())
	const chatID = "chat-new-after-fork"
	s := m.Get("web", "", chatID, "")
	s.Append(msg("user", "hello"))

	if _, err := m.ForkSession("web", "", chatID, 0); err != nil {
		t.Fatalf("ForkSession: %v", err)
	}
	// /new 之后：覆盖表被清掉，活跃解析回到 web 规则（key==chatID）
	m.OpenNewSession("web", "", chatID)
	if got := m.Get("web", "", chatID, "").SessionKey(); got != chatID {
		t.Fatalf("after /new, web resolution should fall back to chatID, got %q", got)
	}
}
