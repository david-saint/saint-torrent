package storage

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain lets the test binary stand in for the CLI as a flush helper, so the
// real spawn path can be exercised without building cmd/sainttorrent.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == FlushHelperArg {
		os.Exit(RunFlushHelper(os.Args[2:]))
	}
	os.Exit(m.Run())
}

// gatedSpawner stands in for the helper: every push it starts finishes only once
// release is called.
type gatedSpawner struct {
	spawns  atomic.Int32
	gate    chan struct{}
	release func()
	files   atomic.Int32
}

func newGatedSpawner(t *testing.T) *gatedSpawner {
	g := &gatedSpawner{gate: make(chan struct{})}
	var once sync.Once
	g.release = func() { once.Do(func() { close(g.gate) }) }
	t.Cleanup(g.release)
	return g
}

func (g *gatedSpawner) spawn(handles []*os.File) (func() error, error) {
	g.spawns.Add(1)
	g.files.Add(int32(len(handles)))
	return func() error { <-g.gate; return nil }, nil
}

func shortFlushBudget(t *testing.T) {
	t.Helper()
	prev := durableFlushBudget
	durableFlushBudget = 50 * time.Millisecond
	t.Cleanup(func() { durableFlushBudget = prev })
}

// A durable checkpoint must not wait on a push that outlasts its budget: it saves
// a hint, reports the deferral, and proves the pieces once the push is done.
func TestDurableCheckpointDefersWhilePushRuns(t *testing.T) {
	shortFlushBudget(t)
	st, hash := checkpointFixture(t)
	helper := newGatedSpawner(t)
	st.spawner = helper.spawn

	start := time.Now()
	err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true)
	if !errors.Is(err, ErrCheckpointDeferred) {
		t.Fatalf("SaveResumeState = %v; want ErrCheckpointDeferred", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("deferred checkpoint waited %v; the budget is %v", waited, durableFlushBudget)
	}
	if got := helper.files.Load(); got != 3 {
		t.Fatalf("helper was handed %d files; want all 3 written ones", got)
	}
	got, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResumeState{Recheck: []int{0, 1, 2, 3}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("after deferral got %+v; want %+v", got, want)
	}

	// A retry while the push still runs defers again without starting another.
	if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); !errors.Is(err, ErrCheckpointDeferred) {
		t.Fatalf("retry = %v; want ErrCheckpointDeferred", err)
	}
	if n := helper.spawns.Load(); n != 1 {
		t.Fatalf("%d helpers started; want 1", n)
	}

	helper.release()
	if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatalf("checkpoint after the push = %v", err)
	}
	if got, err = st.LoadResumeState(hash); err != nil {
		t.Fatal(err)
	}
	if want := (ResumeState{Verified: []int{0, 1, 2, 3}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("after push got %+v; want %+v", got, want)
	}
	if n := helper.spawns.Load(); n != 1 {
		t.Fatalf("%d helpers started; nothing was written after the first", n)
	}
}

// When no helper can start, the checkpoint flushes in-process as before rather
// than leaving the torrent unproven forever.
func TestDurableCheckpointFallsBackWhenHelperCannotStart(t *testing.T) {
	shortFlushBudget(t)
	st, hash := checkpointFixture(t)
	var spawns atomic.Int32
	st.spawner = func([]*os.File) (func() error, error) {
		spawns.Add(1)
		return nil, errors.New("no helper")
	}
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResumeState{Verified: []int{0, 1, 2, 3}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
	if err = st.WriteBlock(0, 0, []byte{5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	st.WriteBehind()
	if n := spawns.Load(); n != 1 {
		t.Fatalf("%d spawn attempts; a failed spawn must turn the helper off", n)
	}
}

// Write-behind keeps one push in flight and only pushes files written since the
// previous push started.
func TestWriteBehindPushesOnlyNewWrites(t *testing.T) {
	st, _ := checkpointFixture(t)
	helper := newGatedSpawner(t)
	st.spawner = helper.spawn

	st.WriteBehind()
	st.WriteBehind()
	if n, files := helper.spawns.Load(), helper.files.Load(); n != 1 || files != 3 {
		t.Fatalf("spawns=%d files=%d; want one push of the 3 written files", n, files)
	}
	if err := st.WriteBlock(3, 0, []byte{4, 3, 2, 1}); err != nil {
		t.Fatal(err)
	}
	st.WriteBehind()
	if n := helper.spawns.Load(); n != 1 {
		t.Fatalf("started a second push while the first runs")
	}
	helper.release()
	deadline := time.Now().Add(5 * time.Second)
	for helper.spawns.Load() < 2 && time.Now().Before(deadline) {
		st.WriteBehind()
		runtime.Gosched()
	}
	// Piece 3 lives entirely in file c.
	if n, files := helper.spawns.Load(), helper.files.Load(); n != 2 || files != 4 {
		t.Fatalf("spawns=%d files=%d; want a second push of file c only", n, files)
	}
	st.WriteBehind()
	if n := helper.spawns.Load(); n != 2 {
		t.Fatalf("pushed again with nothing written")
	}
}

// The real helper: this test binary re-run with FlushHelperArg over inherited
// descriptors.
func TestFlushHelperProcess(t *testing.T) {
	if !flushHelperSupported {
		t.Skip("no flush helper on this platform")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Skip(err)
	}
	st, hash := checkpointFixture(t)
	st.spawner = func(handles []*os.File) (func() error, error) {
		return spawnFlushHelper(exe, handles)
	}
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil && !errors.Is(err, ErrCheckpointDeferred) {
		t.Fatal(err)
	}
	st.pushMu.Lock()
	job := st.pushJob
	spawner := st.spawner
	st.pushMu.Unlock()
	if job == nil || spawner == nil {
		t.Fatal("no helper ran")
	}
	select {
	case <-job.done:
	case <-time.After(30 * time.Second):
		t.Fatal("helper did not finish")
	}
	if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResumeState{Verified: []int{0, 1, 2, 3}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}

	// The helper's exit status reflects the push.
	f, err := os.CreateTemp(t.TempDir(), "payload")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	wait, err := spawnFlushHelper(exe, []*os.File{f})
	if err != nil {
		t.Fatal(err)
	}
	if err = wait(); err != nil {
		t.Fatalf("helper over a regular file: %v", err)
	}
}

func TestRunFlushHelperRejectsBadArguments(t *testing.T) {
	for _, args := range [][]string{nil, {"x"}, {"0"}, {"65"}, {"1", "2"}} {
		if code := RunFlushHelper(args); code != 2 {
			t.Errorf("RunFlushHelper(%q) = %d; want 2", args, code)
		}
	}
}

// Write-behind stays out of the way once enough helpers are running; a durable
// checkpoint still gets its push.
func TestWriteBehindRespectsHelperCap(t *testing.T) {
	shortFlushBudget(t)
	st, hash := checkpointFixture(t)
	helper := newGatedSpawner(t)
	st.spawner = helper.spawn
	runningHelpers.Add(maxWriteBehindHelpers)
	t.Cleanup(func() { runningHelpers.Add(-maxWriteBehindHelpers) })

	st.WriteBehind()
	if n := helper.spawns.Load(); n != 0 {
		t.Fatalf("write-behind started %d helpers past the cap", n)
	}
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); !errors.Is(err, ErrCheckpointDeferred) {
		t.Fatalf("SaveResumeState = %v; want ErrCheckpointDeferred", err)
	}
	if n := helper.spawns.Load(); n != 1 {
		t.Fatalf("durable checkpoint started %d helpers; want 1", n)
	}
}
