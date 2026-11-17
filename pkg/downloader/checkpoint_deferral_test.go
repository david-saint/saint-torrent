package downloader

import (
	"testing"

	"sainttorrent/pkg/storage"
)

// deferringStorage reports every durable checkpoint as deferred, the way the
// storage does while a flush helper is still pushing data out.
type deferringStorage struct {
	*countedVerificationStorage
}

func (s *deferringStorage) SaveResumeState(hash string, verified, unverified []int, durable bool) error {
	if err := s.countedVerificationStorage.SaveResumeState(hash, verified, unverified, false); err != nil {
		return err
	}
	if durable {
		return storage.ErrCheckpointDeferred
	}
	return nil
}

// A deferred checkpoint saved a hint: it is not a persist failure, and it is
// retried on a later tick, except during shutdown, which has no later tick.
func TestDeferredCheckpointRetriesWithoutError(t *testing.T) {
	tor, base := resumeSessionFixture(t, 16)
	st := &deferringStorage{base}
	sess, err := NewSession(tor, st, [20]byte{}, 0, base.BaseDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.verifyResume(sess.ctx)
	base.resetSaves()
	sess.mu.Lock()
	sess.paused = true
	sess.stateDirty = true
	sess.mu.Unlock()
	sess.flushState()

	sess.mu.RLock()
	dirty, persistErr, statusErr := sess.stateDirty, sess.statePersistErr, sess.statusErr
	sess.mu.RUnlock()
	if !dirty {
		t.Fatal("deferred checkpoint was not re-armed for a retry")
	}
	if persistErr != nil || statusErr != nil {
		t.Fatalf("deferral surfaced as an error: persist=%v status=%v", persistErr, statusErr)
	}
	if saves := base.savedStates(); len(saves) != 1 || saves[0] {
		t.Fatalf("saves %v; want the one hint the deferral wrote", saves)
	}

	sess.mu.Lock()
	sess.closing = true
	sess.mu.Unlock()
	sess.flushState()
	sess.mu.RLock()
	dirty = sess.stateDirty
	sess.mu.RUnlock()
	if dirty {
		t.Fatal("a closing session re-armed a checkpoint nothing will retry")
	}
}
