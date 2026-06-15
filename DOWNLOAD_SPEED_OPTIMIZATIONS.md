# Download Speed Optimizations

Findings from the download-path analysis, ordered by tier (impact). Numbering is
stable — items are referred to by number (`#1`, `#4`, …) elsewhere.

Status: ✅ done · 🟡 partial · ⬜ not started

| # | Item | Status |
|---|------|--------|
| 1 | Multi-piece-per-peer pipeline (decouple window from piece size) | ✅ |
| 2 | Async hash/write pool (stop draining the pipeline between pieces) | ✅ |
| 3 | Persistent file handles + drop the global storage lock | ✅ |
| 4 | Lock-free hot-path byte counters | ✅ |
| 5 | Lock-free unlimited fast-path in RateLimiter | ✅ |
| 6 | Cheaper piece picker (avoid the full scan/sort per pick) | ✅ |
| 7 | Rarest-first piece selection | ⬜ |
| 8 | Endgame mode | ⬜ |

✅ #1, #4, #5 landed on branch `perf/peer-pipeline-and-lockless-counters`
(commit `5d6ddb2`). ✅ #2, #3, #6 landed on branch
`perf/async-write-storage-handles-picker`.

---

## Tier 1 — Per-peer throughput ceiling

### #1 — Multi-piece-per-peer pipeline ✅
Each peer used to download a single piece at a time, so the in-flight request
window was capped at one piece's block count (a 256 KB piece = 16 blocks =
256 KB in flight). Per-peer throughput ≈ window ÷ RTT, so small-piece torrents
were badly throttled on high-latency links.

**Done:** a peer now fills the request window across several pieces at once,
opening pieces on demand so depth is bounded by the window (`maxPendingBlockRequests`,
raised 128 → 256), not the piece size. Bounded by `maxConcurrentPiecesPerPeer`.
See `pump`/`openNewPiece` in `pkg/downloader/session.go`.

### #2 — Async hash/write pool ✅
When a piece's last block arrived, `sha1.Sum(pieceData)` and
`s.Storage.WriteBlock(...)` ran inline in the peer read loop, so the socket stopped
draining and no new requests went out until hashing + the disk write + the
fast-resume persist finished — a stall every piece.

**Done:** the `MsgPiece` handler now only assembles the piece buffer and hands it to
a small background hash/write pool (`pieceWriteCh`/`pieceWriteWorker`/
`processCompletedPiece` in `pkg/downloader/session.go`); the peer goroutine keeps
draining the socket and requesting immediately. The pool verifies the SHA-1, writes
to storage, persists state, and advertises `Have`. A bad hash returns the piece to
the picker and closes the feeding peer's connection (the decoupled equivalent of the
old inline disconnect-on-corruption). The pool is created lazily, bounded by
`pieceWriteQueueDepth` (backpressure caps memory), and — like background verification
— is not awaited by `Close`, so a write wedged on slow I/O never blocks shutdown.

---

## Tier 2 — Concurrency / lock contention

### #3 — Persistent file handles + drop the global storage lock ✅
Every `ReadBlock`/`WriteBlock`/`VerifyPiece` used to take a single per-torrent `s.mu`
(serializing **all** disk I/O), call `ResolveAndValidatePath` →
`filepath.EvalSymlinks` + an `os.Lstat` per path component on every call, and do an
`openNoFollow` + `Stat` + I/O + `Close` syscall pair per op. On the seed path
`ReadBlock` runs per 16 KB block, so this was a stat/open/lstat storm under one mutex.

**Done** (`pkg/storage/storage.go`):
- Paths are validated **once** at construction (a single shared `PathResolver`); each
  file's canonical absolute path is cached on its `fileLayout`, so the hot path never
  re-runs `EvalSymlinks`/`Lstat`.
- The global `Mutex` became an `RWMutex`. Block **reads** (`ReadBlock`/`VerifyPiece`)
  take the read lock and run concurrently — positional `ReadAt` is concurrency-safe —
  so the seed path is no longer serialized. Writes/repair/state/`Close` take the write
  lock; they are per-piece (and run on the #2 pool), so they never block peer loops.
- Read handles are cached lazily (`O_RDONLY`, opened on first read, reused after) to
  drop the open/close pair on the hot read path; they are invalidated when a write
  recreates/resizes a file. Opening lazily (rather than at construction) keeps the
  "validate once, no symlink-follow" guarantee and lets a swapped-in file still be
  detected on first access. `Close` releases the cached handles.

Note: handles are not force-closed from `Session.Close` because a background
`VerifyPiece` can hold the read lock while wedged on slow I/O; they are released by
explicit `Storage.Close` and by `*os.File` finalizers, so shutdown never blocks.

### #4 — Lock-free hot-path byte counters ✅
`s.Downloaded`/`s.Uploaded` were bumped under `s.mu` on every 16 KB block — a
session-wide serialization point under many fast peers.

**Done:** session counters are `atomic.Int64`; per-peer counters use `sync/atomic`
helpers; the per-block increments no longer take `s.mu`. `GetActivePeers` builds
its snapshot field-by-field so the lock-free counters are read atomically rather
than via a racy struct copy. See `pkg/downloader/session.go`.

### #5 — Lock-free unlimited fast-path in RateLimiter ✅
`Wait` took the limiter mutex per block even when unlimited, and the global
limiter is shared by every peer of every session — so the whole swarm serialized
on one lock even with no limit set.

**Done:** `limit` is atomic; `Wait` short-circuits without the mutex when
unlimited. Context-cancellation semantics preserved. See `pkg/downloader/ratelimiter.go`.

### #6 — Cheaper piece picker ✅
The old picker allocated a candidate slice over **all** pieces and sorted it on
every pick, holding `s.mu` the whole time. As part of #1 it became a single linear
O(P) pass with no allocation or sort — but still O(total pieces) per pick.

**Done:** the session now maintains `neededPieces`, the set of pieces still
`PieceEmpty` and wanted, updated incrementally on every state transition
(`recomputeNeededLocked`/`addNeededLocked`/`removeNeededLocked`). `openNewPiece` calls
`selectNeededPieceLocked`, which scans only that set, so selection cost is
O(remaining-needed) — shrinking toward zero as the download completes — instead of
O(total pieces) per pick. Priority (highest first) and lowest-index tie-breaking are
preserved, and per-peer availability is still honored. The set is treated as a hint:
selection re-verifies `state == PieceEmpty` and prunes stale entries in-line, so a
direct/out-of-band `PieceStates` mutation can never cause a mis-selection.

(True ~O(1) selection is not achievable while honoring per-peer bitfield availability —
that filtering is inherently per-peer; rarest-first, #7, is the related follow-up.)

---

## Tier 3 — Swarm efficiency (real-world completion time)

### #7 — Rarest-first piece selection ⬜
The picker takes the highest-priority, lowest-index empty piece
(`openNewPiece` in `pkg/downloader/session.go`); it does not consider how rare a
piece is in the swarm. Track piece availability from peer bitfields/`Have`
messages and prefer rarer pieces to keep more pieces fetchable across the swarm.

### #8 — Endgame mode ⬜
With pipelining, the final pieces can trickle in from the slowest peers while fast
peers idle, creating a long completion tail. When few blocks remain, request the
outstanding blocks from multiple peers at once and `SendCancel` on receipt
(`Cancel` already exists in `pkg/peer/client.go`).

---

## Already good (keep)
- 4 MB socket buffers + `TCP_NODELAY` (`tunePeerConn` in `pkg/downloader/session.go`).
- Parallel tracker announces.
- Generous peer caps (200/session outbound).
