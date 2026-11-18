# Lab 3: Making nanoCoder's History Fault-Tolerant (Raft)

## 1 Goal

Implement the key functions of Raft (Section 2) in `src/raft.cc`: every
`/* Your code here */` marker in that one file, and nothing else. You are done
when every `ctest` filter listed under Section 2 and Section 3 is green.

### 1.1 Architecture & layering

```
nanoCoder agent loop → MessageStore interface → RaftKvClient      (leader redirect + exactly-once)
      └─ msgpack-rpc ─────────────── network ───────────────┐
                                                            ▼
                    nanoraftd — two RPC interfaces on one server (server/main.cc)
                      ├─ kv_*    client↔cluster: propose → await commit → reply
                      └─ raft_*  node↔node:      RaftNode::handle_*
                          └─ RaftNode  (src/raft.cc)          ← Section 2: ①②③④ yours
                              ├─ RpcNetwork       node↔node transport interface
                              ├─ RaftStateMachine the replicated app
                              │    └─ KvStateMachine (src/kv_state_machine.cc) ← Section 3, provided
                              └─ RaftStorage      durable state
```

### 1.2 Provided (do not edit)

| Piece | Where |
| --- | --- |
| Node scaffolding: timer thread, `random_timeout`, log-offset helpers, `apply_committed`, `maybe_compact`, `persist`, `recover_from_storage`, `await_applied`, accessors, `send_heartbeats` and the two request builders it calls | `src/raft_scaffold.cc`, `raft.h` |
| The structs sent over the network + the InstallSnapshot protocol | `raft_protocol.h` |
| Transports: `MockNetwork` (emulates different network issues), `LibrpcNetwork` | `test/mock_network.h`, `src/librpc_network.cc` |
| Storage: `MemoryStorage`, `FileStorage` (atomic) | `test/mock_network.h`, `src/file_storage.cc` |
| The replicated app: `KvStateMachine::apply` (Section 3), the KV codec (`encode`/`decode`/`encode_scan`/`scan_prefix`) and `snapshot`/`restore` | `src/kv_state_machine.cc`, `src/kv_codec.cc` |
| `nanoraftd` (both interfaces), `nanoraft_kvctl`, the whole nanoCoder side | `server/`, `client/`, `nanocoder/` |
| Test suites + the `RaftGroup` fixture: the grader; don't edit them | `test/*.cc` (via `ctest`) |

### 1.3 Build, test, grade

From the **repository root** (`nanoraft/` is a subproject; configuring inside it
fails):

```bash
cmake -S . -B build && cmake --build build -j4 && ctest --test-dir build --output-on-failure
```

Run one part at a time with `-R` (a regex on the CTest entry name); the `-R`
command printed under each part of Section 2 and Section 3 is that part's grading rubric.

### 1.4 Scope & assumptions

| | Assumed | Not covered |
| --- | --- | --- |
| **Failures** | fail-stop crash + restart from storage; the network may drop, delay, duplicate, reorder | Byzantine nodes; lying disks (`FileStorage` writes are atomic) |
| **Membership** | fixed peer set, id-ordered, known at startup | config change, leadership transfer, pre-vote |
| **Reads** | every read is a log entry → linearizable | transactional reads and writes |
| **Snapshots** | one blob per InstallSnapshot | chunked transfer: a snapshot must fit one RPC |

- **Election no-op entry:** not used in this lab. Becoming Leader does not
  append an entry; only client proposals extend the log, so tests may assert
  exact log indexes.

---

## 2 The Raft node (`nanoraft/src/raft.cc`)

Implement ① election, then ② log replication: the receiver first, followed by
the sending side. Some election tests also require the AppendEntries receiver,
so rerun the election tests after completing it.

As you implement ① and ②, include the `persist()` calls listed in ③.
Use ③ to check that all required state changes are persisted, then
implement ④ snapshots.

### 2.0 The scaffolding (`src/raft_scaffold.cc`, provided, read first)

- **The state you operate on** (declared in `raft.h`):

  | Name | Meaning |
  | --- | --- |
  | `id_`, `n_` | this node's id, in `[0, n_)`; and the cluster size. The peer set is the fixed id range `0..n_-1`, **`id_` included**, so "a majority" is `count * 2 > n_`, and you skip `peer == id_` when sending RPCs. |
  | `role_`, `current_term_`, `voted_for_` | Raft Fig. 2's `state`/`currentTerm`/`votedFor` (`voted_for_ == -1` = no vote this term). The last two are durable (③). |
  | `log_`, `snapshot_index_`, `snapshot_term_`, `snapshot_` | the offset log (below) + the prefix already applied to the state machine and saved as a snapshot (index/term of its last entry, and the bytes). |
  | `compact_threshold_` | how far `last_applied_` may run past `snapshot_index_` before the provided `maybe_compact` compacts (default 64). |
  | `commit_index_`, `last_applied_` | highest index known committed / already applied to `state_machine_`. `apply_committed` moves the second up to the first. |
  | `next_index_`, `match_index_` | leader-only replication cursors, **sized `n_` and indexed by node id**, including a self slot (`match_index_[id_]` = the leader's own last index). |
  | `net_`, `state_machine_`, `storage_` | the three pluggable interfaces: transport, replicated app, durable state (`storage_` may be null). |

- **Timer thread** (`run_timer`): as Follower or Candidate it waits for a
  timeout (300-600ms, chosen by `random_timeout()`), then checks `last_heard_`.
  It calls `start_election()` only if at least that timeout has elapsed since
  `last_heard_`. Recording valid contact in `last_heard_` postpones the next
  election. As Leader it calls `send_heartbeats()`, then sleeps 100ms.
- **`send_heartbeats`** is one replication round. It makes no Raft decisions
  itself. Per peer it picks one RPC: InstallSnapshot if `next_index_[peer] <=
  snapshot_index_` (compaction dropped the entry that peer needs, ④),
  AppendEntries otherwise. It builds the args (also provided: an AppendEntries
  carries everything from `next_index_[peer]` onwards, so a caught-up peer gets
  an empty batch, and that empty request is the heartbeat), sends each on its
  own thread, and hands each reply to your matching `on_*`. Every decision in it
  is one of your `on_*`, so read it before writing them.
- **Locking:** `mu_` protects the members declared below it in `raft.h`.
  Acquire it inside each `handle_*` and `on_*` function. Never hold `mu_`
  while calling `net_->send_*`.
- **Log indices after a snapshot**

  Each log entry has a Raft index, starting at 1. Taking a snapshot removes
  entries from `log_`, but does **not** renumber the remaining entries.

  For example, suppose a snapshot covers entries 1–5, and entries 6–8 remain:

  ```text
  Snapshot:       entries 1–5
  Raft index:         6        7        8
  Stored in:       log_[0]  log_[1]  log_[2]
  ```

  Here, `snapshot_index_ = 5`, `first_index() = 6`, and `last_log_index() = 8`.

  To access Raft entry `i` in the vector, use `log_[i - first_index()]`.
  For example, entry 7 is stored in `log_[1]`. Only do this if `has_entry(i)`
  is true.

  Use `term_at(i)` to read an entry’s term. It also works at the snapshot
  boundary: `term_at(5)` returns `snapshot_term_`, even though entry 5 is no
  longer in `log_`.

  If no entries remain in `log_`, `last_log_index()` equals `snapshot_index_`.
  It is 0 only when there is no snapshot and no log entry. An AppendEntries
  request uses `prev_log_index = 0` and `prev_log_term = 0` when there is no
  preceding entry.

- **`apply_committed`** applies `(last_applied_, commit_index_]` to
  `state_machine_` in order, then calls `maybe_compact()`, which is Raft §7's
  snapshotting: past `compact_threshold_`, the state machine writes its current
  state to a snapshot, and the log entries up through the last included index
  are deleted. The last included index is `last_applied_` (Raft §7: "the last entry
  the state machine had applied"), not `commit_index_`.
- **`persist()`** saves `{term, vote, log, snapshot}` in one **atomic** call:
  all four or none reach storage (Section 1.4: `FileStorage` writes are atomic), so
  they never disagree on disk after a crash. Where to call it is ③.

### 2.1 Part ① Leader election

**Edit `src/raft.cc`**: write `start_election` / `handle_request_vote`. The
RequestVote structs are in `include/nanoraft/raft_protocol.h` (read-only);
`random_timeout` and the timer thread that calls `start_election` are provided
in `src/raft_scaffold.cc`.

| Function | What it does |
| --- | --- |
| `start_election` | Start an election following Raft §5.2 and Raft Fig. 2. Send RequestVote RPCs concurrently. If elected, initialize `next_index_` and `match_index_`, including the self slot. The provided timer thread will send heartbeats. |
| `handle_request_vote` | Handle incoming RequestVote RPCs following the receiver rules in Raft Fig. 2 and Raft §5.4.1. When granting a vote, update `last_heard_` to reset the election timeout. |

**Tips.**

- **Send RequestVote concurrently**, and never hold `mu_` during an RPC.
- **Up-to-date** is lexicographic on `(last_log_term, last_log_index)` (Raft §5.4.1).
- **Hold `mu_` while accessing shared state**, but release it before calling
  any `net_->send_*`. Do not acquire `mu_` again while already holding it:
  the mutex is not recursive, so doing so deadlocks.
- **The majority counts you:** start at 1, test `votes * 2 > n_`.

**Test.** `ctest --test-dir build -R RaftElection --output-on-failure`

`RejectsCandidateWithStaleLog` needs Section 2.2's `handle_append_entries`, and the two
reconnect tests only run stably once that same function starts answering
heartbeats; without it every follower keeps timing out and re-electing.

### 2.2 Part ② Log replication

**Edit `src/raft.cc`.** The RPC structs are in
`include/nanoraft/raft_protocol.h` (read-only); `apply_committed` is provided in
`src/raft_scaffold.cc`.

Replicating one entry takes both sides: the node that receives it and the leader
that sends it, so the task is split in two steps.

Naming: `handle_*` = an RPC someone sent you; `on_*` = the reply to an RPC you
sent.

`AppendEntriesArgs` / `AppendEntriesReply` (`raft_protocol.h`, read-only) are
Raft Fig. 2's fields, plus two optional hint fields on the reply:

| Field | Meaning |
| --- | --- |
| `term`, `leader_id` | the sender's term; who the follower should redirect clients to |
| `prev_log_index`, `prev_log_term` | the entry this batch attaches *after*; `0/0` means "attach at the very beginning" |
| `entries` | the batch, in index order. **Empty = heartbeat**: the same code path on both sides |
| `leader_commit` | the leader's `commit_index_`, so the follower can raise its own |
| reply `term` | the receiver's term, so a stale leader learns to step down |
| reply `success` | did the consistency check at `prev_log_index` pass |
| reply `conflict_term`, `conflict_index` | optional and ungraded; where to retry after a rejection; `0/0` = no hint |

**The graded path ignores both hint fields.** On a rejection the leader
decrements `next_index_[peer]` by one and retries on the next heartbeat round
until the logs line up, which is enough to pass every test.

**There is optimization space.** (Optional) The 6.824 XTerm/XIndex hint: a follower that
rejects reports which term it has at `prev_log_index` and which index that term
starts at, so the leader can skip a whole term per round trip instead of one
index.

#### Step 1: the receiving side (1 function)

| Function | What it does |
| --- | --- |
| `handle_append_entries` | Handle incoming AppendEntries RPCs, including heartbeats. Follow the receiver rules in Raft Fig. 2 and Raft §5.3. This handler must also accept requests when the node is Candidate or Leader. For a current- or higher-term request, record the leader contact in `last_heard_` and `leader_id_`, even if the log consistency check fails. Call the provided `apply_committed()` when `commit_index_` advances. |

Implement `handle_append_entries` first and run `RaftAppendEntries` to check it.
Then rerun `RaftElection`, because some election tests also depend on this handler.

**Test.** Both steps run the same source, `test/test_raft_replication.cc`, with
one command each:

```bash
ctest --test-dir build -R RaftAppendEntries --output-on-failure
```

#### Step 2: the sending side (3 functions)

| Function | What it does |
| --- | --- |
| `propose` | Append the command to the leader's log, keep `match_index_[self]` up-to-date, persist, then `advance_commit_index()` (a 1-node cluster gets no replies, so this is its only path to commit). Non-leader: append nothing, return `{0, term, false}`. |
| `on_append_entries_reply` | Step down on a higher term; else, **only if you are still the Leader of the term you sent**, advance `match_`/`next_index_` and call `advance_commit_index()` on success, or decrement `next_index_` (never below 1) on failure. |
| `advance_commit_index` | Raise `commit_index_` to the highest `N` replicated on a majority **and** created in the current term; then `apply_committed()`. |

**Test.**

```bash
ctest --test-dir build -R RaftReplication --output-on-failure
```

### 2.3 Part ③ Persistence

**Edit `src/raft.cc`**: call `persist()` in the functions you implemented in ①②.

In Raft, three fields must be durable: `currentTerm`, `votedFor`, `log[]`. The
provided `persist()` saves all of them, plus the snapshot for ④, in one atomic
call (Section 2.0), so one call per site is always enough and you never order separate
writes. These fields are modified in the following functions:

| Function (①/②) | Persist after |
| --- | --- |
| `start_election` | the updated term + the vote for self, before any RequestVote is sent; a higher term adopted from a reply |
| `handle_request_vote` | a higher term adopted; a vote granted, before the reply returns |
| `propose` | the appended entry |
| `handle_append_entries` | a higher term adopted; the truncate/append, before the `success` reply returns |
| `on_append_entries_reply` | the step-down on a higher reply term |

Why *before*: a `handle_request_vote` that answers "granted" and dies before
persisting can vote again in the same term after a restart, which gives one term
two leaders.

**Test.** `ctest --test-dir build -R RaftPersist --output-on-failure`

### 2.4 Part ④ Snapshot / InstallSnapshot

**Edit `src/raft.cc`**: implement the two functions below.

**Each node takes its snapshot independently** (Raft §7), with no RPC involved. The
code is the provided `maybe_compact` (`src/raft_scaffold.cc`): once
`last_applied_` runs `compact_threshold_` past the last snapshot, it saves
`state_machine_->snapshot()` into `snapshot_`, deletes the log entries the
snapshot covers, and records where the snapshot ends in `snapshot_index_` /
`snapshot_term_`, which are Raft Fig. 13's `lastIncludedIndex` /
`lastIncludedTerm`. Read `maybe_compact` before writing anything: both
functions below read or move exactly these variables.

Because each node compacts on its own, a leader will occasionally have already
discarded the next log entry a slow follower needs. That case needs a new RPC,
**InstallSnapshot** (Raft Fig. 13): the leader sends its snapshot instead of log
entries. The sending side is provided. For any peer whose `next_index_` fell at
or below `snapshot_index_`, `send_heartbeats` sends the whole snapshot plus the
index and term of its last entry, the same blob for every peer (Section 1.4). You
write the receiver and the reply handler:

| Function | What it does |
| --- | --- |
| `handle_install_snapshot` | Raft Fig. 13 receiver: adopt the term, `state_machine_->restore()`, discard the covered log, advance commit/applied, persist. |
| `on_install_snapshot_reply` | Handle replies to InstallSnapshot RPCs following Raft §7 and Raft Fig. 13. Update the peer’s `match_index_` and `next_index_` without moving `match_index_` backwards. No `advance_commit_index()` call is needed: the snapshot contains only entries already committed by the leader. |

**Tips.**

- **Use `persist()` to persist the snapshot.** It saves the snapshot and the log
  in the same atomic write (Section 2.0), so make every state change (restore the
  snapshot, discard the covered log) and then persist once. A crash leaves
  either the old state or the new one, never a truncated log without its
  snapshot.
- **Keep a newer tail only if its boundary matches:** entries beyond
  `last_included_index` survive iff the entry there carries the matching term;
  otherwise clear the whole log.
- **Call `state_machine_->restore(snapshot_)`** in `handle_install_snapshot`
  after saving the received bytes into `snapshot_`. Moving `snapshot_index_` /
  `snapshot_term_` alone leaves the state machine on its old state.
- **After `restore`, set `last_applied_ = snapshot_index_`** and raise
  `commit_index_`, or `apply_committed` re-applies entries whose effects the
  snapshot already contains.
- **Process the term and leader contact before ignoring an old snapshot.**
  If `last_included_index <= last_applied_`, return without restoring or
  truncating: already-applied state must not roll back.

**Test.**

```bash
ctest --test-dir build -R 'RaftInstallSnapshot|RaftSnapshot' --output-on-failure
```

---

## 3 The KV state machine (`nanoraft/src/kv_state_machine.cc`, provided)

**Edit nothing.** `KvStateMachine` is the worked example of a
`RaftStateMachine`: a key-value store whose `apply` runs one committed command
(Put, Get or Scan) and returns its result, plus the `snapshot` / `restore` pair
that ④ uses.

**Test.**

```bash
ctest --test-dir build -R 'KvStateMachine|RaftKv' --output-on-failure
```

---

## 4 Integration with nanoCoder (demo, no implementation needed)

This demo stores nanoCoder’s conversation history in the Raft cluster. You will
restart nanoCoder to check history recovery, then stop one Raft node to check
that the session remains available. No additional implementation is required.

Python commands run through [uv](https://docs.astral.sh/uv/): the
first `uv run` creates the venv and installs the dependencies declared in the
root `pyproject.toml`, and every window then shares that same environment.

1. Start a 3-node cluster (storage/logs/pids under `/tmp/nanoraft-demo`):

   ```bash
   BUILD=build bash nanoraft/examples/start_cluster.sh
   ```

   The script also writes the cluster’s node addresses to the `"raft"` key in
   `./nanocoder.config.json`, preserving existing settings. Run all remaining
   commands from the repository root so nanoCoder can find this configuration
   automatically.

2. Open a session and chat, with a real LLM or with `--mock script.json` for a
   deterministic run:

   ```bash
   uv run python -m nanocoder.tui --session S1
   ```

   `--session` is the switch that turns on durable history: with it, messages
   go to the raft cluster instead of an in-process list. Which cluster? The
   `"raft"` node list step 1 wrote into the config (a `--raft` flag would
   override it; `--session` with no nodes in either place is an error).

   `S1` is a session name you choose. nanoCoder loads any messages already
   saved under that name and saves new messages to the cluster. To reopen
   this conversation later, use `--session S1` again. Use a different name
   to start a separate conversation.

3. While chatting, terminate the nanoCoder process from another terminal with
   `kill -9 <pid>`, where `<pid>` is the nanoCoder process ID. Leave the Raft
   cluster running.
4. Attach from a new terminal by running the same command again, from the same
   directory (there is no separate attach command: a new process with the
   same session name restores it):

   ```bash
   uv run python -m nanocoder.tui --session S1
   ```

   On startup, nanoCoder displays `restored N messages` and loads the messages
   saved in the cluster. You can then send a new message to continue the
   conversation.
5. Kill one `nanoraftd` (a minority) and keep chatting. The surviving majority
   elects or keeps a leader, and the client's leader-redirect and failover loop
   finds it:

   ```bash
   # args: <data-dir> <node-id> — kill only node 0 (of nodes 0/1/2);
   # nodes 1 and 2 are still a majority, so the session stays available
   bash nanoraft/examples/stop_cluster.sh /tmp/nanoraft-demo 0
   ```

When done, `bash nanoraft/examples/stop_cluster.sh` stops the remaining nodes
(storage is kept, so restarting the cluster recovers the session).

## 5 Reference

*In Search of an Understandable Consensus Algorithm* (extended version):
https://raft.github.io/raft.pdf

References labeled “Section” point to this lab handout. References labeled
“Raft §” or “Raft Fig.” point to the paper above.
