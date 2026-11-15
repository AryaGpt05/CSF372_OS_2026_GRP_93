# OS Doc — Low-Level Design

## 1. Overview

OS Doc is a client–server system. One `server` process owns a 10×10 document
of words. Up to 10 `client` processes (IDs 0–9) on the same machine read and
write those words concurrently. All communication uses **POSIX message
queues** (`mq_open`, `mq_send`, `mq_receive`). Concurrency inside the server
uses **pthreads**, one **`pthread_rwlock_t` per word**, and one mutex per
client slot.

```
 client 0 ─┬─ main thread ──────┐        ┌──────────── server ───────────────┐
           └─ print_doc thread ─┤        │ main thread: control loop         │
                                │ CONNECT│   /osdoc_server  ──► spawn handler│
                                ├───────►│                                   │
                                │requests│ handler thread for client 0       │
                                ├───────►│   /client_queue_0_req             │
            /client_queue_0  ◄──┤ replies│     ├─ READ/WRITE/UNLOCK ─► reply │
            /client_queue_0_doc◄┘        │     └─ PRINT_READ        ─► doc   │
                                         │ signal thread: SIGTERM ─► output  │
                                         └───────────────────────────────────┘
```

## 2. IPC design

### 2.1 Queues

| Queue                    | Created by | Direction       | Purpose                                     |
|--------------------------|------------|-----------------|---------------------------------------------|
| `/osdoc_server`          | server     | client → server | Control queue. Only `CONNECT` messages.     |
| `/client_queue_<id>_req` | client     | client → server | All requests from client `<id>`.            |
| `/client_queue_<id>`     | client     | server → client | Replies for the client's main thread.       |
| `/client_queue_<id>_doc` | client     | server → client | Replies for the client's print_doc thread.  |

Every queue has `mq_maxmsg = 10` (the Linux default limit for unprivileged
users) and `mq_msgsize = sizeof(struct osdoc_msg)`.

### 2.2 Message format

A single fixed-size struct is used for both requests and replies:

```c
struct osdoc_msg {
    int type;         // CONNECT, DISCONNECT, READ, WRITE, UNLOCK, PRINT_READ
    int status;       // reply: OK, DENIED (dropped), ERROR
    int client_id;
    int x, y;         // line, word position
    int duration_ms;  // WRITE only
    char word[65];    // WRITE payload / READ result (64 chars + NUL)
};
```

### 2.3 Protocol

1. The client creates its three queues. It first unlinks any stale queue
   left by a crashed run.
2. The client sends `CONNECT(id)` on `/osdoc_server`. If the server isn't up
   yet, the client retries opening the queue for up to 5 s.
3. The server opens the client's queues and creates a **handler thread** for
   that client. It replies `CONNECT/OK` on `/client_queue_<id>`. The client
   blocks until this ACK arrives, so it never sends a request before a
   handler exists.
4. Each request gets **exactly one reply**. Every client-side wait uses
   `mq_timedreceive` with a 5 s timeout, so a client never hangs if the server
   dies.
5. On exit, the client sends `DISCONNECT`, waits for the ACK, and unlinks its
   queues.

### 2.4 Why message queues?

* They preserve message boundaries and keep messages in order, so a
  request/response protocol needs no framing code. Pipes would need framing,
  and named pipes would need extra care with multiple writers.
* They support blocking receives with timeouts (`mq_timedreceive`).
* With shared memory, the server would have to trust clients to follow the
  locking protocol themselves. With message queues the **server alone**
  enforces access control. A client can only *ask*, which makes it
  impossible for a buggy client to corrupt the document.
* The provided `Makefile` (`-lrt`, `clean` removes `/dev/mqueue/client_queue_*`)
  and `runner.py` (cleans up `client_queue_<i>`) are built around POSIX
  message queues. The client's queue names follow that `client_queue_<id>`
  convention, so the provided tooling cleans them up.

### 2.5 Why two reply queues per client?

The client has two threads that talk to the server at the same time: the main
thread running commands, and the print_doc thread. If both shared one reply
queue, a thread could receive a reply meant for the other. The server
therefore routes `PRINT_READ` replies to `_doc` and all other replies to the
main queue. Each thread has at most one request in flight, so each thread
always reads its own reply without any client-side locking.

## 3. Server design

### 3.1 Threads

| Thread                   | Job                                                                                              |
|--------------------------|--------------------------------------------------------------------------------------------------|
| main                     | Waits on `/osdoc_server`. On `CONNECT` it opens the client's queues and spawns a handler thread. Also shuts the server down once all clients have left (§3.7). |
| handler (one per client) | Blocks on `/client_queue_<id>_req`, runs each request, and replies.                              |
| signal                   | Waits in `sigwait()` for SIGTERM/SIGINT, writes `output.txt`, unlinks the control queue, exits.  |

A thread per client means one client's requests never delay another
client's. All requests are instantaneous (see §3.3), so each handler just
loops on receive → handle → reply.

### 3.2 Per-word state

```c
struct cell {
    pthread_rwlock_t lock;   // protects word + editor
    char word[65];
    int  editor;             // client id holding the edit lock, -1 if none
};
static struct cell doc[10][10];
```

Locking is **per word**, as the spec requires. Operations on different
words never contend. Two handlers touching the same word synchronise only
on that word's rwlock.

### 3.3 Logical edit lock vs. pthread lock

The edit lock that lasts `<time>` ms is stored as **data** (`editor != -1`).
It is **not** a pthread lock held for that whole time. The rwlock is only held
for the few instructions needed to check and update `editor`/`word`.

Why:
* The spec says a conflicting request must be **dropped**, not queued. A
  non-blocking check of `editor` gives exactly that. A blocking
  `pthread_mutex_lock` would make the second client wait.
* A server thread never sleeps while holding a lock, so handler threads are
  always responsive and can't deadlock against each other.
* A pthread lock must be unlocked by the thread that locked it. The edit lock
  is owned by a *client*, so it is modelled by client id.

### 3.4 Operations

| Request      | rwlock mode | Logic                                                                                                         | Server log                            |
|--------------|-------------|---------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `READ`       | shared      | `editor != -1` → DENIED; else copy word, OK                                                                   | `READ LOCK(x,y) GRANTED/DENIED`       |
| `PRINT_READ` | shared      | same as READ                                                                                                  | `PRINT_DOC READ(x,y) GRANTED/DROPPED` |
| `WRITE`      | exclusive   | `editor != -1` → DENIED; else `editor = cid`, overwrite word, OK                                              | `WRITE LOCK(x,y) GRANTED/DENIED`      |
| `UNLOCK`     | exclusive   | if `editor == cid` → `editor = -1`                                                                            | `UNLOCK(x,y)`                         |
| `DISCONNECT` | exclusive   | releases every word still held by the client                                                                  | `UNLOCK(x,y) (released on disconnect)` |

READ uses the **shared** side of the rwlock, so many clients can read the
same word at the same moment. This matches "multiple readers allowed". WRITE
uses the exclusive side, so check-and-set of `editor` is atomic. When two
clients race to WRITE the same word, exactly one sees `editor == -1`.

The word is updated **at grant time** ("the word is updated with the new
string", then held). While it is held, all other READ/WRITE requests on it
are dropped. As a result, nobody ever observes it mid-edit.

### 3.5 Who measures `<time>`?

The **client** sleeps for `<time>` ms after the grant, then sends `UNLOCK`.
This matches the expected client log
`WRITE(x,y) = '<w>', sleeping for <t>ms` → `WRITE(x,y) COMPLETED`, and the
fact that the client runs its commands strictly in sequence. Because the
server releases a client's locks on `DISCONNECT` (and only the editor can
`UNLOCK`), a client that exits early can never leave a word locked forever.

To optimize disconnect performance, each `client_slot` tracks the coordinates
of cells currently held by that client. On client exit, `release_all_locks`
performs an $O(1)$ check: if `held_count == 0` (the typical case where all
edits finished cleanly), it returns immediately without scanning or acquiring
rwlocks across the 100 document cells. If a client disconnected mid-edit, it
unlocks only the recorded cells.

### 3.6 Validation

The client rejects malformed lines (bad coordinates, words > 64 chars,
negative times, trailing junk). The server re-checks coordinates and
NUL-terminates the word anyway, so a bad message can't index out of bounds.

### 3.7 Shutdown

The server shuts down in either of two ways. Both lead to one
`shutdown_server()` routine, which is serialised by a mutex so it can never
run twice.

**Idle shutdown (the normal path).** `runner.py` reads the server's stdout
until it closes *before* it checks whether the clients have finished. So the
server must exit by itself once the work is done. The server keeps a
mutex-protected count of connected clients. The control loop uses
`mq_timedreceive` with a 100 ms timeout, so it also wakes up periodically. Once at least one client
has connected, the count is back to 0, and no new client has arrived for
1 s, it shuts down. The 1 s grace period covers clients that the runner
starts 200 ms apart, so an early finisher doesn't end the session while
others are still starting.

**Signal shutdown.** SIGTERM (sent by the runner's `terminate()`) or SIGINT
(Ctrl-C) also shut it down. SIGINT/SIGTERM are blocked with
`pthread_sigmask` **before** any thread is created, so every thread inherits
the mask. Only the dedicated signal thread receives them, via `sigwait`. It
runs as a normal thread, not inside an async signal handler, so it can safely
take each word's rwlock, write `output.txt` with stdio, call `doc_destroy()`
to destroy all synchronizers, and call `exit(0)`.

`output.txt` format: for each line, non-empty words joined by single spaces.
Lines with no words are skipped.

## 4. Client design

### 4.1 Main thread

1. Trap `SIGINT` and `SIGTERM` via `sigaction` to clean up message queues on
   early interruption.
2. Connect (§2.3).
3. Start the print_doc thread.
4. Read `input.txt` line by line. Skip lines whose `C<id>` isn't ours, and run
   the rest **sequentially**:
   * `READ x y` → log the request; print `SUCCESS - Value: '<w>'` or `DROPPED`.
   * `WRITE x y w t` → on grant, log `sleeping for t ms`, sleep, send `UNLOCK`,
     then log `COMPLETED`. On deny, log `DROPPED`.
   * `SLEEP t` → `nanosleep`.
5. Stop the print_doc thread, `DISCONNECT`, unlink queues, and exit.

### 4.2 print_doc thread

* Takes a snapshot right away, then one every 2 s until the client exits.
* A snapshot sends 100 `PRINT_READ` requests, one per word. A dropped word
  (being edited) is shown as `???`. Empty words are skipped. The text is built
  in the same format as `output.txt` and passed to the provided `print_doc()`.
  Each snapshot is printed whole, so `output_client<id>.txt` never shows a
  half-written document.
* The 2 s wait is a `pthread_cond_timedwait` on `CLOCK_MONOTONIC` (immune to
  wall-clock changes). The main thread signals the condition variable at
  exit, so the client exits right away instead of waiting out the rest of the
  interval.
* There is **no extra snapshot at exit**. The file must show what the
  periodic snapshots saw. For example, `test_locked_words` expects `???` for
  words whose edit outlives the last 2 s tick.

## 5. Concurrency correctness

| Concern                                     | How it is handled                                                                          |
|---------------------------------------------|--------------------------------------------------------------------------------------------|
| Two writers race on one word                | Check-and-set of `editor` under the exclusive rwlock. Exactly one wins.                    |
| Reader sees a half-written word             | Readers hold the shared lock while copying, and writers update only under the exclusive lock. |
| Deadlock                                    | Each operation takes one word's lock (shutdown/disconnect take them one at a time). No lock is held across I/O or sleep. |
| Replies delivered to the wrong client thread | Separate reply queue per client thread (§2.5).                                             |
| Request sent before the handler exists      | The client waits for the `CONNECT` ACK.                                                    |
| Client crash while holding an edit          | Locks are tracked per client slot for O(1) disconnect checks, and released on `DISCONNECT`. Clients use reply timeouts so they never hang. |
| Signal arrives at client process            | Caught via `sigaction`, unlinks `/dev/mqueue` queues before termination.                    |
| Signal arrives at an arbitrary thread       | Signals are blocked everywhere except the `sigwait` thread.                                |
| Stale queues from earlier runs              | Each side `mq_unlink`s its queue names before creating them.                               |
