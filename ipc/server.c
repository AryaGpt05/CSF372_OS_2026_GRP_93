#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <stdarg.h>
#include <sys/time.h>
#include <pthread.h>
#include <signal.h>
#include <fcntl.h>
#include <mqueue.h>

#define GRID_SIZE 10
#define MAX_STRING_LEN 64
#define MAX_CLIENTS 10

// Timestamped printf that prefixes each line with HH:MM:SS.mmm
static void ts_printf(const char *fmt, ...) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm_info;
    localtime_r(&tv.tv_sec, &tm_info);
    char tbuf[32];
    strftime(tbuf, sizeof(tbuf), "%H:%M:%S", &tm_info);

    fprintf(stdout, "%s.%03ld ", tbuf, tv.tv_usec / 1000);

    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);

    fflush(stdout);
}

#define printf(...) ts_printf(__VA_ARGS__)

/* ------------------------------------------------------------------------
 * Wire protocol (must match client.c)
 *
 * Queues (POSIX message queues):
 *   /osdoc_server          control queue, created by the server.
 *                          Clients send CONNECT here.
 *   /client_queue_<id>_req client -> server requests (one per client),
 *                          created by the client.
 *   /client_queue_<id>     server -> client replies for the main thread.
 *   /client_queue_<id>_doc server -> client replies for the print_doc thread.
 *
 * Every request receives exactly one reply.
 * ---------------------------------------------------------------------- */
#define SERVER_QUEUE "/osdoc_server"
#define QUEUE_MAXMSG 10

enum msg_type {
    MSG_CONNECT = 1,   // control queue: client <id> wants a handler thread
    MSG_DISCONNECT,    // client is exiting; handler thread terminates
    MSG_READ,          // normal READ of (x,y)
    MSG_WRITE,         // WRITE word to (x,y) and take the edit lock
    MSG_UNLOCK,        // release the edit lock on (x,y)
    MSG_PRINT_READ,    // special READ issued by the print_doc thread
};

enum msg_status {
    ST_OK = 0,
    ST_DENIED,         // dropped because someone is editing the word
    ST_ERROR,          // malformed request (bad coordinates etc.)
};

struct osdoc_msg {
    int type;
    int status;
    int client_id;
    int x, y;
    int duration_ms;
    char word[MAX_STRING_LEN + 1];
};

/* ------------------------------------------------------------------------
 * Document
 *
 * Each cell has its own rwlock protecting `word` and `editor`.
 *   - READ  takes the rwlock in shared mode, so any number of readers can
 *     inspect the same word at the same time.
 *   - WRITE / UNLOCK take it in exclusive mode, only for the instant needed
 *     to check-and-set the `editor` field. The *logical* edit lock (the
 *     <time> ms of exclusive access) is represented by `editor != -1`, not
 *     by holding the pthread lock, so a server thread never blocks while a
 *     client is "editing".
 * ---------------------------------------------------------------------- */
struct cell {
    pthread_rwlock_t lock;
    char word[MAX_STRING_LEN + 1];
    int editor;  // client id currently holding the edit lock, -1 if none
};

static struct cell doc[GRID_SIZE][GRID_SIZE];

struct client_slot {
    pthread_mutex_t lock;
    int active;
    pthread_t thread;
    mqd_t req_q;
    mqd_t reply_q;
    mqd_t doc_q;
};

static struct client_slot clients[MAX_CLIENTS];
static mqd_t server_q = (mqd_t)-1;

// Auto-shutdown bookkeeping: once at least one client has connected and
// every client has since disconnected (and nobody new arrives within
// IDLE_SHUTDOWN_MS), the server writes output.txt and exits on its own.
#define IDLE_SHUTDOWN_MS 1000
#define CONTROL_POLL_MS 100
static pthread_mutex_t count_lock = PTHREAD_MUTEX_INITIALIZER;
static int active_clients = 0;
static int ever_connected = 0;
static struct timespec last_disconnect;

static void doc_init(void) {
    for (int i = 0; i < GRID_SIZE; i++) {
        for (int j = 0; j < GRID_SIZE; j++) {
            pthread_rwlock_init(&doc[i][j].lock, NULL);
            doc[i][j].word[0] = '\0';
            doc[i][j].editor = -1;
        }
    }
    for (int c = 0; c < MAX_CLIENTS; c++) {
        pthread_mutex_init(&clients[c].lock, NULL);
        clients[c].active = 0;
    }
}

static int valid_coord(int x, int y) {
    return x >= 0 && x < GRID_SIZE && y >= 0 && y < GRID_SIZE;
}

/* ---------------------------- request handlers -------------------------- */

static void handle_read(int cid, struct osdoc_msg *m, int special) {
    struct cell *c = &doc[m->x][m->y];
    pthread_rwlock_rdlock(&c->lock);
    if (c->editor != -1) {
        m->status = ST_DENIED;
        m->word[0] = '\0';
    } else {
        m->status = ST_OK;
        memcpy(m->word, c->word, sizeof(m->word));
    }
    pthread_rwlock_unlock(&c->lock);

    if (special) {
        printf("Server: Client %d PRINT_DOC READ(%d,%d) %s\n", cid, m->x, m->y,
               m->status == ST_OK ? "GRANTED" : "DROPPED");
    } else {
        printf("Server: Client %d READ LOCK(%d,%d) %s\n", cid, m->x, m->y,
               m->status == ST_OK ? "GRANTED" : "DENIED");
    }
}

static void handle_write(int cid, struct osdoc_msg *m) {
    struct cell *c = &doc[m->x][m->y];
    m->word[MAX_STRING_LEN] = '\0';

    pthread_rwlock_wrlock(&c->lock);
    if (c->editor != -1) {
        m->status = ST_DENIED;
    } else {
        c->editor = cid;
        memcpy(c->word, m->word, sizeof(c->word));
        m->status = ST_OK;
    }
    pthread_rwlock_unlock(&c->lock);

    if (m->status == ST_OK) {
        printf("Server: Client %d WRITE LOCK(%d,%d) GRANTED\n", cid, m->x, m->y);
        printf("Server: Client %d WRITE(%d,%d) = '%s', locked for %dms\n",
               cid, m->x, m->y, m->word, m->duration_ms);
    } else {
        printf("Server: Client %d WRITE LOCK(%d,%d) DENIED\n", cid, m->x, m->y);
    }
}

static void handle_unlock(int cid, struct osdoc_msg *m) {
    struct cell *c = &doc[m->x][m->y];
    pthread_rwlock_wrlock(&c->lock);
    if (c->editor == cid) {
        c->editor = -1;
        m->status = ST_OK;
    } else {
        m->status = ST_ERROR;  // not the owner: ignore
    }
    pthread_rwlock_unlock(&c->lock);

    if (m->status == ST_OK)
        printf("Server: Client %d UNLOCK(%d,%d)\n", cid, m->x, m->y);
    else
        printf("Server: Client %d UNLOCK(%d,%d) IGNORED (not the editor)\n", cid, m->x, m->y);
}

// Release every edit lock held by a client (used when it disconnects so a
// crashed / exiting client can never leave a word locked forever).
static void release_all_locks(int cid) {
    for (int i = 0; i < GRID_SIZE; i++) {
        for (int j = 0; j < GRID_SIZE; j++) {
            struct cell *c = &doc[i][j];
            pthread_rwlock_wrlock(&c->lock);
            int released = 0;
            if (c->editor == cid) {
                c->editor = -1;
                released = 1;
            }
            pthread_rwlock_unlock(&c->lock);
            if (released)
                printf("Server: Client %d UNLOCK(%d,%d) (released on disconnect)\n", cid, i, j);
        }
    }
}

/* ---------------------------- per-client thread ------------------------- */

static void *client_handler(void *arg) {
    int cid = (int)(long)arg;
    struct client_slot *slot = &clients[cid];
    struct osdoc_msg m;

    for (;;) {
        ssize_t n = mq_receive(slot->req_q, (char *)&m, sizeof(m), NULL);
        if (n < 0) {
            if (errno == EINTR) continue;
            printf("Server: Client %d request queue error: %s\n", cid, strerror(errno));
            break;
        }
        if (n != (ssize_t)sizeof(m)) continue;

        if (m.type == MSG_DISCONNECT) {
            m.status = ST_OK;
            mq_send(slot->reply_q, (const char *)&m, sizeof(m), 0);
            break;
        }

        mqd_t out = (m.type == MSG_PRINT_READ) ? slot->doc_q : slot->reply_q;

        if (!valid_coord(m.x, m.y)) {
            printf("Server: Client %d invalid coordinates (%d,%d)\n", cid, m.x, m.y);
            m.status = ST_ERROR;
            mq_send(out, (const char *)&m, sizeof(m), 0);
            continue;
        }

        switch (m.type) {
        case MSG_READ:       handle_read(cid, &m, 0); break;
        case MSG_PRINT_READ: handle_read(cid, &m, 1); break;
        case MSG_WRITE:      handle_write(cid, &m);   break;
        case MSG_UNLOCK:     handle_unlock(cid, &m);  break;
        default:
            printf("Server: Client %d unknown request type %d\n", cid, m.type);
            m.status = ST_ERROR;
            break;
        }

        if (mq_send(out, (const char *)&m, sizeof(m), 0) < 0)
            printf("Server: Client %d reply failed: %s\n", cid, strerror(errno));
    }

    release_all_locks(cid);
    printf("Server: Client %d disconnected\n", cid);

    pthread_mutex_lock(&slot->lock);
    mq_close(slot->req_q);
    mq_close(slot->reply_q);
    mq_close(slot->doc_q);
    slot->active = 0;
    pthread_mutex_unlock(&slot->lock);

    pthread_mutex_lock(&count_lock);
    active_clients--;
    clock_gettime(CLOCK_MONOTONIC, &last_disconnect);
    pthread_mutex_unlock(&count_lock);
    return NULL;
}

static void client_queue_name(char *buf, size_t len, int cid, const char *suffix) {
    snprintf(buf, len, "/client_queue_%d%s", cid, suffix);
}

static void handle_connect(int cid) {
    if (cid < 0 || cid >= MAX_CLIENTS) {
        printf("Server: Rejected connection from invalid client id %d\n", cid);
        return;
    }
    struct client_slot *slot = &clients[cid];

    pthread_mutex_lock(&slot->lock);
    if (slot->active) {
        pthread_mutex_unlock(&slot->lock);
        printf("Server: Client %d already connected, ignoring CONNECT\n", cid);
        return;
    }

    char name[64];
    client_queue_name(name, sizeof(name), cid, "_req");
    slot->req_q = mq_open(name, O_RDONLY);
    client_queue_name(name, sizeof(name), cid, "");
    slot->reply_q = mq_open(name, O_WRONLY);
    client_queue_name(name, sizeof(name), cid, "_doc");
    slot->doc_q = mq_open(name, O_WRONLY);

    if (slot->req_q == (mqd_t)-1 || slot->reply_q == (mqd_t)-1 || slot->doc_q == (mqd_t)-1) {
        printf("Server: Failed to open queues for client %d: %s\n", cid, strerror(errno));
        if (slot->req_q != (mqd_t)-1) mq_close(slot->req_q);
        if (slot->reply_q != (mqd_t)-1) mq_close(slot->reply_q);
        if (slot->doc_q != (mqd_t)-1) mq_close(slot->doc_q);
        pthread_mutex_unlock(&slot->lock);
        return;
    }

    if (pthread_create(&slot->thread, NULL, client_handler, (void *)(long)cid) != 0) {
        printf("Server: Failed to create thread for client %d\n", cid);
        mq_close(slot->req_q);
        mq_close(slot->reply_q);
        mq_close(slot->doc_q);
        pthread_mutex_unlock(&slot->lock);
        return;
    }
    pthread_detach(slot->thread);
    slot->active = 1;
    pthread_mutex_unlock(&slot->lock);

    pthread_mutex_lock(&count_lock);
    active_clients++;
    ever_connected = 1;
    pthread_mutex_unlock(&count_lock);

    printf("Server: Created thread for client %d\n", cid);

    // Tell the client it may start sending requests.
    struct osdoc_msg ack;
    memset(&ack, 0, sizeof(ack));
    ack.type = MSG_CONNECT;
    ack.status = ST_OK;
    ack.client_id = cid;
    mq_send(slot->reply_q, (const char *)&ack, sizeof(ack), 0);
}

/* ------------------------------- shutdown ------------------------------- */

// Writes the document in the required format: one line per non-empty
// document line, non-empty words separated by single spaces.
static void write_output(const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) {
        printf("Server: Could not open %s: %s\n", path, strerror(errno));
        return;
    }
    for (int i = 0; i < GRID_SIZE; i++) {
        int printed = 0;
        for (int j = 0; j < GRID_SIZE; j++) {
            struct cell *c = &doc[i][j];
            pthread_rwlock_rdlock(&c->lock);
            if (c->word[0] != '\0') {
                fprintf(fp, "%s%s", printed ? " " : "", c->word);
                printed = 1;
            }
            pthread_rwlock_unlock(&c->lock);
        }
        if (printed) fputc('\n', fp);
    }
    fclose(fp);
}

// Writes output.txt, removes the control queue and exits. Serialised so the
// signal thread and the idle-shutdown path can never run it twice.
static void shutdown_server(const char *reason) {
    static pthread_mutex_t once = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&once);  // never unlocked: exit() follows

    printf("Server: %s, shutting down\n", reason);
    write_output("output.txt");
    printf("Server: Final document written to output.txt\n");

    mq_close(server_q);
    mq_unlink(SERVER_QUEUE);
    printf("Server: Shutdown complete\n");
    exit(0);
}

// Dedicated signal thread: SIGINT/SIGTERM are blocked in every other thread,
// so shutdown runs here synchronously and may safely take locks / do stdio.
static void *signal_thread(void *arg) {
    sigset_t *set = arg;
    int sig;
    sigwait(set, &sig);
    shutdown_server(sig == SIGTERM ? "Received SIGTERM" : "Received SIGINT");
    return NULL;
}

static long ms_since(const struct timespec *t) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - t->tv_sec) * 1000L + (now.tv_nsec - t->tv_nsec) / 1000000L;
}

static int idle_shutdown_due(void) {
    pthread_mutex_lock(&count_lock);
    int due = ever_connected && active_clients == 0 &&
              ms_since(&last_disconnect) >= IDLE_SHUTDOWN_MS;
    pthread_mutex_unlock(&count_lock);
    return due;
}

int main() {
    printf("Server: Started\n");

    // Block termination signals before creating any thread so every thread
    // inherits the mask; only signal_thread will ever receive them.
    static sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigs, NULL);

    doc_init();

    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg = QUEUE_MAXMSG;
    attr.mq_msgsize = sizeof(struct osdoc_msg);

    mq_unlink(SERVER_QUEUE);  // remove a stale queue from a previous run
    server_q = mq_open(SERVER_QUEUE, O_CREAT | O_RDONLY, 0660, &attr);
    if (server_q == (mqd_t)-1) {
        perror("mq_open server queue");
        return 1;
    }

    pthread_t sig_tid;
    pthread_create(&sig_tid, NULL, signal_thread, &sigs);

    printf("Server: Grid initialized. Waiting for clients on %s\n", SERVER_QUEUE);

    // Control loop: accept CONNECT messages and spawn a handler per client.
    // Wakes every CONTROL_POLL_MS to check whether all clients have left.
    for (;;) {
        if (idle_shutdown_due())
            shutdown_server("All clients disconnected");

        struct osdoc_msg m;
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_nsec += CONTROL_POLL_MS * 1000000L;
        if (dl.tv_nsec >= 1000000000L) {
            dl.tv_sec++;
            dl.tv_nsec -= 1000000000L;
        }
        ssize_t n = mq_timedreceive(server_q, (char *)&m, sizeof(m), NULL, &dl);
        if (n < 0) {
            if (errno == EINTR || errno == ETIMEDOUT) continue;
            perror("mq_receive server queue");
            break;
        }
        if (n != (ssize_t)sizeof(m)) continue;
        if (m.type == MSG_CONNECT)
            handle_connect(m.client_id);
        else
            printf("Server: Unexpected message type %d on control queue\n", m.type);
    }

    // Only reached on a fatal queue error.
    write_output("output.txt");
    mq_unlink(SERVER_QUEUE);
    printf("Server: Shutdown complete\n");
    return 0;
}
