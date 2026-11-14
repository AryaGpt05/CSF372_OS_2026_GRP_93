#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <stdarg.h>
#include <sys/time.h>
#include <pthread.h>
#include <fcntl.h>
#include <mqueue.h>

#define GRID_SIZE 10
#define MAX_STRING_LEN 64
#define MSG_SIZE 256

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

void msleep(int milliseconds) {
    struct timespec ts;
    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (milliseconds % 1000) * 1000000;
    nanosleep(&ts, NULL);
}


int client_id_global;
// Pass the document string to be printed
// Sample: "Hello World\nThis is a test\n"
void print_doc(const char* doc) {
    char filename[64];
    snprintf(filename, sizeof(filename), "output_client%d.txt", client_id_global);

    FILE* fp = fopen(filename, "w");
    if (fp) {
        fprintf(fp, "%s", doc);
        fclose(fp);
    }
}

/* ------------------------------------------------------------------------
 * Wire protocol (must match server.c)
 * ---------------------------------------------------------------------- */
#define SERVER_QUEUE "/osdoc_server"
#define QUEUE_MAXMSG 10
#define PRINT_DOC_INTERVAL_MS 2000
#define REPLY_TIMEOUT_MS 5000

enum msg_type {
    MSG_CONNECT = 1,
    MSG_DISCONNECT,
    MSG_READ,
    MSG_WRITE,
    MSG_UNLOCK,
    MSG_PRINT_READ,
};

enum msg_status {
    ST_OK = 0,
    ST_DENIED,
    ST_ERROR,
};

struct osdoc_msg {
    int type;
    int status;
    int client_id;
    int x, y;
    int duration_ms;
    char word[MAX_STRING_LEN + 1];
};

static int my_id;
static mqd_t server_q = (mqd_t)-1;  // control queue (CONNECT)
static mqd_t req_q = (mqd_t)-1;     // our requests -> server
static mqd_t reply_q = (mqd_t)-1;   // replies for the main thread
static mqd_t doc_q = (mqd_t)-1;     // replies for the print_doc thread
static char req_name[64], reply_name[64], doc_name[64];

// print_doc thread control
static pthread_mutex_t pd_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t pd_cond;
static int pd_stop = 0;

/* ------------------------------ messaging ------------------------------- */

static void deadline_in(struct timespec *ts, clockid_t clk, int ms) {
    clock_gettime(clk, ts);
    ts->tv_sec += ms / 1000;
    ts->tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec++;
        ts->tv_nsec -= 1000000000L;
    }
}

// Receive one reply from `q`, giving up after REPLY_TIMEOUT_MS.
static int recv_reply(mqd_t q, struct osdoc_msg *m) {
    struct timespec dl;
    deadline_in(&dl, CLOCK_REALTIME, REPLY_TIMEOUT_MS);  // mq uses CLOCK_REALTIME
    for (;;) {
        ssize_t n = mq_timedreceive(q, (char *)m, sizeof(*m), NULL, &dl);
        if (n == (ssize_t)sizeof(*m)) return 0;
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
}

// Send a request and wait for its reply. The server routes PRINT_READ
// replies to doc_q and everything else to reply_q, so the main thread and
// the print_doc thread can both have a request in flight at the same time.
static int request(struct osdoc_msg *m) {
    m->client_id = my_id;
    mqd_t in = (m->type == MSG_PRINT_READ) ? doc_q : reply_q;
    if (mq_send(req_q, (const char *)m, sizeof(*m), 0) < 0) return -1;
    return recv_reply(in, m);
}

/* ------------------------------ print_doc ------------------------------- */

// Takes a snapshot using special PRINT_DOC reads. Words that are currently
// being edited come back DENIED and are shown as "???".
static void snapshot_doc(void) {
    char buf[GRID_SIZE * (GRID_SIZE * (MAX_STRING_LEN + 1) + 1) + 1];
    size_t len = 0;
    buf[0] = '\0';

    for (int x = 0; x < GRID_SIZE; x++) {
        int printed = 0;
        for (int y = 0; y < GRID_SIZE; y++) {
            struct osdoc_msg m;
            memset(&m, 0, sizeof(m));
            m.type = MSG_PRINT_READ;
            m.x = x;
            m.y = y;
            const char *w;
            if (request(&m) < 0) {
                printf("Client %d: print_doc lost contact with server\n", my_id);
                return;
            }
            if (m.status == ST_DENIED) w = "???";
            else if (m.status == ST_OK) w = m.word;
            else w = "";
            if (w[0] == '\0') continue;
            len += snprintf(buf + len, sizeof(buf) - len, "%s%s", printed ? " " : "", w);
            printed = 1;
        }
        if (printed) len += snprintf(buf + len, sizeof(buf) - len, "\n");
    }
    print_doc(buf);
}

static void *print_doc_thread(void *arg) {
    (void)arg;
    pthread_mutex_lock(&pd_lock);
    while (!pd_stop) {
        pthread_mutex_unlock(&pd_lock);
        snapshot_doc();
        pthread_mutex_lock(&pd_lock);

        // Wait 2s, but wake immediately if the main thread asks us to stop.
        struct timespec dl;
        deadline_in(&dl, CLOCK_MONOTONIC, PRINT_DOC_INTERVAL_MS);
        while (!pd_stop) {
            if (pthread_cond_timedwait(&pd_cond, &pd_lock, &dl) == ETIMEDOUT) break;
        }
    }
    pthread_mutex_unlock(&pd_lock);
    return NULL;
}

/* ------------------------------- commands ------------------------------- */

static void do_read(int x, int y) {
    printf("Client %d: Requesting READ lock for (%d,%d)\n", my_id, x, y);
    struct osdoc_msg m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_READ;
    m.x = x;
    m.y = y;
    if (request(&m) < 0) {
        printf("Client %d: READ(%d,%d) ERROR - no reply from server\n", my_id, x, y);
        return;
    }
    if (m.status == ST_OK)
        printf("Client %d: READ(%d,%d) SUCCESS - Value: '%s'\n", my_id, x, y, m.word);
    else if (m.status == ST_DENIED)
        printf("Client %d: READ(%d,%d) DROPPED\n", my_id, x, y);
    else
        printf("Client %d: READ(%d,%d) ERROR\n", my_id, x, y);
}

static void do_write(int x, int y, const char *word, int duration) {
    printf("Client %d: Requesting WRITE lock for (%d,%d)\n", my_id, x, y);
    struct osdoc_msg m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_WRITE;
    m.x = x;
    m.y = y;
    m.duration_ms = duration;
    snprintf(m.word, sizeof(m.word), "%s", word);
    if (request(&m) < 0) {
        printf("Client %d: WRITE(%d,%d) ERROR - no reply from server\n", my_id, x, y);
        return;
    }
    if (m.status == ST_DENIED) {
        printf("Client %d: WRITE(%d,%d) DROPPED\n", my_id, x, y);
        return;
    }
    if (m.status != ST_OK) {
        printf("Client %d: WRITE(%d,%d) ERROR\n", my_id, x, y);
        return;
    }

    // We now hold exclusive edit access for `duration` ms.
    printf("Client %d: WRITE(%d,%d) = '%s', sleeping for %dms\n", my_id, x, y, word, duration);
    msleep(duration);

    memset(&m, 0, sizeof(m));
    m.type = MSG_UNLOCK;
    m.x = x;
    m.y = y;
    if (request(&m) < 0 || m.status != ST_OK)
        printf("Client %d: UNLOCK(%d,%d) failed\n", my_id, x, y);
    printf("Client %d: WRITE(%d,%d) COMPLETED\n", my_id, x, y);
}

static int valid_coord(int x, int y) {
    return x >= 0 && x < GRID_SIZE && y >= 0 && y < GRID_SIZE;
}

// Runs every command in input.txt that belongs to this client, in order.
static void run_commands(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        printf("Client %d: Could not open %s: %s\n", my_id, path, strerror(errno));
        return;
    }

    char line[512];
    while (fgets(line, sizeof(line), fp)) {
        int id;
        char cmd[16];
        if (sscanf(line, " C%d %15s", &id, cmd) != 2) continue;  // blank / junk
        if (id != my_id) continue;

        int x, y, t;
        char word[MAX_STRING_LEN + 2];
        char extra;

        if (strcmp(cmd, "READ") == 0) {
            if (sscanf(line, " C%*d READ %d %d %c", &x, &y, &extra) != 2 || !valid_coord(x, y)) {
                printf("Client %d: Invalid command: %s", my_id, line);
                continue;
            }
            do_read(x, y);
        } else if (strcmp(cmd, "WRITE") == 0) {
            // %65s lets us detect words longer than the 64-char limit.
            if (sscanf(line, " C%*d WRITE %d %d %65s %d %c", &x, &y, word, &t, &extra) != 4 ||
                !valid_coord(x, y) || strlen(word) > MAX_STRING_LEN || t < 0) {
                printf("Client %d: Invalid command: %s", my_id, line);
                continue;
            }
            do_write(x, y, word, t);
        } else if (strcmp(cmd, "SLEEP") == 0) {
            if (sscanf(line, " C%*d SLEEP %d %c", &t, &extra) != 1 || t < 0) {
                printf("Client %d: Invalid command: %s", my_id, line);
                continue;
            }
            printf("Client %d: Sleeping for %dms\n", my_id, t);
            msleep(t);
        } else {
            printf("Client %d: Unknown command: %s", my_id, line);
        }
    }
    fclose(fp);
}

/* ---------------------------- setup / teardown -------------------------- */

static mqd_t make_queue(const char *name, int flags) {
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg = QUEUE_MAXMSG;
    attr.mq_msgsize = sizeof(struct osdoc_msg);
    mq_unlink(name);  // drop a stale queue from a crashed earlier run
    return mq_open(name, O_CREAT | O_EXCL | flags, 0660, &attr);
}

static void cleanup_queues(void) {
    if (server_q != (mqd_t)-1) mq_close(server_q);
    if (req_q != (mqd_t)-1) mq_close(req_q);
    if (reply_q != (mqd_t)-1) mq_close(reply_q);
    if (doc_q != (mqd_t)-1) mq_close(doc_q);
    mq_unlink(req_name);
    mq_unlink(reply_name);
    mq_unlink(doc_name);
}

static int connect_to_server(void) {
    snprintf(req_name, sizeof(req_name), "/client_queue_%d_req", my_id);
    snprintf(reply_name, sizeof(reply_name), "/client_queue_%d", my_id);
    snprintf(doc_name, sizeof(doc_name), "/client_queue_%d_doc", my_id);

    req_q = make_queue(req_name, O_WRONLY);
    reply_q = make_queue(reply_name, O_RDONLY);
    doc_q = make_queue(doc_name, O_RDONLY);
    if (req_q == (mqd_t)-1 || reply_q == (mqd_t)-1 || doc_q == (mqd_t)-1) {
        printf("Client %d: Failed to create message queues: %s\n", my_id, strerror(errno));
        return -1;
    }

    // The server may still be starting up; retry for a few seconds.
    for (int i = 0; i < 50; i++) {
        server_q = mq_open(SERVER_QUEUE, O_WRONLY);
        if (server_q != (mqd_t)-1) break;
        msleep(100);
    }
    if (server_q == (mqd_t)-1) {
        printf("Client %d: Could not reach server: %s\n", my_id, strerror(errno));
        return -1;
    }

    struct osdoc_msg m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_CONNECT;
    m.client_id = my_id;
    if (mq_send(server_q, (const char *)&m, sizeof(m), 0) < 0 ||
        recv_reply(reply_q, &m) < 0 || m.type != MSG_CONNECT || m.status != ST_OK) {
        printf("Client %d: Server did not accept connection\n", my_id);
        return -1;
    }
    printf("Client %d: Connected to server\n", my_id);
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <client_id>\n", argv[0]);
        return 1;
    }

    int client_id = atoi(argv[1]);
    client_id_global = client_id;
    my_id = client_id;
    printf("Client %d: Starting\n", client_id);

    if (client_id < 0 || client_id > 9) {
        printf("Client %d: Client id must be in range 0-9\n", client_id);
        return 1;
    }

    if (connect_to_server() < 0) {
        cleanup_queues();
        return 1;
    }

    // print_doc thread: snapshot now and then every 2 seconds until exit.
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&pd_cond, &ca);
    pthread_condattr_destroy(&ca);

    pthread_t pd_tid;
    int pd_started = pthread_create(&pd_tid, NULL, print_doc_thread, NULL) == 0;
    if (!pd_started) printf("Client %d: Failed to start print_doc thread\n", client_id);

    run_commands("input.txt");

    if (pd_started) {
        pthread_mutex_lock(&pd_lock);
        pd_stop = 1;
        pthread_cond_signal(&pd_cond);
        pthread_mutex_unlock(&pd_lock);
        pthread_join(pd_tid, NULL);
    }

    struct osdoc_msg m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_DISCONNECT;
    request(&m);

    cleanup_queues();
    printf("Client %d: Exiting\n", client_id);
    return 0;
}
