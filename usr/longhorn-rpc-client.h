#ifndef LONGHORN_RPC_CLIENT_HEADER
#define LONGHORN_RPC_CLIENT_HEADER

#include <pthread.h>

#include "longhorn-rpc-protocol.h"

struct client_connection {
        int seq;  // must be atomic
        int fd;
        int notify_fd;
        int timeout_fd;
        int state;
        pthread_mutex_t mutex;

        pthread_t response_thread;
        pthread_t timeout_thread;

        struct Message *msg_hashtable;
        struct Message *msg_list;
        pthread_mutex_t msg_mutex;
};

enum {
        CLIENT_CONN_STATE_OPEN = 0,
        CLIENT_CONN_STATE_CLOSE,
};

struct client_connection *new_client_connection(char *socket_path);
int shutdown_client_connection(struct client_connection *conn);

int read_at(struct client_connection *conn, void *buf, size_t count, off_t offset);
int write_at(struct client_connection *conn, void *buf, size_t count, off_t offset);

void start_response_processing(struct client_connection *conn);

#endif
