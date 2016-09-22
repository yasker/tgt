#ifndef LONGHORN_RPC_CLIENT_HEADER
#define LONGHORN_RPC_CLIENT_HEADER

#include <pthread.h>

#include "longhorn-rpc-protocol.h"

struct client_connection {
        int seq;  // must be atomic
        int fd;
        int notify_fd;

        pthread_t response_thread;

        struct Message *msg_table;
        pthread_mutex_t mutex;
};

struct client_connection *new_client_connection(char *socket_path);
int shutdown_client_connection(struct client_connection *conn);

int read_at(struct client_connection *conn, void *buf, size_t count, off_t offset);
int write_at(struct client_connection *conn, void *buf, size_t count, off_t offset);

void start_response_processing(struct client_connection *conn);

#endif
