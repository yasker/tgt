#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "longhorn-rpc-client.h"
#include "longhorn-rpc-protocol.h"

int send_request(struct client_connection *conn, struct Message *req) {
        int rc = 0;

        pthread_mutex_lock(&conn->mutex);
        rc = send_msg(conn->fd, req);
        pthread_mutex_unlock(&conn->mutex);
        return rc;
}

int receive_response(struct client_connection *conn, struct Message *resp) {
        int rc = 0;

        rc = receive_msg(conn->fd, resp);
        return rc;
}

void* response_process(void *arg) {
        struct client_connection *conn = arg;
        struct Message *req, *resp;
        int ret = 0;

	resp = malloc(sizeof(struct Message));
        if (resp == NULL) {
            perror("cannot allocate memory for resp");
            return NULL;
        }

        // TODO Need to add multiple event poll to gracefully shutdown
	ret = receive_response(conn, resp);
        while (ret == 0) {
                /*
                if (resp->Seq % 1024 == 0) {
                        printf("Received seq %d\n", resp->Seq);
                }
                */

                if (resp->Type != TypeResponse) {
                        fprintf(stderr, "Wrong type for response of seq %d\n",
                                        resp->Seq);
                        continue;
                }

                pthread_mutex_lock(&conn->mutex);
                HASH_FIND_INT(conn->msg_table, &resp->Seq, req);
                if (req != NULL) {
                        HASH_DEL(conn->msg_table, req);
                }
                pthread_mutex_unlock(&conn->mutex);

                pthread_mutex_lock(&req->mutex);
                memcpy(req->Data, resp->Data, req->DataLength);
                free(resp->Data);
                pthread_mutex_unlock(&req->mutex);

                pthread_cond_signal(&req->cond);

                ret = receive_response(conn, resp);
        }
        free(resp);
        if (ret != 0) {
                fprintf(stderr, "Receive response returned error");
        }
        return NULL;
}

void start_response_processing(struct client_connection *conn) {
        int rc;

        rc = pthread_create(&conn->response_thread, NULL, &response_process, conn);
        if (rc < 0) {
                perror("Fail to create response thread");
                exit(-1);
        }
}

int new_seq(struct client_connection *conn) {
        return __sync_fetch_and_add(&conn->seq, 1);
}

int process_request(struct client_connection *conn, void *buf, size_t count, off_t offset, 
                uint32_t type) {
        struct Message *req = malloc(sizeof(struct Message));
        int rc = 0;

        if (req == NULL) {
                perror("cannot allocate memory for req");
                return -EINVAL;
        }

        if (type != TypeRead && type != TypeWrite) {
                fprintf(stderr, "BUG: Invalid type for process_request %d\n", type);
                rc = -EFAULT;
                goto free;
        }
        req->Seq = new_seq(conn);
        req->Type = type;
        req->Offset = offset;
        req->DataLength = count;
        req->Data = buf;

        if (req->Type == TypeRead) {
                bzero(req->Data, count);
        }

        rc = pthread_cond_init(&req->cond, NULL);
        if (rc < 0) {
                perror("Fail to init phread_cond");
                rc = -EFAULT;
                goto free;
        }
        rc = pthread_mutex_init(&req->mutex, NULL);
        if (rc < 0) {
                perror("Fail to init phread_mutex");
                rc = -EFAULT;
                goto free;
        }

        pthread_mutex_lock(&conn->mutex);
        HASH_ADD_INT(conn->msg_table, Seq, req);
        pthread_mutex_unlock(&conn->mutex);

        pthread_mutex_lock(&req->mutex);
        rc = send_request(conn, req);
        if (rc < 0) {
                goto out;
        }

        pthread_cond_wait(&req->cond, &req->mutex);
out:
        pthread_mutex_unlock(&req->mutex);
free:
        free(req);
        return rc;
}

int read_at(struct client_connection *conn, void *buf, size_t count, off_t offset) {
        return process_request(conn, buf, count, offset, TypeRead);
}

int write_at(struct client_connection *conn, void *buf, size_t count, off_t offset) {
        return process_request(conn, buf, count, offset, TypeWrite);
}

struct client_connection *new_client_connection(char *socket_path) {
        struct sockaddr_un addr;
        int fd, rc = 0;
        struct client_connection *conn = NULL;

        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd == -1) {
                perror("socket error");
                exit(-1);
        }

        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        if (strlen(socket_path) >= 108) {
                fprintf(stderr, "socket path is too long, more than 108 characters");
                exit(-EINVAL);
        }

        strncpy(addr.sun_path, socket_path, strlen(socket_path));

        if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
                perror("connect error");
                exit(-EFAULT);
        }

        conn = malloc(sizeof(struct client_connection));
        if (conn == NULL) {
            perror("cannot allocate memory for conn");
            return NULL;
        }

        conn->fd = fd;
        conn->seq = 0;
        conn->msg_table = NULL;

        rc = pthread_mutex_init(&conn->mutex, NULL);
        if (rc < 0) {
                perror("fail to init conn->mutex");
                exit(-EFAULT);
        }
        return conn;
}

int shutdown_client_connection(struct client_connection *conn) {
        close(conn->fd);
        free(conn);
        return 0;
}
