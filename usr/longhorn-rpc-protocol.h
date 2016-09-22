#ifndef LONGHORN_RPC_PROTOCOL_HEADER
#define LONGHORN_RPC_PROTOCOL_HEADER

#include <pthread.h>

#include "uthash.h"

struct Message {
        uint32_t        Seq;
        uint32_t        Type;
        int64_t         Offset;
        uint32_t        DataLength;
        void*           Data;

	pthread_cond_t  cond;
	pthread_mutex_t mutex;

        UT_hash_handle hh;
};

enum uint32_t {
	TypeRead,
	TypeWrite,
	TypeResponse,
	TypeError,
	TypeEOF
};

int send_msg(int fd, struct Message *msg);
int receive_msg(int fd, struct Message *msg);

#endif
