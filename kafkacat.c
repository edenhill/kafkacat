/*
 * kafkacat - Apache Kafka consumer and producer
 *
 * Copyright (c) 2014-2020, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _MSC_VER
#include <unistd.h>
#include <syslog.h>
#include <sys/time.h>
#include <sys/mman.h>
#else
#pragma comment(lib, "ws2_32.lib")
#include "win32/wingetopt.h"
#include <io.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <signal.h>
#include <ctype.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>



#include "kafkacat.h"
#include "input.h"

#if RD_KAFKA_VERSION >= 0x01040000
#define ENABLE_TXNS 1
#endif

#if RD_KAFKA_VERSION >= 0x01060000
#define ENABLE_INCREMENTAL_ASSIGN 1
#endif


struct conf conf = {
        .run = 1,
        .verbosity = 1,
        .exitonerror = 1,
        .partition = RD_KAFKA_PARTITION_UA,
        .msg_size = 1024*1024,
        .null_str = "NULL",
        .fixed_key = NULL,
        .metadata_timeout = 5,
        .offset = RD_KAFKA_OFFSET_INVALID,
};

static struct stats {
        uint64_t tx;
        uint64_t tx_err_q;
        uint64_t tx_err_dr;
        uint64_t tx_delivered;

        uint64_t rx;
} stats;


/* Partition's stopped state array */
int *part_stop = NULL;
/* Number of partitions that are stopped */
int part_stop_cnt = 0;
/* Threshold level (partitions stopped) before exiting */
int part_stop_thres = 0;



/**
 * Fatal error: print error and exit
 */
void RD_NORETURN fatal0 (const char *func, int line,
                         const char *fmt, ...) {
        va_list ap;
        char buf[1024];

        va_start(ap, fmt);
        vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);

        KC_INFO(2, "Fatal error at %s:%i:\n", func, line);
        fprintf(stderr, "%% ERROR: %s\n", buf);
        exit(1);
}

/**
 * Print error and exit if needed
 */
void error0 (int exitonerror, const char *func, int line,
             const char *fmt, ...) {
        va_list ap;
        char buf[1024];

        va_start(ap, fmt);
        vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);

        if (exitonerror)
                KC_INFO(2, "Error at %s:%i:\n", func, line);

        fprintf(stderr, "%% ERROR: %s%s\n",
                buf, exitonerror ? " : terminating":"");

        if (exitonerror)
                exit(1);
}



/**
 * The delivery report callback is called once per message to
 * report delivery success or failure.
 */
static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage,
                       void *opaque) {
        static int say_once = 1;
        int32_t broker_id = -1;
        struct buf *b = rkmessage->_private;

        if (b)
                buf_destroy(b);

        if (rkmessage->err) {
                KC_INFO(1, "Delivery failed for message: %s\n",
                        rd_kafka_err2str(rkmessage->err));
                stats.tx_err_dr++;
                return;
        }

#if RD_KAFKA_VERSION >= 0x010500ff
        broker_id = rd_kafka_message_broker_id(rkmessage);
#endif

        KC_INFO(3,
                "Message delivered to partition %"PRId32" (offset %"PRId64") "
                "on broker %"PRId32"\n",
                rkmessage->partition, rkmessage->offset, broker_id);

        if (rkmessage->offset == 0 && say_once) {
                KC_INFO(3, "Enable message offset reporting "
                        "with '-X topic.produce.offset.report=true'\n");
                say_once = 0;
        }
        stats.tx_delivered++;
}


/**
 * Produces a single message, retries on queue congestion, and
 * exits hard on error.
 */
static void produce (void *buf, size_t len,
                     const void *key, size_t key_len, int msgflags,
                     void *msg_opaque) {
        rd_kafka_headers_t *hdrs = NULL;

        /* Headers are freed on successful producev(), pass a copy. */
        if (conf.headers)
                hdrs = rd_kafka_headers_copy(conf.headers);

        /* Produce message: keep trying until it succeeds. */
        do {
                rd_kafka_resp_err_t err;

                if (!conf.run)
                        KC_FATAL("Program terminated while "
                                 "producing message of %zd bytes", len);

                err = rd_kafka_producev(
                        conf.rk,
                        RD_KAFKA_V_RKT(conf.rkt),
                        RD_KAFKA_V_PARTITION(conf.partition),
                        RD_KAFKA_V_MSGFLAGS(msgflags),
                        RD_KAFKA_V_VALUE(buf, len),
                        RD_KAFKA_V_KEY(key, key_len),
                        RD_KAFKA_V_HEADERS(hdrs),
                        RD_KAFKA_V_OPAQUE(msg_opaque),
                        RD_KAFKA_V_END);

                if (!err) {
                        stats.tx++;
                        break;
                }

                if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL)
                        KC_FATAL("Failed to produce message (%zd bytes): %s",
                                 len, rd_kafka_err2str(err));

                stats.tx_err_q++;

                /* Internal queue full, sleep to allow
                 * messages to be produced/time out
                 * before trying again. */
                rd_kafka_poll(conf.rk, 5);
        } while (1);

        /* Poll for delivery reports, errors, etc. */
        rd_kafka_poll(conf.rk, 0);
}


/**
 * Produce contents of file as a single message.
 * Returns the file length on success, else -1.
 */
static ssize_t produce_file (const char *path) {
        int fd;
        void *ptr;
        struct stat st;
        ssize_t sz;
        int msgflags = 0;

        if ((fd = _COMPAT(open)(path, O_RDONLY)) == -1) {
                KC_INFO(1, "Failed to open %s: %s\n", path, strerror(errno));
                return -1;
        }

        if (fstat(fd, &st) == -1) {
                KC_INFO(1, "Failed to stat %s: %s\n", path, strerror(errno));
                _COMPAT(close)(fd);
                return -1;
        }

        if (st.st_size == 0) {
                KC_INFO(3, "Skipping empty file %s\n", path);
                _COMPAT(close)(fd);
                return 0;
        }

#ifndef _MSC_VER
        ptr = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
                KC_INFO(1, "Failed to mmap %s: %s\n", path, strerror(errno));
                _COMPAT(close)(fd);
                return -1;
        }
        sz = st.st_size;
        msgflags = RD_KAFKA_MSG_F_COPY;
#else
        ptr = malloc(st.st_size);
        if (!ptr) {
                KC_INFO(1, "Failed to allocate message for %s: %s\n",
                        path, strerror(errno));
                _COMPAT(close)(fd);
                return -1;
        }

        sz = _read(fd, ptr, st.st_size);
        if (sz < st.st_size) {
                KC_INFO(1, "Read failed for %s (%zd/%zd): %s\n",
                        path, sz, (size_t)st.st_size, sz == -1 ? strerror(errno) :
                        "incomplete read");
                free(ptr);
                close(fd);
                return -1;
        }
        msgflags = RD_KAFKA_MSG_F_FREE;
#endif

        KC_INFO(4, "Producing file %s (%"PRIdMAX" bytes)\n",
                path, (intmax_t)st.st_size);
        produce(ptr, sz, conf.fixed_key, conf.fixed_key_len, msgflags, NULL);

        _COMPAT(close)(fd);

        if (!(msgflags & RD_KAFKA_MSG_F_FREE)) {
#ifndef _MSC_VER
                munmap(ptr, st.st_size);
#else
                free(ptr);
#endif
        }
        return sz;
}


static char *rd_strnstr (const char *haystack, size_t size,
                         const char *needle, size_t needle_len) {
        const char *nend = needle + needle_len - 1;
        const char *t;
        size_t of = needle_len - 1;

        while (of < size &&
               (t = (const char *)memchr((void *)(haystack + of),
                                         (int)*nend,
                                         size - of))) {
                const char *n = nend;
                const char *p = t;

                do {
                        n--;
                        p--;
                } while (n >= needle && *n == *p);

                if (n < needle)
                        return (char *)p+1;

                of = (size_t)(t - haystack) + 1;
        }

        return NULL;
}


/**
 * Run producer, reading messages from 'fp' and producing to kafka.
 * Or if 'pathcnt' is > 0, read messages from files in 'paths' instead.
 */
static void producer_run (FILE *fp, char **paths, int pathcnt) {
        char    errstr[512];
        char    tmp[16];
        size_t  tsize = sizeof(tmp);

        if (rd_kafka_conf_get(conf.rk_conf, "transactional.id",
                              tmp, &tsize) == RD_KAFKA_CONF_OK && tsize > 1) {
                KC_INFO(1, "Using transactional producer\n");
                conf.txn = 1;
        }

        tsize = sizeof(tmp);
        if (rd_kafka_conf_get(conf.rk_conf, "message.max.bytes",
                              tmp, &tsize) == RD_KAFKA_CONF_OK && tsize > 1) {
                int msg_max_bytes = atoi(tmp);
                KC_INFO(3, "Setting producer input buffer max size to "
                        "message.max.bytes value %d\n", msg_max_bytes);
                conf.msg_size = msg_max_bytes;
        }

        /* Assign per-message delivery report callback. */
        rd_kafka_conf_set_dr_msg_cb(conf.rk_conf, dr_msg_cb);

        /* Create producer */
        if (!(conf.rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf.rk_conf,
                                     errstr, sizeof(errstr))))
                KC_FATAL("Failed to create producer: %s", errstr);

        if (!conf.debug && conf.verbosity == 0)
                rd_kafka_set_log_level(conf.rk, 0);

#if ENABLE_TXNS
        if (conf.txn) {
                rd_kafka_error_t *error;

                error = rd_kafka_init_transactions(conf.rk,
                                                   conf.metadata_timeout*1000);
                if (error)
                        KC_FATAL("init_transactions(): %s",
                                 rd_kafka_error_string(error));

                error = rd_kafka_begin_transaction(conf.rk);
                if (error)
                        KC_FATAL("begin_transaction(): %s",
                                 rd_kafka_error_string(error));
        }
#endif

        /* Create topic */
        if (!(conf.rkt = rd_kafka_topic_new(conf.rk, conf.topic,
                                            conf.rkt_conf)))
                KC_FATAL("Failed to create topic %s: %s", conf.topic,
                         rd_kafka_err2str(rd_kafka_last_error()));

        conf.rk_conf  = NULL;
        conf.rkt_conf = NULL;


        if (pathcnt > 0 && !(conf.flags & CONF_F_LINE)) {
                int i;
                int good = 0;
                /* Read messages from files, each file is its own message. */

                for (i = 0 ; i < pathcnt ; i++)
                        if (produce_file(paths[i]) != -1)
                                good++;

                if (!good)
                        conf.exitcode = 1;
                else if (good < pathcnt)
                        KC_INFO(1, "Failed to produce from %i/%i files\n",
                                pathcnt - good, pathcnt);

        } else {
                struct inbuf inbuf;
                struct buf *b;

                inbuf_init(&inbuf, conf.msg_size, conf.delim, conf.delim_size);

                /* Read messages from input, delimited by conf.delim */
                while (conf.run &&
                       inbuf_read_to_delimeter(&inbuf, fp, &b)) {
                        int msgflags = 0;
                        char *buf = b->buf;
                        char *key = NULL;
                        size_t key_len = 0;
                        size_t len = b->size;

                        if (len == 0) {
                                buf_destroy(b);
                                continue;
                        }

                        /* Extract key, if desired and found. */
                        if (conf.flags & CONF_F_KEY_DELIM) {
                                char *t;
                                if ((t = rd_strnstr(buf, len,
                                                    conf.key_delim,
                                                    conf.key_delim_size))) {
                                        key_len = (size_t)(t-buf);
                                        key     = buf;
                                        buf     = t + conf.key_delim_size;
                                        len    -= key_len + conf.key_delim_size;

                                        if (conf.flags & CONF_F_NULL) {
                                                if (len == 0)
                                                        buf = NULL;
                                                if (key_len == 0)
                                                        key = NULL;
                                        }
                                }
                        }

                        if (!key && conf.fixed_key) {
                                key = conf.fixed_key;
                                key_len = conf.fixed_key_len;
                        }

                        if (len < 1024) {
                                /* If message is smaller than this arbitrary
                                 * threshold it will be more effective to
                                 * copy the data in librdkafka. */
                                msgflags |= RD_KAFKA_MSG_F_COPY;
                        }

                        /* Produce message */
                        produce(buf, len, key, key_len, msgflags,
                                (msgflags & RD_KAFKA_MSG_F_COPY) ?
                                NULL : b);

                        if (conf.flags & CONF_F_TEE &&
                            fwrite(b->buf, b->size, 1, stdout) != 1)
                                KC_FATAL("Tee write error for message "
                                         "of %zd bytes: %s",
                                         b->size, strerror(errno));

                        if (msgflags & RD_KAFKA_MSG_F_COPY) {
                                /* librdkafka made a copy of the input. */
                                buf_destroy(b);
                        }

                        /* Enforce -c <cnt> */
                        if (stats.tx == (uint64_t)conf.msg_cnt)
                                conf.run = 0;
                }

                if (conf.run) {
                        if (!feof(fp))
                                KC_FATAL("Unable to read message: %s",
                                         strerror(errno));
                }
        }

#if ENABLE_TXNS
        if (conf.txn) {
                rd_kafka_error_t *error;
                const char *what;

                if (conf.term_sig) {
                        KC_INFO(0,
                                "Aborting transaction due to "
                                "termination signal\n");
                        what = "abort_transaction()";
                        error = rd_kafka_abort_transaction(
                                conf.rk, conf.metadata_timeout * 1000);
                } else {
                        KC_INFO(1, "Committing transaction\n");
                        what = "commit_transaction()";
                        error = rd_kafka_commit_transaction(
                                conf.rk, conf.metadata_timeout * 1000);
                        if (!error)
                                KC_INFO(1,
                                        "Transaction successfully committed\n");
                }

                if (error)
                        KC_FATAL("%s: %s", what, rd_kafka_error_string(error));
        }
#endif


        /* Wait for all messages to be transmitted */
        conf.run = 1;
        while (conf.run && rd_kafka_outq_len(conf.rk))
                rd_kafka_poll(conf.rk, 50);

        rd_kafka_topic_destroy(conf.rkt);
        rd_kafka_destroy(conf.rk);

        if (stats.tx_err_dr)
                conf.exitcode = 1;
}

static void stop_partition (rd_kafka_message_t *rkmessage) {
        if (!part_stop[rkmessage->partition]) {
                /* Stop consuming this partition */
                rd_kafka_consume_stop(rkmessage->rkt,
                                      rkmessage->partition);
                part_stop[rkmessage->partition] = 1;
                part_stop_cnt++;
                if (part_stop_cnt >= part_stop_thres)
                        conf.run = 0;
        }
}


/**
 * @brief Mark partition as not at EOF
 */
static void partition_not_eof (const rd_kafka_message_t *rkmessage) {
        rd_kafka_topic_partition_t *rktpar;

        if (conf.mode != 'G' || !conf.exit_eof ||
            !conf.assignment || conf.eof_cnt == 0)
                return;

        /* Find partition in assignment */
        rktpar = rd_kafka_topic_partition_list_find(
                conf.assignment,
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition);

        if (!rktpar || rktpar->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                return;

        rktpar->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        conf.eof_cnt--;
}


/**
 * @brief Mark partition as at EOF
 */
static void partition_at_eof (rd_kafka_message_t *rkmessage) {

        if (conf.mode == 'C') {
                /* Store EOF offset.
                 * If partition is empty and at offset 0,
                 * store future first message (0). */
                rd_kafka_offset_store(rkmessage->rkt,
                                      rkmessage->partition,
                                      rkmessage->offset == 0 ?
                                      0 : rkmessage->offset-1);
                if (conf.exit_eof) {
                        stop_partition(rkmessage);
                }

        } else if (conf.mode == 'G' && conf.exit_eof && conf.assignment) {
                /* Find partition in assignment */
                rd_kafka_topic_partition_t *rktpar;

                rktpar = rd_kafka_topic_partition_list_find(
                        conf.assignment,
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition);

                if (rktpar && rktpar->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        rktpar->err = RD_KAFKA_RESP_ERR__PARTITION_EOF;
                        conf.eof_cnt++;

                        if (conf.eof_cnt == conf.assignment->cnt)
                                conf.run = 0;
                }
        }

        KC_INFO(1, "Reached end of topic %s [%"PRId32"] "
                "at offset %"PRId64"%s\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset,
                !conf.run ? ": exiting" : "");
}


/**
 * Consume callback, called for each message consumed.
 */
static void consume_cb (rd_kafka_message_t *rkmessage, void *opaque) {
        FILE *fp = opaque;

        if (!conf.run)
                return;

        if (rkmessage->err) {
                if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        partition_at_eof(rkmessage);
                        return;
                }

                if (rkmessage->rkt)
                        KC_FATAL("Topic %s [%"PRId32"] error: %s",
                                 rd_kafka_topic_name(rkmessage->rkt),
                                 rkmessage->partition,
                                 rd_kafka_message_errstr(rkmessage));
                else
                        KC_FATAL("Consumer error: %s",
                                 rd_kafka_message_errstr(rkmessage));

        } else {
                partition_not_eof(rkmessage);
        }

        if (conf.stopts) {
                int64_t ts = rd_kafka_message_timestamp(rkmessage, NULL);
                if (ts >= conf.stopts) {
                        stop_partition(rkmessage);
                        KC_INFO(1, "Reached stop timestamp for topic "
                                "%s [%"PRId32"] "
                                "at offset %"PRId64"%s\n",
                                rd_kafka_topic_name(rkmessage->rkt),
                                rkmessage->partition,
                                rkmessage->offset,
                                !conf.run ? ": exiting" : "");
                        return;
                }
        }

        /* Print message */
        fmt_msg_output(fp, rkmessage);

        if (conf.mode == 'C') {
                rd_kafka_offset_store(rkmessage->rkt,
                                      rkmessage->partition,
                                      rkmessage->offset);
        }

        if (++stats.rx == (uint64_t)conf.msg_cnt) {
                conf.run = 0;
                rd_kafka_yield(conf.rk);
        }
}


#if RD_KAFKA_VERSION >= 0x00090000
static void throttle_cb (rd_kafka_t *rk, const char *broker_name,
                         int32_t broker_id, int throttle_time_ms, void *opaque){
        KC_INFO(1, "Broker %s (%"PRId32") throttled request for %dms\n",
                broker_name, broker_id, throttle_time_ms);
}
#endif

#if ENABLE_KAFKACONSUMER
static void print_partition_list (int is_assigned,
                                  const rd_kafka_topic_partition_list_t
                                  *partitions) {
        int i;
        for (i = 0 ; i < partitions->cnt ; i++) {
                fprintf(stderr, "%s%s [%"PRId32"]",
                        i > 0 ? ", ":"",
                        partitions->elems[i].topic,
                        partitions->elems[i].partition);
        }
        fprintf(stderr, "\n");
}


#if ENABLE_INCREMENTAL_ASSIGN
static void
incremental_rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {
        rd_kafka_error_t *error = NULL;
        int i;

        KC_INFO(1, "Group %s rebalanced: incremental %s of %d partition(s) "
                "(memberid %s%s, %s rebalance protocol): ",
                conf.group,
                err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ?
                "assignment" : "revoke",
                partitions->cnt,
                rd_kafka_memberid(rk),
                rd_kafka_assignment_lost(rk) ? ", assignment lost" : "",
                rd_kafka_rebalance_protocol(rk));

        switch (err)
        {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                if (conf.verbosity >= 1)
                        print_partition_list(1, partitions);

                if (!conf.assignment)
                        conf.assignment =
                                rd_kafka_topic_partition_list_new(
                                        partitions->cnt);

                for (i = 0 ; i < partitions->cnt ; i++) {
                        rd_kafka_topic_partition_t *rktpar =
                                &partitions->elems[i];

                        /* Set start offset from -o .. */
                        if (conf.offset != RD_KAFKA_OFFSET_INVALID)
                                rktpar->offset = conf.offset;

                        rktpar->offset = conf.offset;

                        rd_kafka_topic_partition_list_add(conf.assignment,
                                                          rktpar->topic,
                                                          rktpar->partition);
                }
                error = rd_kafka_incremental_assign(rk, partitions);
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                if (conf.verbosity >= 1)
                        print_partition_list(1, partitions);

                /* Remove partitions from conf.assignment in reverse order
                 * to minimize array shrink sizes. */
                for (i = partitions->cnt - 1 ;
                     conf.assignment && i >= 0 ;
                     i--) {
                        rd_kafka_topic_partition_t *rktpar =
                                &partitions->elems[i];

                        rd_kafka_topic_partition_list_del(conf.assignment,
                                                          rktpar->topic,
                                                          rktpar->partition);
                }

                error = rd_kafka_incremental_unassign(rk, partitions);
                break;

        default:
                KC_INFO(0, "failed: %s\n", rd_kafka_err2str(err));
                break;
        }

        if (error) {
                KC_ERROR("Incremental rebalance %s of %d partition(s) "
                         "failed: %s\n",
                         err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ?
                         "assign" : "unassign",
                         partitions->cnt, rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        }
}
#endif

static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {
        rd_kafka_resp_err_t ret_err = RD_KAFKA_RESP_ERR_NO_ERROR;

#if ENABLE_INCREMENTAL_ASSIGN
        if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE")) {
                incremental_rebalance_cb(rk, err, partitions, opaque);
                return;
        }
#endif

        KC_INFO(1, "Group %s rebalanced (memberid %s): ",
                conf.group, rd_kafka_memberid(rk));

        switch (err)
        {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                if (conf.verbosity >= 1) {
                        fprintf(stderr, "assigned: ");
                        print_partition_list(1, partitions);
                }
                if (conf.offset != RD_KAFKA_OFFSET_INVALID) {
                        int i;
                        for (i = 0 ; i < partitions->cnt ; i++)
                                partitions->elems[i].offset = conf.offset;
                }

                if (conf.assignment)
                        rd_kafka_topic_partition_list_destroy(conf.assignment);
                conf.assignment =
                        rd_kafka_topic_partition_list_copy(partitions);

                ret_err = rd_kafka_assign(rk, partitions);
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                if (conf.verbosity >= 1) {
                        fprintf(stderr, "revoked: ");
                        print_partition_list(1, partitions);
                }

                if (conf.assignment) {
                        rd_kafka_topic_partition_list_destroy(conf.assignment);
                        conf.assignment = NULL;
                }

                ret_err = rd_kafka_assign(rk, NULL);
                break;

        default:
                KC_INFO(0, "failed: %s\n", rd_kafka_err2str(err));
                break;
        }

        if (ret_err)
                KC_ERROR("Rebalance %s of %d partition(s) failed: %s\n",
                         err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ?
                         "assign" : "unassign",
                         partitions->cnt, rd_kafka_err2str(ret_err));
}

/**
 * Run high-level KafkaConsumer, write messages to 'fp'
 */
static void kafkaconsumer_run (FILE *fp, char *const *topics, int topic_cnt) {
        char    errstr[512];
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *topiclist;
        int i;

        rd_kafka_conf_set_rebalance_cb(conf.rk_conf, rebalance_cb);
        rd_kafka_conf_set_default_topic_conf(conf.rk_conf, conf.rkt_conf);
        conf.rkt_conf = NULL;

        /* Create consumer */
        if (!(conf.rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf.rk_conf,
                                     errstr, sizeof(errstr))))
                KC_FATAL("Failed to create consumer: %s", errstr);
        conf.rk_conf  = NULL;

        /* Forward main event queue to consumer queue so we can
         * serve both queues with a single consumer_poll() call. */
        rd_kafka_poll_set_consumer(conf.rk);

        if (conf.debug)
                rd_kafka_set_log_level(conf.rk, LOG_DEBUG);
        else if (conf.verbosity == 0)
                rd_kafka_set_log_level(conf.rk, 0);

        /* Build subscription set */
        topiclist = rd_kafka_topic_partition_list_new(topic_cnt);
        for (i = 0 ; i < topic_cnt ; i++)
                rd_kafka_topic_partition_list_add(topiclist, topics[i], -1);

        /* Subscribe */
        if ((err = rd_kafka_subscribe(conf.rk, topiclist)))
                KC_FATAL("Failed to subscribe to %d topics: %s\n",
                         topiclist->cnt, rd_kafka_err2str(err));

        KC_INFO(1, "Waiting for group rebalance\n");

        rd_kafka_topic_partition_list_destroy(topiclist);

        /* Read messages from Kafka, write to 'fp'. */
        while (conf.run) {
                rd_kafka_message_t *rkmessage;

                rkmessage = rd_kafka_consumer_poll(conf.rk, 100);
                if (!rkmessage)
                        continue;

                consume_cb(rkmessage, fp);

                rd_kafka_message_destroy(rkmessage);
        }

        if ((err = rd_kafka_consumer_close(conf.rk)))
                KC_FATAL("Failed to close consumer: %s\n",
                         rd_kafka_err2str(err));

        /* Wait for outstanding requests to finish. */
        conf.run = 1;
        while (conf.run && rd_kafka_outq_len(conf.rk) > 0)
                rd_kafka_poll(conf.rk, 50);

        if (conf.assignment)
                rd_kafka_topic_partition_list_destroy(conf.assignment);

        rd_kafka_destroy(conf.rk);
}
#endif

/**
 * Get offsets from conf.startts for consumer_run
 */
static int64_t *get_offsets (rd_kafka_metadata_topic_t *topic) {
        int i;
        int64_t *offsets;
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *rktparlistp =
                rd_kafka_topic_partition_list_new(1);

        for (i = 0 ; i < topic->partition_cnt ; i++) {
                int32_t partition = topic->partitions[i].id;

                /* If -p <part> was specified: skip unwanted partitions */
                if (conf.partition != RD_KAFKA_PARTITION_UA &&
                    conf.partition != partition)
                        continue;

                rd_kafka_topic_partition_list_add(
                        rktparlistp,
                        rd_kafka_topic_name(conf.rkt),
                        partition)->offset = conf.startts;

                if (conf.partition != RD_KAFKA_PARTITION_UA)
                        break;
        }
        err = rd_kafka_offsets_for_times(conf.rk, rktparlistp,
                                         conf.metadata_timeout * 1000);
        if (err)
                KC_FATAL("offsets_for_times failed: %s", rd_kafka_err2str(err));

        offsets = calloc(sizeof(int64_t), topic->partition_cnt);
        for (i = 0 ; i < rktparlistp->cnt ; i++) {
                const rd_kafka_topic_partition_t *p = &rktparlistp->elems[i];
                offsets[p->partition] = p->offset;
        }
        rd_kafka_topic_partition_list_destroy(rktparlistp);

        return offsets;
}

/**
 * Run consumer, consuming messages from Kafka and writing to 'fp'.
 */
static void consumer_run (FILE *fp) {
        char    errstr[512];
        rd_kafka_resp_err_t err;
        const rd_kafka_metadata_t *metadata;
        int i;
        int64_t *offsets = NULL;
        rd_kafka_queue_t *rkqu;

        /* Create consumer */
        if (!(conf.rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf.rk_conf,
                                     errstr, sizeof(errstr))))
                KC_FATAL("Failed to create consumer: %s", errstr);

        if (!conf.debug && conf.verbosity == 0)
                rd_kafka_set_log_level(conf.rk, 0);

        /* The callback-based consumer API's offset store granularity is
         * not good enough for us, disable automatic offset store
         * and do it explicitly per-message in the consume callback instead. */
        if (rd_kafka_topic_conf_set(conf.rkt_conf,
                                    "auto.commit.enable", "false",
                                    errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
                KC_FATAL("%s", errstr);

        /* Create topic */
        if (!(conf.rkt = rd_kafka_topic_new(conf.rk, conf.topic,
                                            conf.rkt_conf)))
                KC_FATAL("Failed to create topic %s: %s", conf.topic,
                         rd_kafka_err2str(rd_kafka_last_error()));

        conf.rk_conf  = NULL;
        conf.rkt_conf = NULL;


        /* Query broker for topic + partition information. */
        if ((err = rd_kafka_metadata(conf.rk, 0, conf.rkt, &metadata,
                                     conf.metadata_timeout * 1000)))
                KC_FATAL("Failed to query metadata for topic %s: %s",
                         rd_kafka_topic_name(conf.rkt), rd_kafka_err2str(err));

        /* Error handling */
        if (metadata->topic_cnt == 0)
                KC_FATAL("No such topic in cluster: %s",
                         rd_kafka_topic_name(conf.rkt));

        if ((err = metadata->topics[0].err))
                KC_FATAL("Topic %s error: %s",
                         rd_kafka_topic_name(conf.rkt), rd_kafka_err2str(err));

        if (metadata->topics[0].partition_cnt == 0)
                KC_FATAL("Topic %s has no partitions",
                         rd_kafka_topic_name(conf.rkt));

        /* If Exit-at-EOF is enabled, set up array to track EOF
         * state for each partition. */
        if (conf.exit_eof || conf.stopts) {
                part_stop = calloc(sizeof(*part_stop),
                                   metadata->topics[0].partition_cnt);

                if (conf.partition != RD_KAFKA_PARTITION_UA)
                        part_stop_thres = 1;
                else
                        part_stop_thres = metadata->topics[0].partition_cnt;
        }

#if RD_KAFKA_VERSION >= 0x00090300
        if (conf.startts) {
                offsets = get_offsets(&metadata->topics[0]);
        }
#endif

        /* Create a shared queue that combines messages from
         * all wanted partitions. */
        rkqu = rd_kafka_queue_new(conf.rk);

        /* Start consuming from all wanted partitions. */
        for (i = 0 ; i < metadata->topics[0].partition_cnt ; i++) {
                int32_t partition = metadata->topics[0].partitions[i].id;

                /* If -p <part> was specified: skip unwanted partitions */
                if (conf.partition != RD_KAFKA_PARTITION_UA &&
                    conf.partition != partition)
                        continue;

                /* Start consumer for this partition */
                if (rd_kafka_consume_start_queue(conf.rkt, partition,
                                                 offsets ? offsets[i] :
                                                 (conf.offset ==
                                                  RD_KAFKA_OFFSET_INVALID ?
                                                  RD_KAFKA_OFFSET_BEGINNING :
                                                  conf.offset),
                                                 rkqu) == -1)
                        KC_FATAL("Failed to start consuming "
                                 "topic %s [%"PRId32"]: %s",
                                 conf.topic, partition,
                                 rd_kafka_err2str(rd_kafka_last_error()));

                if (conf.partition != RD_KAFKA_PARTITION_UA)
                        break;
        }
        free(offsets);

        if (conf.partition != RD_KAFKA_PARTITION_UA &&
            i == metadata->topics[0].partition_cnt)
                KC_FATAL("Topic %s (with partitions 0..%i): "
                         "partition %i does not exist",
                         rd_kafka_topic_name(conf.rkt),
                         metadata->topics[0].partition_cnt-1,
                         conf.partition);


        /* Read messages from Kafka, write to 'fp'. */
        while (conf.run) {
                rd_kafka_consume_callback_queue(rkqu, 100,
                                                consume_cb, fp);

                /* Poll for errors, etc */
                rd_kafka_poll(conf.rk, 0);
        }

        /* Stop consuming */
        for (i = 0 ; i < metadata->topics[0].partition_cnt ; i++) {
                int32_t partition = metadata->topics[0].partitions[i].id;

                /* If -p <part> was specified: skip unwanted partitions */
                if (conf.partition != RD_KAFKA_PARTITION_UA &&
                    conf.partition != partition)
                        continue;

                /* Dont stop already stopped partitions */
                if (!part_stop || !part_stop[partition])
                        rd_kafka_consume_stop(conf.rkt, partition);

                rd_kafka_consume_stop(conf.rkt, partition);
        }

        /* Destroy shared queue */
        rd_kafka_queue_destroy(rkqu);

        /* Wait for outstanding requests to finish. */
        conf.run = 1;
        while (conf.run && rd_kafka_outq_len(conf.rk) > 0)
                rd_kafka_poll(conf.rk, 50);

        if (conf.assignment)
                rd_kafka_topic_partition_list_destroy(conf.assignment);

        rd_kafka_metadata_destroy(metadata);
        rd_kafka_topic_destroy(conf.rkt);
        rd_kafka_destroy(conf.rk);
}


/**
 * Print metadata information
 */
static void metadata_print (const rd_kafka_metadata_t *metadata,
                            int32_t controllerid) {
        int i, j, k;
        int total_partition_cnt = 0;

        printf("Metadata for %s (from broker %"PRId32": %s):\n",
               conf.topic ? conf.topic : "all topics",
               metadata->orig_broker_id, metadata->orig_broker_name);

        /* Iterate brokers */
        printf(" %i brokers:\n", metadata->broker_cnt);
        for (i = 0 ; i < metadata->broker_cnt ; i++)
                printf("  broker %"PRId32" at %s:%i%s\n",
                       metadata->brokers[i].id,
                       metadata->brokers[i].host,
                       metadata->brokers[i].port,
                       controllerid == metadata->brokers[i].id ?
                       " (controller)" : "");

        /* Iterate topics */
        printf(" %i topics:\n", metadata->topic_cnt);
        for (i = 0 ; i < metadata->topic_cnt ; i++) {
                const rd_kafka_metadata_topic_t *t = &metadata->topics[i];
                printf("  topic \"%s\" with %i partitions:",
                       t->topic,
                       t->partition_cnt);
                if (t->err) {
                        printf(" %s", rd_kafka_err2str(t->err));
                        if (t->err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
                                printf(" (try again)");
                }
                printf("\n");

                /* Iterate topic's partitions */
                for (j = 0 ; j < t->partition_cnt ; j++) {
                        const rd_kafka_metadata_partition_t *p;
                        p = &t->partitions[j];
                        total_partition_cnt += p->replica_cnt;
                        printf("    partition %"PRId32", "
                               "leader %"PRId32", replicas: ",
                               p->id, p->leader);

                        /* Iterate partition's replicas */
                        for (k = 0 ; k < p->replica_cnt ; k++)
                                printf("%s%"PRId32,
                                       k > 0 ? ",":"", p->replicas[k]);

                        /* Iterate partition's ISRs */
                        printf(", isrs: ");
                        for (k = 0 ; k < p->isr_cnt ; k++)
                                printf("%s%"PRId32,
                                       k > 0 ? ",":"", p->isrs[k]);
                        if (p->err)
                                printf(", %s\n", rd_kafka_err2str(p->err));
                        else
                                printf("\n");
                }
        }
        printf(" %i partitions, including replicas, in the cluster\n %i partitions, including replicas, for each broker\n",
               total_partition_cnt,
               total_partition_cnt / metadata->broker_cnt);
}


/**
 * Lists metadata
 */
static void metadata_list (void) {
        char    errstr[512];
        rd_kafka_resp_err_t err;
        const rd_kafka_metadata_t *metadata;
        int32_t controllerid = -1;

        /* Create handle */
        if (!(conf.rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf.rk_conf,
                                     errstr, sizeof(errstr))))
                KC_FATAL("Failed to create producer: %s", errstr);

        if (!conf.debug && conf.verbosity == 0)
                rd_kafka_set_log_level(conf.rk, 0);

        /* Create topic, if specified */
        if (conf.topic &&
            !(conf.rkt = rd_kafka_topic_new(conf.rk, conf.topic,
                                            conf.rkt_conf)))
                KC_FATAL("Failed to create topic %s: %s", conf.topic,
                         rd_kafka_err2str(rd_kafka_last_error()));

        conf.rk_conf  = NULL;
        conf.rkt_conf = NULL;


        /* Fetch metadata */
        err = rd_kafka_metadata(conf.rk, conf.rkt ? 0 : 1, conf.rkt,
                                &metadata, conf.metadata_timeout * 1000);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                KC_FATAL("Failed to acquire metadata: %s%s",
                         rd_kafka_err2str(err),
                         err == RD_KAFKA_RESP_ERR__TRANSPORT ?
                         " (Are the brokers reachable? "
                         "Also try increasing the metadata timeout with "
                         "-m <timeout>?)" : "");

#if HAVE_CONTROLLERID
        controllerid = rd_kafka_controllerid(conf.rk, 0);
#endif

        /* Print metadata */
#if ENABLE_JSON
        if (conf.flags & CONF_F_FMT_JSON)
                metadata_print_json(metadata, controllerid);
        else
#endif
                metadata_print(metadata, controllerid);

        rd_kafka_metadata_destroy(metadata);

        if (conf.rkt)
                rd_kafka_topic_destroy(conf.rkt);
        rd_kafka_destroy(conf.rk);
}


/**
 * Print usage and exit.
 */
static void RD_NORETURN usage (const char *argv0, int exitcode,
                               const char *reason,
                               int version_only) {

        FILE *out = stdout;
        char features[256];
        size_t flen;
        rd_kafka_conf_t *tmpconf;

        if (reason) {
                out = stderr;
                fprintf(out, "Error: %s\n\n", reason);
        }

        if (!version_only)
                fprintf(out, "Usage: %s <options> [file1 file2 .. | topic1 topic2 ..]]\n",
                        argv0);

        /* Create a temporary config object to extract builtin.features */
        tmpconf = rd_kafka_conf_new();
        flen = sizeof(features);
        if (rd_kafka_conf_get(tmpconf, "builtin.features",
                              features, &flen) != RD_KAFKA_CONF_OK)
                strncpy(features, "n/a", sizeof(features));
        rd_kafka_conf_destroy(tmpconf);

        fprintf(out,
                "kafkacat - Apache Kafka producer and consumer tool\n"
                "https://github.com/edenhill/kafkacat\n"
                "Copyright (c) 2014-2021, Magnus Edenhill\n"
                "Version %s (%slibrdkafka %s builtin.features=%s)\n"
                "\n",
                KAFKACAT_VERSION,
                ""
#if ENABLE_JSON
                "JSON, "
#endif
#if ENABLE_AVRO
                "Avro, "
#endif
#if ENABLE_TXNS
                "Transactions, "
#endif
#if ENABLE_INCREMENTAL_ASSIGN
                "IncrementalAssign, "
#endif
                ,
                rd_kafka_version_str(), features
                );

        if (version_only)
                exit(exitcode);

        fprintf(out, "\n"
                "General options:\n"
                "  -C | -P | -L | -Q  Mode: Consume, Produce, Metadata List, Query mode\n"
#if ENABLE_KAFKACONSUMER
                "  -G <group-id>      Mode: High-level KafkaConsumer (Kafka >=0.9 balanced consumer groups)\n"
                "                     Expects a list of topics to subscribe to\n"
#endif
                "  -t <topic>         Topic to consume from, produce to, "
                "or list\n"
                "  -p <partition>     Partition\n"
                "  -b <brokers,..>    Bootstrap broker(s) (host[:port])\n"
                "  -D <delim>         Message delimiter string:\n"
                "                     a-z | \\r | \\n | \\t | \\xNN ..\n"
                "                     Default: \\n\n"
                "  -K <delim>         Key delimiter (same format as -D)\n"
                "  -c <cnt>           Limit message count\n"
                "  -m <seconds>       Metadata (et.al.) request timeout.\n"
                "                     This limits how long kafkacat will block\n"
                "                     while waiting for initial metadata to be\n"
                "                     retrieved from the Kafka cluster.\n"
                "                     It also sets the timeout for the producer's\n"
                "                     transaction commits, init, aborts, etc.\n"
                "                     Default: 5 seconds.\n"
                "  -F <config-file>   Read configuration properties from file,\n"
                "                     file format is \"property=value\".\n"
                "                     The KAFKACAT_CONFIG=path environment can "
                "also be used, but -F takes precedence.\n"
                "                     The default configuration file is "
                "$HOME/.config/kafkacat.conf\n"
                "  -X list            List available librdkafka configuration "
                "properties\n"
                "  -X prop=val        Set librdkafka configuration property.\n"
                "                     Properties prefixed with \"topic.\" are\n"
                "                     applied as topic properties.\n"
#if ENABLE_AVRO
                "  -X schema.registry.prop=val Set libserdes configuration property "
                "for the Avro/Schema-Registry client.\n"
#endif
                "  -X dump            Dump configuration and exit.\n"
                "  -d <dbg1,...>      Enable librdkafka debugging:\n"
                "                     " RD_KAFKA_DEBUG_CONTEXTS "\n"
                "  -q                 Be quiet (verbosity set to 0)\n"
                "  -v                 Increase verbosity\n"
                "  -E                 Do not exit on non-fatal error\n"
                "  -V                 Print version\n"
                "  -h                 Print usage help\n"
                "\n"
                "Producer options:\n"
                "  -z snappy|gzip|lz4 Message compression. Default: none\n"
                "  -p -1              Use random partitioner\n"
                "  -D <delim>         Delimiter to split input into messages\n"
                "  -K <delim>         Delimiter to split input key and message\n"
                "  -k <str>           Use a fixed key for all messages.\n"
                "                     If combined with -K, per-message keys\n"
                "                     takes precendence.\n"
                "  -H <header=value>  Add Message Headers "
                "(may be specified multiple times)\n"
                "  -l                 Send messages from a file separated by\n"
                "                     delimiter, as with stdin.\n"
                "                     (only one file allowed)\n"
                "  -T                 Output sent messages to stdout, acting like tee.\n"
                "  -c <cnt>           Exit after producing this number "
                "of messages\n"
                "  -Z                 Send empty messages as NULL messages\n"
                "  file1 file2..      Read messages from files.\n"
                "                     With -l, only one file permitted.\n"
                "                     Otherwise, the entire file contents will\n"
                "                     be sent as one single message.\n"
                "  -X transactional.id=.. Enable transactions and send all\n"
                "                     messages in a single transaction which\n"
                "                     is committed when stdin is closed or the\n"
                "                     input file(s) are fully read.\n"
                "                     If kafkacat is terminated through Ctrl-C\n"
                "                     (et.al) the transaction will be aborted.\n"
                "\n"
                "Consumer options:\n"
                "  -o <offset>        Offset to start consuming from:\n"
                "                     beginning | end | stored |\n"
                "                     <value>  (absolute offset) |\n"
                "                     -<value> (relative offset from end)\n"
#if RD_KAFKA_VERSION >= 0x00090300
                "                     s@<value> (timestamp in ms to start at)\n"
                "                     e@<value> (timestamp in ms to stop at "
                "(not included))\n"
#endif
                "  -e                 Exit successfully when last message "
                "received\n"
                "  -f <fmt..>         Output formatting string, see below.\n"
                "                     Takes precedence over -D and -K.\n"
#if ENABLE_JSON
                "  -J                 Output with JSON envelope\n"
#endif
                "  -s key=<serdes>    Deserialize non-NULL keys using <serdes>.\n"
                "  -s value=<serdes>  Deserialize non-NULL values using <serdes>.\n"
                "  -s <serdes>        Deserialize non-NULL keys and values using <serdes>.\n"
                "                     Available deserializers (<serdes>):\n"
                "                       <pack-str> - A combination of:\n"
                "                                    <: little-endian,\n"
                "                                    >: big-endian (recommended),\n"
                "                                    b: signed 8-bit integer\n"
                "                                    B: unsigned 8-bit integer\n"
                "                                    h: signed 16-bit integer\n"
                "                                    H: unsigned 16-bit integer\n"
                "                                    i: signed 32-bit integer\n"
                "                                    I: unsigned 32-bit integer\n"
                "                                    q: signed 64-bit integer\n"
                "                                    Q: unsigned 64-bit integer\n"
                "                                    c: ASCII character\n"
                "                                    s: remaining data is string\n"
                "                                    $: match end-of-input (no more bytes remaining or a parse error is raised).\n"
                "                                       Not including this token skips any\n"
                "                                       remaining data after the pack-str is\n"
                "                                       exhausted.\n"
#if ENABLE_AVRO
                "                       avro       - Avro-formatted with schema in Schema-Registry (requires -r)\n"
                "                     E.g.: -s key=i -s value=avro - key is 32-bit integer, value is Avro.\n"
                "                       or: -s avro - both key and value are Avro-serialized\n"
#endif
#if ENABLE_AVRO
                "  -r <url>           Schema registry URL (requires avro deserializer to be used with -s)\n"
#endif
                "  -D <delim>         Delimiter to separate messages on output\n"
                "  -K <delim>         Print message keys prefixing the message\n"
                "                     with specified delimiter.\n"
                "  -O                 Print message offset using -K delimiter\n"
                "  -c <cnt>           Exit after consuming this number "
                "of messages\n"
                "  -Z                 Print NULL values and keys as \"%s\" "
                "instead of empty.\n"
                "                     For JSON (-J) the nullstr is always null.\n"
                "  -u                 Unbuffered output\n"
                "\n"
                "Metadata options (-L):\n"
                "  -t <topic>         Topic to query (optional)\n"
                "\n"
                "Query options (-Q):\n"
                "  -t <t>:<p>:<ts>    Get offset for topic <t>,\n"
                "                     partition <p>, timestamp <ts>.\n"
                "                     Timestamp is the number of milliseconds\n"
                "                     since epoch UTC.\n"
                "                     Requires broker >= 0.10.0.0 and librdkafka >= 0.9.3.\n"
                "                     Multiple -t .. are allowed but a partition\n"
                "                     must only occur once.\n"
                "\n"
                "Format string tokens:\n"
                "  %%s                 Message payload\n"
                "  %%S                 Message payload length (or -1 for NULL)\n"
                "  %%R                 Message payload length (or -1 for NULL) serialized\n"
                "                     as a binary big endian 32-bit signed integer\n"
                "  %%k                 Message key\n"
                "  %%K                 Message key length (or -1 for NULL)\n"
#if RD_KAFKA_VERSION >= 0x000902ff
                "  %%T                 Message timestamp (milliseconds since epoch UTC)\n"
#endif
#if HAVE_HEADERS
                "  %%h                 Message headers (n=v CSV)\n"
#endif
                "  %%t                 Topic\n"
                "  %%p                 Partition\n"
                "  %%o                 Message offset\n"
                "  \\n \\r \\t           Newlines, tab\n"
                "  \\xXX \\xNNN         Any ASCII character\n"
                " Example:\n"
                "  -f 'Topic %%t [%%p] at offset %%o: key %%k: %%s\\n'\n"
                "\n"
#if ENABLE_JSON
                "JSON message envelope (on one line) when consuming with -J:\n"
                " { \"topic\": str, \"partition\": int, \"offset\": int,\n"
                "   \"tstype\": \"create|logappend|unknown\", \"ts\": int, "
                "// timestamp in milliseconds since epoch\n"
                "   \"broker\": int,\n"
                "   \"headers\": { \"<name>\": str, .. }, // optional\n"
                "   \"key\": str|json, \"payload\": str|json,\n"
                "   \"key_error\": str, \"payload_error\": str, //optional\n"
                "   \"key_schema_id\": int, "
                "\"value_schema_id\": int //optional\n"
                " }\n"
                " notes:\n"
                "   - key_error and payload_error are only included if "
                "deserialization fails.\n"
                "   - key_schema_id and value_schema_id are included for "
                "successfully deserialized Avro messages.\n"
                "\n"
#endif
                "Consumer mode (writes messages to stdout):\n"
                "  kafkacat -b <broker> -t <topic> -p <partition>\n"
                " or:\n"
                "  kafkacat -C -b ...\n"
                "\n"
#if ENABLE_KAFKACONSUMER
                "High-level KafkaConsumer mode:\n"
                "  kafkacat -b <broker> -G <group-id> topic1 top2 ^aregex\\d+\n"
                "\n"
#endif
                "Producer mode (reads messages from stdin):\n"
                "  ... | kafkacat -b <broker> -t <topic> -p <partition>\n"
                " or:\n"
                "  kafkacat -P -b ...\n"
                "\n"
                "Metadata listing:\n"
                "  kafkacat -L -b <broker> [-t <topic>]\n"
                "\n"
                "Query offset by timestamp:\n"
                "  kafkacat -Q -b broker -t <topic>:<partition>:<timestamp>\n"
                "\n",
                conf.null_str
                );
        exit(exitcode);
}


/**
 * Terminate by putting out the run flag.
 */
static void term (int sig) {
        conf.run = 0;
        conf.term_sig = sig;
}


/**
 * librdkafka error callback
 */
static void error_cb (rd_kafka_t *rk, int err,
                      const char *reason, void *opaque) {
#if RD_KAFKA_VERSION >= 0x01000000
        if (err == RD_KAFKA_RESP_ERR__FATAL) {
                /* A fatal error has been raised, extract the
                 * underlying error, and start graceful termination -
                 * this to make sure producer delivery reports are
                 * handled before exiting. */
                char fatal_errstr[512];
                rd_kafka_resp_err_t fatal_err;

                fatal_err = rd_kafka_fatal_error(rk, fatal_errstr,
                                                 sizeof(fatal_errstr));
                KC_INFO(0, "FATAL CLIENT ERROR: %s: %s: terminating\n",
                        rd_kafka_err2str(fatal_err), fatal_errstr);
                conf.run = 0;

        } else
#endif
                if (err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
                        KC_ERROR("%s: %s", rd_kafka_err2str(err),
                                 reason ? reason : "");
                } else {
                        KC_INFO(1, "ERROR: %s: %s\n", rd_kafka_err2str(err),
                                reason ? reason : "");
                }
}


/**
 * @brief Parse delimiter string from command line arguments and return
 *        an allocated copy.
 */
static char *parse_delim (const char *instr) {
        char *str;

        /* Make a copy so we can modify the string. */
        str = strdup(instr);

        while (1) {
                size_t skip = 0;
                char *t;

                if ((t = strstr(str, "\\n"))) {
                        *t = '\n';
                        skip = 1;
                } else if ((t = strstr(str, "\\t"))) {
                        *t = '\t';
                        skip = 1;
                } else if ((t = strstr(str, "\\x"))) {
                        char *end;
                        int x;
                        x = strtoul(t+strlen("\\x"), &end, 16) & 0xff;
                        if (end == t+strlen("\\x"))
                                KC_FATAL("Delimiter %s expects hex number", t);
                        *t = (char)x;
                        skip = (int)(end - t) - 1;
                } else
                        break;

                if (t && skip)
                        memmove(t+1, t+1+skip, strlen(t+1+skip)+1);
        }

        return str;
}

/**
 * @brief Add topic+partition+offset to list, from :-separated string.
 *
 * "<t>:<p>:<o>"
 *
 * @remark Will modify \p str
 */
static void add_topparoff (const char *what,
                           rd_kafka_topic_partition_list_t *rktparlist,
                           char *str) {
        char *s, *t, *e;
        char *topic;
        int partition;
        int64_t offset;

        if (!(s = strchr(str, ':')) ||
            !(t = strchr(s+1, ':')))
                KC_FATAL("%s: expected \"topic:partition:offset_or_timestamp\"", what);

        topic = str;
        *s = '\0';

        partition = strtoul(s+1, &e, 0);
        if (e == s+1)
                KC_FATAL("%s: expected \"topic:partition:offset_or_timestamp\"", what);

        offset = strtoll(t+1, &e, 0);
        if (e == t+1)
                KC_FATAL("%s: expected \"topic:partition:offset_or_timestamp\"", what);

        rd_kafka_topic_partition_list_add(rktparlist, topic, partition)->offset = offset;
}

/**
 * Dump current rdkafka configuration to stdout.
 */
static void conf_dump (void) {
        const char **arr;
        size_t cnt;
        int pass;

        for (pass = 0 ; pass < 2 ; pass++) {
                int i;

                if (pass == 0) {
                        arr = rd_kafka_conf_dump(conf.rk_conf, &cnt);
                        printf("# Global config\n");
                } else {
                        printf("# Topic config\n");
                        arr = rd_kafka_topic_conf_dump(conf.rkt_conf, &cnt);
                }

                for (i = 0 ; i < (int)cnt ; i += 2)
                        printf("%s = %s\n",
                               arr[i], arr[i+1]);

                printf("\n");

                rd_kafka_conf_dump_free(arr, cnt);
        }
}


/**
 * @brief Try setting a config property. Provides "topic." fallthru.
 *
 * @returns -1 on failure or 0 on success.
 */
static int try_conf_set (const char *name, const char *val,
                         char *errstr, size_t errstr_size) {
        rd_kafka_conf_res_t res = RD_KAFKA_CONF_UNKNOWN;
        size_t srlen = strlen("schema.registry.");

        /* Pass schema.registry. config to the serdes */
        if (!strncmp(name, "schema.registry.", srlen)) {
#if ENABLE_AVRO
                serdes_err_t serr;

                if (!conf.srconf)
                        conf.srconf = serdes_conf_new(NULL, 0, NULL);

                if (!strcmp(name, "schema.registry.url")) {
                        conf.flags |= CONF_F_SR_URL_SEEN;
                        srlen = 0;
                }

                serr = serdes_conf_set(conf.srconf, name+srlen, val,
                                       errstr, errstr_size);
                return serr == SERDES_ERR_OK ? 0 : -1;
#else
                snprintf(errstr, errstr_size,
                         "This build of kafkacat lacks "
                         "Avro/Schema-Registry support");
                return -1;
#endif
        }


        /* Try "topic." prefixed properties on topic
         * conf first, and then fall through to global if
         * it didnt match a topic configuration property. */
        if (!strncmp(name, "topic.", strlen("topic.")))
                res = rd_kafka_topic_conf_set(conf.rkt_conf,
                                              name+
                                              strlen("topic."),
                                              val,
                                              errstr, errstr_size);
        else
                /* If no "topic." prefix, try the topic config first. */
                res = rd_kafka_topic_conf_set(conf.rkt_conf,
                                              name, val,
                                              errstr, errstr_size);

        if (res == RD_KAFKA_CONF_UNKNOWN)
                res = rd_kafka_conf_set(conf.rk_conf, name, val,
                                        errstr, errstr_size);

        if (res != RD_KAFKA_CONF_OK)
                return -1;

        if (!strcmp(name, "metadata.broker.list") ||
            !strcmp(name, "bootstrap.servers"))
                conf.flags |= CONF_F_BROKERS_SEEN;

        if (!strcmp(name, "api.version.request"))
                conf.flags |= CONF_F_APIVERREQ_USER;

#if RD_KAFKA_VERSION >= 0x00090000
        rd_kafka_conf_set_throttle_cb(conf.rk_conf, throttle_cb);
#endif


        return 0;
}

/**
 * @brief Intercept configuration properties and try to identify
 *        incompatible properties that needs to be converted to librdkafka
 *        configuration properties.
 *
 * @returns -1 on failure, 0 if the property was not handled,
 *          or 1 if it was handled.
 */
static int try_java_conf_set (const char *name, const char *val,
                              char *errstr, size_t errstr_size) {
        if (!strcmp(name, "ssl.endpoint.identification.algorithm"))
                return 1; /* SSL server verification:
                           * not supported by librdkafka: ignore for now */

        if (!strcmp(name, "sasl.jaas.config")) {
                char sasl_user[128], sasl_pass[128];
                if (sscanf(val,
                           "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%[^\"]\" password=\"%[^\"]\"",
                           sasl_user, sasl_pass) == 2) {
                        if (try_conf_set("sasl.username", sasl_user,
                                         errstr, errstr_size) == -1 ||
                            try_conf_set("sasl.password", sasl_pass,
                                         errstr, errstr_size) == -1)
                                return -1;
                        return 1;
                }
        }

        return 0;
}


/**
 * @brief Read config file, fail terminally if fatal is true, else
 *        fail silently.
 *
 * @returns 0 on success or -1 on failure (unless fatal is true
 *          in which case the app will have exited).
 */
static int read_conf_file (const char *path, int fatal) {
        FILE *fp;
        char buf[512];
        int line = 0;

        if (!(fp = fopen(path, "r"))) {
                if (fatal)
                        KC_FATAL("Failed to open %s: %s",
                                 path, strerror(errno));
                return -1;
        }

        KC_INFO(fatal ? 1 : 3, "Reading configuration from file %s\n", path);

        while (fgets(buf, sizeof(buf), fp)) {
                char *s = buf;
                char *t;
                char errstr[512];
                int r;

                line++;

                /* Left-trim */
                while (isspace(*s))
                        s++;

                /* Right-trim and remove newline */
                t = s + strlen(s) - 1;
                while (t > s && isspace(*t)) {
                        *t = 0;
                        t--;
                }

                /* Ignore Empty line */
                if (!*s)
                        continue;

                /* Ignore comments */
                if (*s == '#')
                        continue;

                /* Strip escapes for \: \= which can be encountered in
                 * Java configs (see comment below) */
                while ((t = strstr(s, "\\:"))) {
                        memmove(t, t+1, strlen(t+1)+1); /* overwrite \: */
                        *t = ':'; /* reinsert : */
                }
                while ((t = strstr(s, "\\="))) {
                        memmove(t, t+1, strlen(t+1)+1); /* overwrite \= */
                        *t = '='; /* reinsert : */
                }

                /* Parse prop=value */
                if (!(t = strchr(s, '=')) || t == s)
                        KC_FATAL("%s:%d: expected property=value\n",
                                 path, line);

                *t = 0;
                t++;

                /**
                 * Attempt to support Java client configuration files,
                 * such as the ccloud config.
                 * There are some quirks with unnecessary escaping with \
                 * that we remove, as well as parsing special configuration
                 * properties that don't match librdkafka's.
                 */
                r = try_java_conf_set(s, t, errstr, sizeof(errstr));
                if (r == -1)
                        KC_FATAL("%s:%d: %s (java config conversion)\n",
                                 path, line, errstr);
                else if (r == 1)
                        continue; /* Handled */

                if (try_conf_set(s, t, errstr, sizeof(errstr)) == -1)
                        KC_FATAL("%s:%d: %s\n", path, line, errstr);
        }

        fclose(fp);

        return 0;
}


/**
 * @returns the value for environment variable \p env, or NULL
 *          if it is not set or it is empty.
 */
static const char *kc_getenv (const char *env) {
        const char *val;
        if (!(val = getenv(env)) || !*val)
                return NULL;
        return val;
}

static void read_default_conf_files (void) {
        char path[512];
        const char *home;

        if (!(home = kc_getenv("HOME")))
                return;

        snprintf(path, sizeof(path), "%s/.config/kafkacat.conf", home);

        read_conf_file(path, 0/*not fatal*/);
}



static int unittest_strnstr (void) {
        struct {
                const char *sep;
                const char *hay;
                int offset;
        } exp[] = {
                { ";Sep;", ";Sep;Post", 0 },
                { ";Sep;", ";Sep;", 0 },
                { ";Sep;", "Pre;Sep;Post", 3 },
                { ";Sep;", "Pre;Sep;", 3 },
                { ";Sep;", "Pre;SepPost", -1 },
                { ";KeyDel;", "Key1;KeyDel;Value1", 4 },
                { ";", "Is The;", 6 },
                { NULL },
        };
        int i;
        int fails = 0;

        for (i = 0 ; exp[i].sep ; i++) {
                const char *t = rd_strnstr(exp[i].hay, strlen(exp[i].hay),
                                           exp[i].sep, strlen(exp[i].sep));
                const char *e = exp[i].hay + exp[i].offset;
                const char *fail = NULL;

                if (exp[i].offset == -1) {
                        if (t)
                                fail = "expected no match";
                } else if (!t) {
                        fail = "expected match";
                } else if (t != e)
                        fail = "invalid match";

                if (!fail)
                        continue;

                fprintf(stderr,
                        "%s: FAILED: for hay %d: "
                        "match is %p '%s' for %p '%s' in %p '%s' "
                        "(want offset %d, not %d): %s\n",
                        __FUNCTION__,
                        i,
                        t, t,
                        exp[i].sep, exp[i].sep,
                        exp[i].hay, exp[i].hay,
                        exp[i].offset,
                        t ? (int)(t - exp[i].hay) : -1,
                        fail);
                fails++;
        }

        return fails;
}

static int unittest_parse_delim (void) {
        struct {
                const char *in;
                const char *exp;
        } delims[] = {
                { "", "" },
                { "\\n", "\n" },
                { "\\t\\n\\n", "\t\n\n" },
                { "\\x54!\\x45\\x53T", "T!EST" },
                { "\\x30", "0" },
                { NULL }
        };
        int i;
        int fails = 0;

        for (i = 0 ; delims[i].in ; i++) {
                char *out = parse_delim(delims[i].in);
                if (strcmp(out, delims[i].exp))
                        fprintf(stderr, "%s: FAILED: "
                                "expected '%s' to return '%s', not '%s'\n",
                                __FUNCTION__, delims[i].in, delims[i].exp, out);
                free(out);
        }

        return fails;
}


/**
 * @brief Run unittests
 *
 * @returns the number of failed tests.
 */
static int unittest (void) {
        int r = 0;

        r += unittest_strnstr();
        r += unittest_parse_delim();

        return r;
}

/**
 * @brief Add a single header specified as a command line option.
 *
 * @param inp "name=value" or "name" formatted header
 */
static void add_header (const char *inp) {
        const char *t;
        rd_kafka_resp_err_t err;

        t = strchr(inp, '=');
        if (t == inp || !*inp)
                KC_FATAL("Expected -H \"name=value..\" or -H \"name\"");

        if (!conf.headers)
                conf.headers = rd_kafka_headers_new(8);


        err = rd_kafka_header_add(conf.headers,
                                  inp,
                                  t ? (ssize_t)(t-inp) : -1,
                                  t ? t+1 : NULL, -1);
        if (err)
                KC_FATAL("Failed to add header \"%s\": %s",
                         inp, rd_kafka_err2str(err));
}


/**
 * Parse command line arguments
 */
static void argparse (int argc, char **argv,
                      rd_kafka_topic_partition_list_t **rktparlistp) {
        char errstr[512];
        int opt;
        const char *fmt = NULL;
        const char *delim = "\n";
        const char *key_delim = NULL;
        char tmp_fmt[64];
        int do_conf_dump = 0;
        int conf_files_read = 0;
        int i;

        while ((opt = getopt(argc, argv,
                             ":PCG:LQt:p:b:z:o:eED:K:k:H:Od:qvF:X:c:Tuf:ZlVh"
                             "s:r:Jm:U")) != -1) {
                switch (opt) {
                case 'P':
                case 'C':
                case 'L':
                case 'Q':
                        conf.mode = opt;
                        break;
#if ENABLE_KAFKACONSUMER
                case 'G':
                        conf.mode = opt;
                        conf.group = optarg;
                        if (rd_kafka_conf_set(conf.rk_conf, "group.id", optarg,
                                              errstr, sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK)
                                KC_FATAL("%s", errstr);
                        break;
#endif
                case 't':
                        if (conf.mode == 'Q') {
                                if (!*rktparlistp)
                                        *rktparlistp = rd_kafka_topic_partition_list_new(1);
                                add_topparoff("-t", *rktparlistp, optarg);
                                conf.flags |= CONF_F_APIVERREQ;
                        } else
                                conf.topic = optarg;

                        break;
                case 'p':
                        conf.partition = atoi(optarg);
                        break;
                case 'b':
                        conf.brokers = optarg;
                        conf.flags |= CONF_F_BROKERS_SEEN;
                        break;
                case 'z':
                        if (rd_kafka_conf_set(conf.rk_conf,
                                              "compression.codec", optarg,
                                              errstr, sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK)
                                KC_FATAL("%s", errstr);
                        break;
                case 'o':
                        if (!strcmp(optarg, "end"))
                                conf.offset = RD_KAFKA_OFFSET_END;
                        else if (!strcmp(optarg, "beginning"))
                                conf.offset = RD_KAFKA_OFFSET_BEGINNING;
                        else if (!strcmp(optarg, "stored"))
                                conf.offset = RD_KAFKA_OFFSET_STORED;
#if RD_KAFKA_VERSION >= 0x00090300
                        else if (!strncmp(optarg, "s@", 2)) {
                                conf.startts = strtoll(optarg+2, NULL, 10);
                                conf.flags |= CONF_F_APIVERREQ;
                        } else if (!strncmp(optarg, "e@", 2)) {
                                conf.stopts = strtoll(optarg+2, NULL, 10);
                                conf.flags |= CONF_F_APIVERREQ;
                        }
#endif
                        else {
                                conf.offset = strtoll(optarg, NULL, 10);
                                if (conf.offset < 0)
                                        conf.offset = RD_KAFKA_OFFSET_TAIL(-conf.offset);
                        }
                        break;
                case 'e':
                        conf.exit_eof = 1;
                        break;
                case 'E':
                        conf.exitonerror = 0;
                        break;
                case 'f':
                        fmt = optarg;
                        break;
                case 'J':
#if ENABLE_JSON
                        conf.flags |= CONF_F_FMT_JSON;
#else
                        KC_FATAL("This build of kafkacat lacks JSON support");
#endif
                        break;

                case 's':
                {
                        int field = -1;
                        const char *t = optarg;

                        if (!strncmp(t, "key=", strlen("key="))) {
                                t += strlen("key=");
                                field = KC_MSG_FIELD_KEY;
                        } else if (!strncmp(t, "value=", strlen("value="))) {
                                t += strlen("value=");
                                field = KC_MSG_FIELD_VALUE;
                        }

                        if (field == -1 || field == KC_MSG_FIELD_KEY) {
                                if (strcmp(t, "avro"))
                                        pack_check("key", t);
                                conf.pack[KC_MSG_FIELD_KEY] = t;
                        }

                        if (field == -1 || field == KC_MSG_FIELD_VALUE) {
                                if (strcmp(t, "avro"))
                                        pack_check("value", t);
                                conf.pack[KC_MSG_FIELD_VALUE] = t;
                        }
                }
                break;
                case 'r':
#if ENABLE_AVRO
                        if (!*optarg)
                                KC_FATAL("-s url must not be empty");
                        conf.schema_registry_url = optarg;
                        conf.flags |= CONF_F_SR_URL_SEEN;
#else
                        KC_FATAL("This build of kafkacat lacks "
                                 "Avro/Schema-Registry support");
#endif
                        break;
                case 'D':
                        delim = optarg;
                        break;
                case 'K':
                        key_delim = optarg;
                        conf.flags |= CONF_F_KEY_DELIM;
                        break;
                case 'k':
                        conf.fixed_key = optarg;
                        conf.fixed_key_len = (size_t)(strlen(conf.fixed_key));
                        break;
                case 'H':
                        add_header(optarg);
                        break;
                case 'l':
                        conf.flags |= CONF_F_LINE;
                        break;
                case 'O':
                        conf.flags |= CONF_F_OFFSET;
                        break;
                case 'c':
                        conf.msg_cnt = strtoll(optarg, NULL, 10);
                        break;
                case 'm':
                        conf.metadata_timeout = strtoll(optarg, NULL, 10);
                        break;
                case 'Z':
                        conf.flags |= CONF_F_NULL;
                        conf.null_str_len = strlen(conf.null_str);
                        break;
                case 'd':
                        conf.debug = optarg;
                        if (rd_kafka_conf_set(conf.rk_conf, "debug", conf.debug,
                                              errstr, sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK)
                                KC_FATAL("%s", errstr);
                        break;
                case 'q':
                        conf.verbosity = 0;
                        break;
                case 'v':
                        conf.verbosity++;
                        break;
                case 'T':
                        conf.flags |= CONF_F_TEE;
                        break;
                case 'u':
                        setbuf(stdout, NULL);
                        break;
                case 'F':
                        conf.flags |= CONF_F_NO_CONF_SEARCH;
                        if (!strcmp(optarg, "-") || !strcmp(optarg, "none"))
                                break;

                        read_conf_file(optarg, 1);
                        conf_files_read++;
                        break;
                case 'X':
                {
                        char *name, *val;

                        if (!strcmp(optarg, "list") ||
                            !strcmp(optarg, "help")) {
                                rd_kafka_conf_properties_show(stdout);
                                exit(0);
                        }

                        if (!strcmp(optarg, "dump")) {
                                do_conf_dump = 1;
                                continue;
                        }

                        name = optarg;
                        if (!(val = strchr(name, '='))) {
                                fprintf(stderr, "%% Expected "
                                        "-X property=value, not %s, "
                                        "use -X list to display available "
                                        "properties\n", name);
                                exit(1);
                        }

                        *val = '\0';
                        val++;

                        if (try_conf_set(name, val,
                                         errstr, sizeof(errstr)) == -1)
                                KC_FATAL("%s", errstr);
                }
                break;

                case 'U':
                        if (unittest())
                                exit(1);
                        else
                                exit(0);
                        break;

                case 'V':
                        usage(argv[0], 0, NULL, 1);
                        break;

                case 'h':
                        usage(argv[0], 0, NULL, 0);
                        break;

                default:
                        usage(argv[0], 1, "unknown argument", 0);
                        break;
                }
        }


        if (conf_files_read == 0) {
                const char *cpath = kc_getenv("KAFKACAT_CONFIG");
                if (cpath) {
                        conf.flags |= CONF_F_NO_CONF_SEARCH;
                        read_conf_file(cpath, 1/*fatal errors*/);
                }
        }

        if (!(conf.flags & CONF_F_NO_CONF_SEARCH))
                read_default_conf_files();

        /* Dump configuration and exit, if so desired. */
        if (do_conf_dump) {
                conf_dump();
                exit(0);
        }

        if (!(conf.flags & CONF_F_BROKERS_SEEN))
                usage(argv[0], 1, "-b <broker,..> missing", 0);

        /* Decide mode if not specified */
        if (!conf.mode) {
                if (_COMPAT(isatty)(STDIN_FILENO))
                        conf.mode = 'C';
                else
                        conf.mode = 'P';
                KC_INFO(1, "Auto-selecting %s mode (use -P or -C to override)\n",
                        conf.mode == 'C' ? "Consumer":"Producer");
        }


        if (!strchr("GLQ", conf.mode) && !conf.topic)
                usage(argv[0], 1, "-t <topic> missing", 0);
        else if (conf.mode == 'Q' && !*rktparlistp)
                usage(argv[0], 1,
                      "-t <topic>:<partition>:<offset_or_timestamp> missing",
                      0);

        if (conf.brokers &&
            rd_kafka_conf_set(conf.rk_conf, "metadata.broker.list",
                              conf.brokers, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                usage(argv[0], 1, errstr, 0);

        rd_kafka_conf_set_error_cb(conf.rk_conf, error_cb);

        fmt_init();

        /*
         * Verify serdes
         */
        for (i = 0 ; i < KC_MSG_FIELD_CNT ; i++) {
                if (!conf.pack[i])
                        continue;

                if (!strchr("GC", conf.mode))
                        KC_FATAL("-s serdes only available in the consumer");

                if (conf.pack[i] && !strcmp(conf.pack[i], "avro")) {
#if !ENABLE_AVRO
                        KC_FATAL("This build of kafkacat lacks "
                                 "Avro/Schema-Registry support");
#endif
#if ENABLE_JSON
                        /* Avro is decoded to JSON which needs to be
                         * written verbatim to the JSON envelope when
                         * using -J: libyajl does not support this,
                         * but my own fork of yajl does. */
                        if (conf.flags & CONF_F_FMT_JSON &&
                            !json_can_emit_verbatim())
                                KC_FATAL("This build of kafkacat lacks "
                                         "support for emitting "
                                         "JSON-formatted "
                                         "message keys and values: "
                                         "try without -J or build "
                                         "kafkacat with yajl from "
                                         "https://github.com/edenhill/yajl");
#endif

                        if (i == KC_MSG_FIELD_VALUE)
                                conf.flags |= CONF_F_FMT_AVRO_VALUE;
                        else if (i == KC_MSG_FIELD_KEY)
                                conf.flags |= CONF_F_FMT_AVRO_KEY;
                        continue;
                }
        }


        /*
         * Verify and initialize Avro/SR
         */
#if ENABLE_AVRO
        if (!!(conf.flags & CONF_F_SR_URL_SEEN) !=
            !!(conf.flags & (CONF_F_FMT_AVRO_VALUE|CONF_F_FMT_AVRO_KEY)))
                KC_FATAL("-r requires -s avro and vice-versa");

        if (conf.schema_registry_url) {
                char *t;

                if (!strchr("GC", conf.mode))
                        KC_FATAL("Schema-registry support is only available "
                                 "in the consumer");

                /* Trim trailing slashes from URL to avoid 404 */
                t = &conf.schema_registry_url[strlen(conf.
                                                     schema_registry_url)-1];
                while (t >= conf.schema_registry_url && *t == '/') {
                        *t = '\0';
                        t--;
                }

                if (try_conf_set("schema.registry.url",
                                 conf.schema_registry_url,
                                 errstr, sizeof(errstr)) == -1)
                        KC_FATAL("%s", errstr);


                /* Initialize Avro/Schema-Registry client */
                kc_avro_init(NULL, NULL, NULL, NULL);
        }
#endif


        /* If avro key is to be deserialized, set up an delimiter so that
         * the key is actually emitted. */
        if ((conf.flags & CONF_F_FMT_AVRO_KEY) && !key_delim)
                key_delim = "";

        if (key_delim) {
                conf.key_delim = parse_delim(key_delim);
                conf.key_delim_size = strlen(conf.key_delim);
        }

        conf.delim = parse_delim(delim);
        conf.delim_size = strlen(conf.delim);

        if (strchr("GC", conf.mode)) {
                /* Must be explicitly enabled for librdkafka >= v1.0.0 */
                rd_kafka_conf_set(conf.rk_conf, "enable.partition.eof", "true",
                                  NULL, 0);

                if (!fmt) {
                        if ((conf.flags & CONF_F_FMT_JSON)) {
                                /* For JSON the format string is simply the
                                 * output object delimiter (e.g., newline). */
                                fmt = conf.delim;
                        } else {
                                if (conf.key_delim)
                                        snprintf(tmp_fmt, sizeof(tmp_fmt),
                                                 "%%k%s%%s%s",
                                                 conf.key_delim, conf.delim);
                                else
                                        snprintf(tmp_fmt, sizeof(tmp_fmt),
                                                 "%%s%s", conf.delim);
                                fmt = tmp_fmt;
                        }
                }

                fmt_parse(fmt);

        } else if (conf.mode == 'P') {
                if (conf.delim_size == 0)
                        KC_FATAL("Message delimiter -D must not be empty "
                                 "when producing");
        }

        /* Automatically enable API version requests if needed and
         * user hasn't explicitly configured it (in any way). */
        if ((conf.flags & (CONF_F_APIVERREQ | CONF_F_APIVERREQ_USER)) ==
            CONF_F_APIVERREQ) {
                KC_INFO(2, "Automatically enabling api.version.request=true\n");
                rd_kafka_conf_set(conf.rk_conf, "api.version.request", "true",
                                  NULL, 0);
        }
}




int main (int argc, char **argv) {
#ifdef SIGIO
        char tmp[16];
#endif
        FILE *in = stdin;
        struct timeval tv;
        rd_kafka_topic_partition_list_t *rktparlist = NULL;

        /* Certain Docker images don't have kafkacat as the entry point,
         * requiring `kafkacat` to be the first argument. As these images
         * are fixed the examples get outdated and that first argument
         * will still be passed to the container and thus kafkacat,
         * so remove it here. */
        if (argc > 1 && !strcmp(argv[1], "kafkacat")) {
                if (argc > 2)
                        memmove(&argv[1], &argv[2], sizeof(*argv) * (argc - 2));
                argc--;
        }

        /* Seed rng for random partitioner, jitter, etc. */
        rd_gettimeofday(&tv, NULL);
        srand(tv.tv_usec);

        /* Create config containers */
        conf.rk_conf  = rd_kafka_conf_new();
        conf.rkt_conf = rd_kafka_topic_conf_new();

        /*
         * Default config
         */
#ifdef SIGIO
        /* Enable quick termination of librdkafka */
        snprintf(tmp, sizeof(tmp), "%i", SIGIO);
        rd_kafka_conf_set(conf.rk_conf, "internal.termination.signal",
                          tmp, NULL, 0);
#endif

        /* Log callback */
        rd_kafka_conf_set_log_cb(conf.rk_conf, rd_kafka_log_print);

        /* Parse command line arguments */
        argparse(argc, argv, &rktparlist);

        if (optind < argc) {
                if (!strchr("PG", conf.mode))
                        usage(argv[0], 1,
                              "file/topic list only allowed in "
                              "producer(-P)/kafkaconsumer(-G) mode", 0);
                else if ((conf.flags & CONF_F_LINE) && argc - optind > 1)
                        KC_FATAL("Only one file allowed for line mode (-l)");
                else if (conf.flags & CONF_F_LINE) {
                        in = fopen(argv[optind], "r");
                        if (in == NULL)
                                KC_FATAL("Cannot open %s: %s", argv[optind],
                                      strerror(errno));
                }
        }

        signal(SIGINT, term);
        signal(SIGTERM, term);
#ifdef SIGPIPE
        signal(SIGPIPE, term);
#endif

        /* Run according to mode */
        switch (conf.mode)
        {
        case 'C':
                consumer_run(stdout);
                break;

#if ENABLE_KAFKACONSUMER
        case 'G':
                if (conf.stopts || conf.startts)
                        KC_FATAL("-o ..@ timestamps can't be used "
                                 "with -G mode\n");
                kafkaconsumer_run(stdout, &argv[optind], argc-optind);
                break;
#endif

        case 'P':
                producer_run(in, &argv[optind], argc-optind);
                break;

        case 'L':
                metadata_list();
                break;

        case 'Q':
                if (!rktparlist)
                        usage(argv[0], 1,
                              "-Q requires one or more "
                              "-t <topic>:<partition>:<timestamp>", 0);

                query_offsets_by_time(rktparlist);

                rd_kafka_topic_partition_list_destroy(rktparlist);
                break;

        default:
                usage(argv[0], 0, NULL, 0);
                break;
        }

        if (conf.headers)
                rd_kafka_headers_destroy(conf.headers);

        if (conf.key_delim)
                free(conf.key_delim);
        if (conf.delim)
                free(conf.delim);

        if (in != stdin)
                fclose(in);

        rd_kafka_wait_destroyed(5000);

#if ENABLE_AVRO
        kc_avro_term();
#endif

        fmt_term();

        exit(conf.exitcode);
}
