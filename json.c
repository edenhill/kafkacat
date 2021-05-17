/*
 * kafkacat - Apache Kafka consumer and producer
 *
 * Copyright (c) 2015, Magnus Edenhill
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

#include "kafkacat.h"

#include <yajl/yajl_gen.h>

#define JS_STR(G, STR) do {                                             \
                const char *_s = (STR);                                 \
                yajl_gen_string(G, (const unsigned char *)_s, strlen(_s)); \
        } while (0)
#define JS_INT(G, INT) yajl_gen_integer(g, INT)

void fmt_msg_output_json (FILE *fp, const rd_kafka_message_t *rkmessage) {
        yajl_gen g;
        const char *topic = rd_kafka_topic_name(rkmessage->rkt);
        const unsigned char *buf;
        size_t len;

        g = yajl_gen_alloc(NULL);

        yajl_gen_map_open(g);
        JS_STR(g, "topic");
        JS_STR(g, topic);

        JS_STR(g, "partition");
        yajl_gen_integer(g, (int)rkmessage->partition);

        JS_STR(g, "offset");
        yajl_gen_integer(g, (long long int)rkmessage->offset);

#if RD_KAFKA_VERSION >= 0x000902ff
        {
                rd_kafka_timestamp_type_t tstype;
                int64_t ts = rd_kafka_message_timestamp(rkmessage, &tstype);
                if (tstype != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
                        JS_STR(g, "tstype");
                        if (tstype == RD_KAFKA_TIMESTAMP_CREATE_TIME)
                                JS_STR(g, "create");
                        else if (tstype == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)
                                JS_STR(g, "logappend");
                        else
                                JS_STR(g, "unknown");
                        JS_STR(g, "ts");
                        yajl_gen_integer(g, (long long int)ts);
                }
        }
#else
        JS_STR(g, "tstype");
        JS_STR(g, "unknown");
        JS_STR(g, "ts");
        yajl_gen_integer(g, 0);
#endif

        JS_STR(g, "broker");
#if RD_KAFKA_VERSION >= 0x010500ff
        yajl_gen_integer(g, (int)rd_kafka_message_broker_id(rkmessage));
#else
        yajl_gen_integer(g, -1);
#endif


#if HAVE_HEADERS
        {
                rd_kafka_headers_t *hdrs;

                if (!rd_kafka_message_headers(rkmessage, &hdrs)) {
                        const char *name;
                        const void *value;
                        size_t size;

                        JS_STR(g, "headers");
                        yajl_gen_map_open(g);

                        while (!rd_kafka_header_get_all(hdrs, 0, &name,
                                                        &value, &size)) {
                                JS_STR(g, name);
                                size_t idx = 0;
                                yajl_gen_array_open(g);
                                while (!rd_kafka_header_get(hdrs, idx++, name,
                                                            &value, &size)) {
                                        if (value)
                                                yajl_gen_string(g, value, size);
                                        else
                                                yajl_gen_null(g);
                                }
                                yajl_gen_array_close(g);
                                rd_kafka_header_remove(hdrs, name);
                        }

                        yajl_gen_map_close(g);
                }
        }
#endif


        JS_STR(g, "key");
        if (rkmessage->key) {
#if ENABLE_AVRO && YAJL_HAS_GEN_VERBATIM
                if (conf.flags & CONF_F_FMT_AVRO_KEY) {
                        char errstr[256];
                        int schema_id = -1;
                        char *json = kc_avro_to_json(
                                rkmessage->key,
                                rkmessage->key_len,
                                &schema_id,
                                errstr, sizeof(errstr));

                        if (!json) {
                                KC_ERROR("Failed to deserialize key in "
                                         "message in %s [%"PRId32"] at "
                                         "offset %"PRId64": %s",
                                         rd_kafka_topic_name(rkmessage->rkt),
                                         rkmessage->partition,
                                         rkmessage->offset, errstr);
                                yajl_gen_null(g);
                                JS_STR(g, "key_error");
                                JS_STR(g, errstr);
                        } else {
                                yajl_gen_verbatim(g, json, strlen(json));
                                JS_STR(g, "key_schema_id");
                                JS_INT(g, schema_id);
                        }
                        free(json);
                } else
#endif
                        yajl_gen_string(g,
                                        (const unsigned char *)rkmessage->key,
                                        rkmessage->key_len);
        } else
                yajl_gen_null(g);

        JS_STR(g, "payload");
        if (rkmessage->payload) {
#if ENABLE_AVRO && YAJL_HAS_GEN_VERBATIM
                if (conf.flags & CONF_F_FMT_AVRO_VALUE) {
                        char errstr[256];
                        int schema_id = -1;
                        char *json = kc_avro_to_json(
                                rkmessage->payload,
                                rkmessage->len,
                                &schema_id,
                                errstr, sizeof(errstr));

                        if (!json) {
                                KC_ERROR("Failed to deserialize value in "
                                         "message in %s [%"PRId32"] at "
                                         "offset %"PRId64": %s",
                                         rd_kafka_topic_name(rkmessage->rkt),
                                         rkmessage->partition,
                                         rkmessage->offset, errstr);
                                yajl_gen_null(g);
                                JS_STR(g, "payload_error");
                                JS_STR(g, errstr);
                        } else {
                                yajl_gen_verbatim(g, json, strlen(json));
                                JS_STR(g, "value_schema_id");
                                JS_INT(g, schema_id);
                        }

                        free(json);
                } else
#endif
                        yajl_gen_string(g,
                                        (const unsigned char *)
                                        rkmessage->payload,
                                        rkmessage->len);
        } else
                yajl_gen_null(g);

        yajl_gen_map_close(g);

        yajl_gen_get_buf(g, &buf, &len);

        if (fwrite(buf, len, 1, fp) != 1 ||
            (conf.fmt[0].str_len > 0 &&
             fwrite(conf.fmt[0].str, conf.fmt[0].str_len, 1, fp) != 1))
                KC_FATAL("Output write error: %s", strerror(errno));

        yajl_gen_free(g);
}



/**
 * Print metadata information
 */
void metadata_print_json (const struct rd_kafka_metadata *metadata,
                          int32_t controllerid) {
        yajl_gen g;
        int i, j, k;
        const unsigned char *buf;
        size_t len;

        g = yajl_gen_alloc(NULL);

        yajl_gen_map_open(g);

        JS_STR(g, "originating_broker");
        yajl_gen_map_open(g);
        JS_STR(g, "id");
        yajl_gen_integer(g, (long long int)metadata->orig_broker_id);
        JS_STR(g, "name");
        JS_STR(g, metadata->orig_broker_name);
        yajl_gen_map_close(g);


        JS_STR(g, "query");
        yajl_gen_map_open(g);
        JS_STR(g, "topic");
        JS_STR(g, conf.topic ? : "*");
        yajl_gen_map_close(g);

        JS_STR(g, "controllerid");
        yajl_gen_integer(g, (long long int)controllerid);

        /* Iterate brokers */
        JS_STR(g, "brokers");
        yajl_gen_array_open(g);
        for (i = 0 ; i < metadata->broker_cnt ; i++) {
                int blen = strlen(metadata->brokers[i].host);
                char *host = alloca(blen+1+5+1);
                sprintf(host, "%s:%i",
                        metadata->brokers[i].host, metadata->brokers[i].port);

                yajl_gen_map_open(g);

                JS_STR(g, "id");
                yajl_gen_integer(g, (long long int)metadata->brokers[i].id);

                JS_STR(g, "name");
                JS_STR(g, host);

                yajl_gen_map_close(g);
        }
        yajl_gen_array_close(g);

        /* Iterate topics */
        JS_STR(g, "topics");
        yajl_gen_array_open(g);
        for (i = 0 ; i < metadata->topic_cnt ; i++) {
                const struct rd_kafka_metadata_topic *t = &metadata->topics[i];

                yajl_gen_map_open(g);
                JS_STR(g, "topic");
                JS_STR(g, t->topic);

                if (t->err) {
                        JS_STR(g, "error");
                        JS_STR(g, rd_kafka_err2str(t->err));
                }

                JS_STR(g, "partitions");
                yajl_gen_array_open(g);

                /* Iterate topic's partitions */
                for (j = 0 ; j < t->partition_cnt ; j++) {
                        const struct rd_kafka_metadata_partition *p;
                        p = &t->partitions[j];

                        yajl_gen_map_open(g);

                        JS_STR(g, "partition");
                        yajl_gen_integer(g, (long long int)p->id);

                        if (p->err) {
                                JS_STR(g, "error");
                                JS_STR(g, rd_kafka_err2str(p->err));
                        }

                        JS_STR(g, "leader");
                        yajl_gen_integer(g, (long long int)p->leader);

                        /* Iterate partition's replicas */
                        JS_STR(g, "replicas");
                        yajl_gen_array_open(g);
                        for (k = 0 ; k < p->replica_cnt ; k++) {
                                yajl_gen_map_open(g);
                                JS_STR(g, "id");
                                yajl_gen_integer(g,
                                                 (long long int)p->replicas[k]);
                                yajl_gen_map_close(g);
                        }
                        yajl_gen_array_close(g);


                        /* Iterate partition's ISRs */
                        JS_STR(g, "isrs");
                        yajl_gen_array_open(g);
                        for (k = 0 ; k < p->isr_cnt ; k++) {
                                yajl_gen_map_open(g);
                                JS_STR(g, "id");
                                yajl_gen_integer(g, (long long int)p->isrs[k]);
                                yajl_gen_map_close(g);
                        }
                        yajl_gen_array_close(g);

                        yajl_gen_map_close(g);

                }
                yajl_gen_array_close(g);

                yajl_gen_map_close(g);
        }
        yajl_gen_array_close(g);

        yajl_gen_map_close(g);

        yajl_gen_get_buf(g, &buf, &len);

        if (fwrite(buf, len, 1, stdout) != 1)
                KC_FATAL("Output write error: %s", strerror(errno));

        yajl_gen_free(g);
}


/**
 * @brief Generate (if json_gen is a valid yajl_gen), or print (if json_gen is NULL)
 *        a map of topic+partitions+offsets[+errors]
 *
 * { "<topic>": { "topic": "<topic>",
 *                "<partition>": { "partition": <partition>, "offset": <o>,
 *                                  ["error": "..."]},
 *                 .. },
 *  .. }
 */
void partition_list_print_json (const rd_kafka_topic_partition_list_t *parts,
                                void *json_gen) {
        yajl_gen g = (yajl_gen)json_gen;
        int i;
        const char *last_topic = "";

        if (!g)
                g = yajl_gen_alloc(NULL);

        yajl_gen_map_open(g);
        for (i = 0 ; i < parts->cnt ; i++) {
                const rd_kafka_topic_partition_t *p = &parts->elems[i];
                char partstr[16];

                if (strcmp(last_topic, p->topic)) {
                        if (*last_topic)
                                yajl_gen_map_close(g); /* topic */

                        JS_STR(g, p->topic);
                        yajl_gen_map_open(g); /* topic */
                        JS_STR(g, "topic");
                        JS_STR(g, p->topic);
                        last_topic = p->topic;
                }

                snprintf(partstr, sizeof(partstr), "%"PRId32, p->partition);

                JS_STR(g, partstr);
                yajl_gen_map_open(g);
                JS_STR(g, "partition");
                yajl_gen_integer(g, p->partition);
                JS_STR(g, "offset");
                yajl_gen_integer(g, p->offset);
                if (p->err) {
                        JS_STR(g, "error");
                        JS_STR(g, rd_kafka_err2str(p->err));
                }
                yajl_gen_map_close(g);

        }

        if (*last_topic)
                yajl_gen_map_close(g); /* topic */

        yajl_gen_map_close(g);


        if (!json_gen) {
                const unsigned char *buf;
                size_t len;

                yajl_gen_get_buf(g, &buf, &len);
                (void)fwrite(buf, len, 1, stdout);
                yajl_gen_free(g);
        }
}



void fmt_init_json (void) {
}

void fmt_term_json (void) {
}


int json_can_emit_verbatim (void) {
#if YAJL_HAS_GEN_VERBATIM
        return 1;
#else
        return 0;
#endif
}
