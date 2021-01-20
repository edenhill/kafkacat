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
#include "rdendian.h"
#include <time.h>

static void fmt_add (fmt_type_t type, const char *str, int len) {
        if (conf.fmt_cnt == KC_FMT_MAX_SIZE)
                KC_FATAL("Too many formatters & strings (KC_FMT_MAX_SIZE=%i)",
                         KC_FMT_MAX_SIZE);

        conf.fmt[conf.fmt_cnt].type = type;

        /* For STR types */
        if (len) {
                const char *s;
                char *d;
                conf.fmt[conf.fmt_cnt].str = d = malloc(len+1);
                memcpy(d, str, len);
                d[len] = '\0';
                s = d;

                /* Convert \.. sequences */
                while (*s) {
                        if (*s == '\\' && *(s+1)) {
                                int base = 0;
                                const char *next;
                                s++;
                                switch (*s) {
                                case 't':
                                        *d = '\t';
                                        break;
                                case 'n':
                                        *d = '\n';
                                        break;
                                case 'r':
                                        *d = '\r';
                                        break;
                                case 'x':
                                        s++;
                                        base = 16;
                                        /* FALLTHRU */
                                default:
                                        if (*s >= '0' && *s <= '9') {
                                                *d = (char)strtoul(
                                                        s, (char **)&next,
                                                        base);
                                                if (next > s)
                                                        s = next - 1;
                                        } else {
                                                *d = *s;
                                        }
                                        break;
                                }
                        } else {
                                *d = *s;
                        }
                        s++;
                        d++;
                }

                *d = '\0';

                conf.fmt[conf.fmt_cnt].str_len =
                        strlen(conf.fmt[conf.fmt_cnt].str);
        }

        conf.fmt_cnt++;
}


/**
 * Parse a format string to create a formatter list.
 */
void fmt_parse (const char *fmt) {
        const char *s = fmt, *t;

        while (*s) {
                if ((t = strchr(s, '%'))) {
                        if (t > s)
                                fmt_add(KC_FMT_STR, s, (int)(t-s));

                        s = t+1;
                        switch (*s)
                        {
                        case 'o':
                                fmt_add(KC_FMT_OFFSET, NULL, 0);
                                break;
                        case 'k':
                                fmt_add(KC_FMT_KEY, NULL, 0);
                                break;
                        case 'K':
                                fmt_add(KC_FMT_KEY_LEN, NULL, 0);
                                break;
                        case 's':
                                fmt_add(KC_FMT_PAYLOAD, NULL, 0);
                                break;
                        case 'S':
                                fmt_add(KC_FMT_PAYLOAD_LEN, NULL, 0);
                                break;
                        case 'R':
                                fmt_add(KC_FMT_PAYLOAD_LEN_BINARY, NULL, 0);
                                break;
                        case 't':
                                fmt_add(KC_FMT_TOPIC, NULL, 0);
                                break;
                        case 'p':
                                fmt_add(KC_FMT_PARTITION, NULL, 0);
                                break;
                        case 'T':
                                fmt_add(KC_FMT_TIMESTAMP, NULL, 0);
                                conf.flags |= CONF_F_APIVERREQ;
                                break;
#if HAVE_HEADERS
                        case 'h':
                                fmt_add(KC_FMT_HEADERS, NULL, 0);
                                conf.flags |= CONF_F_APIVERREQ;
                                break;
#endif
                        case '%':
                                fmt_add(KC_FMT_STR, s, 1);
                                break;
                        case '\0':
                                KC_FATAL("Empty formatter");
                                break;
                        default:
                                KC_FATAL("Unsupported formatter: %%%c", *s);
                                break;
                        }
                        s++;
                } else {
                        fmt_add(KC_FMT_STR, s, strlen(s));
                        break;
                }

        }
}




void fmt_init (void) {
#if ENABLE_JSON
        if (conf.flags & CONF_F_FMT_JSON)
                fmt_init_json();
#endif
}

void fmt_term (void) {
#if ENABLE_JSON
        if (conf.flags & CONF_F_FMT_JSON)
                fmt_term_json();
#endif
}


#if HAVE_HEADERS
static int print_headers (FILE *fp, const rd_kafka_headers_t *hdrs) {
        size_t idx = 0;
        const char *name;
        const void *value;
        size_t size;

        while (!rd_kafka_header_get_all(hdrs, idx++, &name, &value, &size)) {
                fprintf(fp, "%s%s=", idx > 1 ? "," : "", name);
                if (value && size > 0)
                        fprintf(fp, "%.*s", (int)size, (const char *)value);
                else if (!value)
                        fprintf(fp, "NULL");
        }
        return 1;
}
#endif


/**
 * @brief Check that the pack-format string is valid.
 */
void pack_check (const char *what, const char *fmt) {
        const char *f = fmt;
        static const char *valid = " <>bBhHiIqQcs$";

        if (!*fmt)
                KC_FATAL("%s pack-format must not be empty", what);

        while (*f) {
                if (!strchr(valid, (int)(*f)))
                        KC_FATAL("Invalid token '%c' in %s pack-format, "
                                 "see -s usage.",
                                 (int)(*f), what);
                f++;
        }
}


/**
 * @brief Unpack (deserialize) the data at \p buf using the
 *        pack-format string \p fmt.
 *        \p fmt must be a valid pack-format string.
 *        Prints result to \p fp.
 *
 * Format is inspired by Python's struct.unpack()
 *
 * @returns 0 on success or -1 on error.
 */
static int unpack (FILE *fp, const char *what, const char *fmt,
                   const char *buf, size_t len,
                   char *errstr, size_t errstr_size) {
        const char *b = buf;
        const char *end = buf+len;
        const char *f = fmt;
        enum {
                big_endian,
                little_endian
        } endian = big_endian;

#define endian_swap(val,to_little,to_big)                       \
        (endian == big_endian ? to_big(val) : to_little(val))

#define fup_copy(dst,sz) do {                                           \
                if ((sz) > remaining) {                                 \
                        snprintf(errstr, errstr_size,                   \
                                 "%s truncated, expected %d bytes "     \
                                 "to unpack %c but only %d bytes remaining", \
                                 what, (int)(sz), (int)(*f),            \
                                 (int)remaining);                       \
                        return -1;                                      \
                }                                                       \
                memcpy(dst, b, sz);                                     \
                b += (sz);                                              \
        } while (0)

        while (*f) {
                size_t remaining = (int)(end - b);

                switch ((int)*f)
                {
                case ' ':
                        fprintf(fp, " ");
                        break;
                case '<':
                        endian = little_endian;
                        break;
                case '>':
                        endian = big_endian;
                        break;
                case 'b':
                {
                        int8_t v;
                        fup_copy(&v, sizeof(v));
                        fprintf(fp, "%d", (int)v);
                }
                break;
                case 'B':
                {
                        uint8_t v;
                        fup_copy(&v, sizeof(v));
                        fprintf(fp, "%u", (unsigned int)v);
                }
                break;
                case 'h':
                {
                        int16_t v;
                        fup_copy(&v, sizeof(v));
                        v = endian_swap(v, be16toh, be16toh);
                        fprintf(fp, "%hd", v);
                }
                break;
                case 'H':
                {
                        uint16_t v;
                        fup_copy(&v, sizeof(v));
                        v = endian_swap(v, be16toh, be16toh);
                        fprintf(fp, "%hu", v);
                }
                break;
                case 'i':
                {
                        int32_t v;
                        fup_copy(&v, sizeof(v));
                        v = endian_swap(v, be32toh, htobe32);
                        fprintf(fp, "%d", v);
                }
                break;
                case 'I':
                {
                        uint32_t v;
                        fup_copy(&v, sizeof(v));
                        v = endian_swap(v, be32toh, htobe32);
                        fprintf(fp, "%u", v);
                }
                break;
                case 'q':
                {
                        int64_t v;
                        fup_copy(&v, sizeof(v));
                        v = endian_swap(v, be64toh, htobe64);
                        fprintf(fp, "%"PRId64, v);
                }
                break;
                case 'Q':
                {
                        uint64_t v;
                        fup_copy(&v, sizeof(v));
                        v = endian_swap(v, be64toh, htobe64);
                        fprintf(fp, "%"PRIu64, v);
                }
                break;
                case 'c':
                        fprintf(fp, "%c", (int)*b);
                        b++;
                        break;
                case 's':
                {
                        fprintf(fp, "%.*s", (int)remaining, b);
                        b += remaining;
                }
                break;
                case '$':
                {
                        if (remaining > 0) {
                                snprintf(errstr, errstr_size,
                                         "expected %s end-of-input, "
                                         "but %d bytes remaining",
                                         what, (int)remaining);
                                return -1;
                        }
                }
                break;

                default:
                        KC_FATAL("Invalid pack-format token '%c'", (int)*f);
                        break;

                }

                f++;
        }

        /* Ignore remaining input bytes, if any. */
        return 0;

#undef endian_swap
#undef fup_copy
}


void timestamp_as_utc_string(char *dst, int64_t epoch) {
    time_t seconds = (time_t)(epoch/1000);
    int milliseconds = epoch % 1000;
    struct tm ts;

    // Format time, "ddd yyyy-mm-dd hh:mm:ss.xxx zzz", xxx is place for milliseconds
    ts = *localtime(&seconds);
    strftime(dst, 64, "%a %Y-%m-%d %H:%M:%S.xxx %Z", &ts);

    char* p;
    p = strstr(dst, "xxx");
    char ms[4];
    sprintf(ms, "%03d", milliseconds);
    strncpy(p, ms, 3);
}


/**
 * Delimited output
 */
static void fmt_msg_output_str (FILE *fp,
                                const rd_kafka_message_t *rkmessage) {
        int i;
        char errstr[256];

        *errstr = '\0';

        for (i = 0 ; i < conf.fmt_cnt ; i++) {
                int r = 1;
                uint32_t belen;
                const char *what_failed = "";

                switch (conf.fmt[i].type)
                {
                case KC_FMT_OFFSET:
                        r = fprintf(fp, "%"PRId64, rkmessage->offset);
                        break;

                case KC_FMT_KEY:
                        if (rkmessage->key_len) {
                                if (conf.flags & CONF_F_FMT_AVRO_KEY) {
#if ENABLE_AVRO
                                        char *json = kc_avro_to_json(
                                                rkmessage->key,
                                                rkmessage->key_len,
                                                errstr, sizeof(errstr));

                                        if (!json) {
                                                what_failed =
                                                        "Avro/Schema-registry "
                                                        "key deserialization";
                                                goto fail;
                                        }

                                        r = fprintf(fp, "%s", json);
                                        free(json);
#else
                                        KC_FATAL("NOTREACHED");
#endif
                                } else if (conf.pack[KC_MSG_FIELD_KEY]) {
                                        if (unpack(fp,
                                                   "key",
                                                   conf.pack[KC_MSG_FIELD_KEY],
                                                   rkmessage->key,
                                                   rkmessage->key_len,
                                                   errstr, sizeof(errstr)) ==
                                            -1)
                                                goto fail;
                                } else
                                        r = fwrite(rkmessage->key,
                                                   rkmessage->key_len, 1, fp);

                        } else if (conf.flags & CONF_F_NULL)
                                r = fwrite(conf.null_str,
                                           conf.null_str_len, 1, fp);

                        break;

                case KC_FMT_KEY_LEN:
                        r = fprintf(fp, "%zd",
                                    /* Use -1 to indicate NULL keys */
                                    rkmessage->key ? (ssize_t)rkmessage->key_len : -1);
                        break;

                case KC_FMT_PAYLOAD:
                        if (rkmessage->len) {
                                if (conf.flags & CONF_F_FMT_AVRO_VALUE) {
#if ENABLE_AVRO
                                        char *json = kc_avro_to_json(
                                                rkmessage->payload,
                                                rkmessage->len,
                                                errstr,
                                                sizeof(errstr));

                                        if (!json) {
                                                what_failed =
                                                        "Avro/Schema-registry "
                                                        "message "
                                                        "deserialization";
                                                goto fail;
                                        }

                                        r = fprintf(fp, "%s", json);
                                        free(json);
#else
                                        KC_FATAL("NOTREACHED");
#endif
                                } else if (conf.pack[KC_MSG_FIELD_VALUE]) {
                                        if (unpack(fp,
                                                   "value",
                                                   conf.pack[KC_MSG_FIELD_VALUE],
                                                   rkmessage->payload,
                                                   rkmessage->len,
                                                   errstr, sizeof(errstr)) ==
                                            -1)
                                                goto fail;
                                } else
                                        r = fwrite(rkmessage->payload,
                                                   rkmessage->len, 1, fp);

                        } else if (conf.flags & CONF_F_NULL)
                                r = fwrite(conf.null_str,
                                           conf.null_str_len, 1, fp);
                        break;

                case KC_FMT_PAYLOAD_LEN:
                        r = fprintf(fp, "%zd",
                                    /* Use -1 to indicate NULL messages */
                                    rkmessage->payload ? (ssize_t)rkmessage->len : -1);
                        break;

                case KC_FMT_PAYLOAD_LEN_BINARY:
                        /* Use -1 to indicate NULL messages */
                        belen = htobe32((uint32_t)(rkmessage->payload ?
                                                   (ssize_t)rkmessage->len :
                                                   -1));
                        r = fwrite(&belen, sizeof(uint32_t), 1, fp);
                        break;

                case KC_FMT_STR:
                        r = fwrite(conf.fmt[i].str, conf.fmt[i].str_len, 1, fp);
                        break;

                case KC_FMT_TOPIC:
                        r = fprintf(fp, "%s",
                                    rd_kafka_topic_name(rkmessage->rkt));
                        break;

                case KC_FMT_PARTITION:
                        r = fprintf(fp, "%"PRId32, rkmessage->partition);
                        break;

                case KC_FMT_TIMESTAMP:
                {
#if RD_KAFKA_VERSION >= 0x000902ff
                        rd_kafka_timestamp_type_t tstype;
                        int64_t epoch = rd_kafka_message_timestamp(rkmessage, &tstype);

                        char buf[80];
                        timestamp_as_utc_string(buf, epoch);

                        r = fprintf(fp, "%"PRId64" (%s)", epoch, buf);
#else
                        r = fprintf(fp, "-1");
#endif
                        break;
                }

                case KC_FMT_HEADERS:
                {
#if HAVE_HEADERS
                        rd_kafka_headers_t *hdrs;
                        rd_kafka_resp_err_t err;

                        err = rd_kafka_message_headers(rkmessage, &hdrs);
                        if (err == RD_KAFKA_RESP_ERR__NOENT) {
                                r = 1; /* Fake it to continue */
                        } else if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                                what_failed = "Failed to parse headers";
                                snprintf(errstr, sizeof(errstr), "%s",
                                         rd_kafka_err2str(err));
                                goto fail;
                        } else {
                                r = print_headers(fp, hdrs);
                        }
#endif
                        break;
                }
                }


                if (r < 1)
                        KC_FATAL("Write error for message "
                                 "of %zd bytes in %s [%"PRId32"] "
                                 "at offset %"PRId64": %s",
                                 rkmessage->len,
                                 rd_kafka_topic_name(rkmessage->rkt),
                                 rkmessage->partition,
                                 rkmessage->offset,
                                 strerror(errno));

                continue;

        fail:
                KC_ERROR("Failed to format message in %s [%"PRId32"] "
                         "at offset %"PRId64": %s%s%s",
                         rd_kafka_topic_name(rkmessage->rkt),
                         rkmessage->partition,
                         rkmessage->offset,
                         what_failed, *what_failed ? ": " : "",
                         errstr);
                return;
        }

}


/**
 * Format and output a received message.
 */
void fmt_msg_output (FILE *fp, const rd_kafka_message_t *rkmessage) {

#if ENABLE_JSON
        if (conf.flags & CONF_F_FMT_JSON)
                fmt_msg_output_json(fp, rkmessage);
        else
#endif
                fmt_msg_output_str(fp, rkmessage);

}
