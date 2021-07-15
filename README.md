![logo by @dtrapezoid](./resources/kafkacat_small.png)

# kafkacat

Copyright (c) 2014-2020 Magnus Edenhill

[https://github.com/edenhill/kafkacat](https://github.com/edenhill/kafkacat)

*kafkacat logo by [@dtrapezoid](https://twitter.com/dtrapezoid)*


## What is kafkacat

**kafkacat** is a generic non-JVM producer and consumer for Apache Kafka >=0.8,
think of it as a netcat for Kafka.

In **producer** mode kafkacat reads messages from stdin, delimited with a
configurable delimiter (-D, defaults to newline), and produces them to the
provided Kafka cluster (-b), topic (-t) and partition (-p).

In **consumer** mode kafkacat reads messages from a topic and partition and
prints them to stdout using the configured message delimiter.

There's also support for the Kafka >=0.9 high-level balanced consumer, use
the `-G <group>` switch and provide a list of topics to join the group.

kafkacat also features a Metadata list (-L) mode to display the current
state of the Kafka cluster and its topics and partitions.

Supports Avro message deserialization using the Confluent Schema-Registry,
and generic primitive deserializers (see examples below).

kafkacat is fast and lightweight; statically linked it is no more than 150Kb.


## Try it out with docker

```bash
# List brokers and topics in cluster
$ docker run -it --network=host edenhill/kafkacat:1.6.0 -b YOUR_BROKER -L
```

See [Examples](#examples) for usage options, and [Running in Docker](#running-in-docker) for more information on how to properly run docker-based clients with Kafka.


## Install

### On recent enough Debian systems:

````
apt-get install kafkacat
````

On recent openSUSE systems:

```
zypper addrepo https://download.opensuse.org/repositories/network:utilities/openSUSE_Factory/network:utilities.repo
zypper refresh
zypper install kafkacat
```
(see [this page](https://software.opensuse.org/download/package?package=kafkacat&project=network%3Autilities) for instructions to install with openSUSE LEAP)

### On Mac OS X with homebrew installed:

````
brew install kafkacat
````

### On Fedora

```
# dnf copr enable bvn13/kafkacat
# dnf update
# dnf install kafkacat
```

See [this blog](https://rmoff.net/2020/04/20/how-to-install-kafkacat-on-fedora/) for how to build from sources and install kafkacat on recent Fedora systems.


### Otherwise follow directions below


## Requirements

 * librdkafka - https://github.com/edenhill/librdkafka
 * libyajl (for JSON support, optional)
 * libavro-c and libserdes (for Avro support, optional. See https://github.com/confluentinc/libserdes)

On Ubuntu or Debian: `sudo apt-get install librdkafka-dev libyajl-dev`

## Build

    ./configure <usual-configure-options>
    make
    sudo make install

### Build for Windows

    cd win32
    nuget restore
    msbuild

## Quick build

The bootstrap.sh build script will download and build the required dependencies,
providing a quick and easy means of building kafkacat.
Internet connectivity and wget/curl is required by this script.
The resulting kafkacat binary will be linked statically to avoid runtime
dependencies.
**NOTE**: Requires `curl` and `cmake` (for yajl) to be installed.

    ./bootstrap.sh


## Configuration

Any librdkafka [configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
property can be set on the command line using `-X property=value`, or in
a configuration file specified by `-F <config-file>`.

If no configuration file was specified with `-F ..` on the command line,
kafkacat will try the `$KAFKACAT_CONFIG` environment variable,
and then the default configuration file `~/.config/kafkacat.conf`.

Configuration files are optional.


## Examples

High-level balanced KafkaConsumer: subscribe to topic1 and topic2
(requires broker >=0.9.0 and librdkafka version >=0.9.1)

    $ kafkacat -b mybroker -G mygroup topic1 topic2


Read messages from stdin, produce to 'syslog' topic with snappy compression

    $ tail -f /var/log/syslog | kafkacat -b mybroker -t syslog -z snappy


Read messages from Kafka 'syslog' topic, print to stdout

    $ kafkacat -b mybroker -t syslog


Produce messages from file (one file is one message)

    $ kafkacat -P -b mybroker -t filedrop -p 0 myfile1.bin /etc/motd thirdfile.tgz


Produce messages transactionally (one single transaction for all messages):

    $ kafkacat -P -b mybroker -t mytopic -X transactional.id=myproducerapp


Read the last 2000 messages from 'syslog' topic, then exit

    $ kafkacat -C -b mybroker -t syslog -p 0 -o -2000 -e


Consume from all partitions from 'syslog' topic

    $ kafkacat -C -b mybroker -t syslog


Output consumed messages in JSON envelope:

    $ kafkacat -b mybroker -t syslog -J


Decode Avro key (`-s key=avro`), value (`-s value=avro`) or both (`-s avro`) to JSON using schema from the Schema-Registry:

    $ kafkacat -b mybroker -t ledger -s avro -r http://schema-registry-url:8080


Decode Avro message value and extract Avro record's "age" field:

    $ kafkacat -b mybroker -t ledger -s value=avro -r http://schema-registry-url:8080 | jq .payload.age


Decode key as 32-bit signed integer and value as 16-bit signed integer followed by an unsigned byte followed by string:

    $ kafkacat -b mybroker -t mytopic -s key='i$' -s value='hB s'


*Hint: see `./kafkacat -h` for all available deserializer options.*


Output consumed messages according to format string:

    $ kafkacat -b mybroker -t syslog -f 'Topic %t[%p], offset: %o, key: %k, payload: %S bytes: %s\n'


Read the last 100 messages from topic 'syslog' with  librdkafka configuration parameter 'broker.version.fallback' set to '0.8.2.1' :

    $ kafkacat -C -b mybroker -X broker.version.fallback=0.8.2.1 -t syslog -p 0 -o -100 -e


Produce a tombstone (a "delete" for compacted topics) for key "abc" by providing an empty message value which `-Z` interpretes as NULL:

    $ echo "abc:" | kafkacat -b mybroker -t mytopic -Z -K:


Produce with headers:

    $ echo "hello there" | kafkacat -b mybroker -H "header1=header value" -H "nullheader" -H "emptyheader=" -H "header1=duplicateIsOk"


Print headers in consumer:

    $ kafkacat -b mybroker -C -t mytopic -f 'Headers: %h: Message value: %s\n'


Enable the idempotent producer, providing exactly-once and strict-ordering
**producer** guarantees:

    $ kafkacat -b mybroker -X enable.idempotence=true -P -t mytopic ....


Connect to cluster using SSL and SASL PLAIN authentication:

    $ kafkacat -b mybroker -X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN -X sasl.username=myapikey -X sasl.password=myapisecret ...


Metadata listing:

```
$ kafkacat -L -b mybroker
Metadata for all topics (from broker 1: mybroker:9092/1):
 3 brokers:
  broker 1 at mybroker:9092
  broker 2 at mybrokertoo:9092
  broker 3 at thirdbroker:9092
 16 topics:
  topic "syslog" with 3 partitions:
    partition 0, leader 3, replicas: 1,2,3, isrs: 1,2,3
    partition 1, leader 1, replicas: 1,2,3, isrs: 1,2,3
    partition 2, leader 1, replicas: 1,2, isrs: 1,2
  topic "rdkafkatest1_auto_49f744a4327b1b1e" with 2 partitions:
    partition 0, leader 3, replicas: 3, isrs: 3
    partition 1, leader 1, replicas: 1, isrs: 1
  topic "rdkafkatest1_auto_e02f58f2c581cba" with 2 partitions:
    partition 0, leader 3, replicas: 3, isrs: 3
    partition 1, leader 1, replicas: 1, isrs: 1
  ....
```


JSON metadata listing

    $ kafkacat -b mybroker -L -J


Pretty-printed JSON metadata listing

    $ kafkacat -b mybroker -L -J | jq .


Query offset(s) by timestamp(s)

    $ kafkacat -b mybroker -Q -t mytopic:3:2389238523 -t mytopic2:0:18921841


Consume messages between two timestamps

    $ kafkacat -b mybroker -C -t mytopic -o s@1568276612443 -o e@1568276617901



## Running in Docker

The latest kafkacat docker image is `edenhill/kafkacat:1.6.0`, there's
also [Confluent's kafkacat docker images on Docker Hub](https://hub.docker.com/r/confluentinc/cp-kafkacat/).

If you are connecting to Kafka brokers also running on Docker you should specify the network name as part of the `docker run` command using the `--network` parameter. For more details of networking with Kafka and Docker [see this post](https://rmoff.net/2018/08/02/kafka-listeners-explained/).

Here are two short examples of using kafkacat from Docker. See the [Docker Hub listing](https://hub.docker.com/r/confluentinc/cp-kafkacat/) and [kafkacat docs](https://docs.confluent.io/current/app-development/kafkacat-usage.html) for more details:

**Send messages using [here doc](http://tldp.org/LDP/abs/html/here-docs.html):**

```
docker run -it --rm \
        edenhill/kafkacat \
                -b kafka-broker:9092 \
                -t test \
                -K: \
                -P <<EOF

1:{"order_id":1,"order_ts":1534772501276,"total_amount":10.50,"customer_name":"Bob Smith"}
2:{"order_id":2,"order_ts":1534772605276,"total_amount":3.32,"customer_name":"Sarah Black"}
3:{"order_id":3,"order_ts":1534772742276,"total_amount":21.00,"customer_name":"Emma Turner"}
EOF
```

**Consume messages:**

```
docker run -it --rm \
        edenhill/kafkacat \
           -b kafka-broker:9092 \
           -C \
           -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\n\Partition: %p\tOffset: %o\n--\n' \
           -t test

Key (1 bytes): 1
Value (88 bytes): {"order_id":1,"order_ts":1534772501276,"total_amount":10.50,"customer_name":"Bob Smith"}
Partition: 0    Offset: 0
--

Key (1 bytes): 2
Value (89 bytes): {"order_id":2,"order_ts":1534772605276,"total_amount":3.32,"customer_name":"Sarah Black"}
Partition: 0    Offset: 1
--

Key (1 bytes): 3
Value (90 bytes): {"order_id":3,"order_ts":1534772742276,"total_amount":21.00,"customer_name":"Emma Turner"}
Partition: 0    Offset: 2
--
% Reached end of topic test [0] at offset 3
```
