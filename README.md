OIDBS(Open IoT Database Benchmark Suite)
----------------------------------------

## Introduction

Since there is no suitable benchmark program to bench the new end-to-end IoT database, we built this open source project for benchmarking more scientifically and simply.

The open-source benchmark suite is a great tool for evaluating the performance of the IoT domain data systems.

## Benchmark Result

The official up-to-date benchmark result could be seen in [the website](https://joinbase.io/benchmark/). We encourage you to reproduce the benchmark by yourself. Any feedback or suggestion is welcome.

## Principles

There are common pitfalls in existed benchmarks, like:

* only using non-real-world, artificial datasets,
* or only choosing bench items which may be beneficial to the benchers,
* or confusing concepts intentionally or unintentionally,
* or benchmark logics are too deeply bound to specific languages, which makes benchmarks difficult to use or extend.

In the new benchmark suite, we set the following principles:

1. Real-world scenarios oriented, for revealing meaningful production performance values.
2. Modular design, to allow the separation of different benchmarking, testing and stressing concerns.
3. Simple to use, with reasonable default values, for most possible parameters.
4. Top benchmark performance, to avoid overheads and pitfalls within suites themselves.

## Unique Features

1. IoT messaging native

    Note, JoinBase is the true end-to-end IoT database which allow users to ingest MQTT messages from devices to the DB server directly without any intermediates. 

2. Physicalized device

    The "Device" concept is mapped to a physical TCP connection, rather than just a model parameter. It is no longer able or meaningful to setup 1 million devices in one 1 client - 1 server test topology.

3. Top performance bench client

    As our testing, our preemptive bench client can provide 3x more higher peak throughput than that of the goroutine based bench codes with enough parallelisms. 

## Architecture

![OIDBS Architecture](docs/arch.jpg?raw=true "OIDBS Architecture")

### Benchmark Model

The core of OIDBS is the benchmark model, which is used for grouping different benchmark dataset and its corresponding data-gens, schemas, queries.

More about current two workable models could [be seen here](https://joinbase.io/benchmark/#benchmark-model).

### Composable Commands


## Quick Start

### Get the OIDBS

1. Download
    Download the binary from `release`.

2. Building (for contributors)
    ```bash
    $ cargo b --release --bin oidbs --help
    ```

### Use 
```bash
$ oidbs --help 
$ oidbs gen --help
$ oidbs import --help
$ oidbs bench --help
```

#### Gen

To generate dataset for benchmarking.

> :mag_right:  you must specify the output directory to ensure that the output location confirmed

```bash
$ oidbs gen /data/n4/oidbs_data 
```

#### Import

To import dataset to targeted servers.

> :mag_right:  the dataset could be external, then not needed to be generated before importing. For example, nyct series model.

> :mag_right:  you need to create a user for the JoinBase before importing and put the JoinBase's mqtt server uri part and JoinBase's pg server uri part here, because JoinBase has no default user.

```bash
$ oidbs import /dataset/nyc_data -n nyct_lite
```

#### Bench

To run all benchmark queries against the target servers.

> :mag_right:  you need to create a user for the JoinBase before importing and put the JoinBase's mqtt server uri part and JoinBase's pg server uri part here, because JoinBase has no default user.

```bash
$ oidbs bench -n nyct_strip
```
