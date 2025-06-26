AMQP Tools
==========

## Installation

```bash
git clone git@github.com:timfennis/amqp-tools.git
cargo install --path amqp-tools/
```

## Configuration

To configure the utility you must create a `config.toml` file in the [appropriate directory](https://crates.io/crates/dirs). Inside
this config file you can specify multiple sources.

```toml
[local]
username = "guest"
password = "guest"
host = "localhost"
port = 5672
secure = false
vhost = "/"

[prod]
username = "asdf"
password = "hunter2"
host = "example.com"
port = 5671
secure = true
vhost = "my-vhost"
```

When running the application you can select the source using the `-c [name]` flag.

## Usage

Read 10 messages from a queue and store them inside an output directory:

```bash
amqp-tools read -c local --limit 10 --output out/ my_queue
```

Look write the first message in the queue to stdout without removing it:

```bash
amqp-tools peek -c local my_queue
```