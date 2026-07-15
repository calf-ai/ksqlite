# How to connect to a secured cluster

This guide shows you how to configure KSQLite against a cluster that requires
SASL or TLS.

KSQLite runs three Kafka clients: a producer for appends, a consumer for
rehydrates, and an admin client for topic checks. **All three must
authenticate**, and you configure them through two parameters.

## Configure both `producer_config` and `consumer_config`

This is the part that catches people out. `producer_config` and `consumer_config`
are independent — nothing copies security settings from one to the other. The
admin client, however, takes its security settings from `consumer_config`.

| Client | Security settings come from |
|---|---|
| Rehydrate consumer | `consumer_config` |
| Admin client | `consumer_config` (security keys only) |
| Changelog producer | `producer_config` |

So define the settings once and pass them to both:

```python
security = {
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_plain_username": os.environ["KAFKA_USER"],
    "sasl_plain_password": os.environ["KAFKA_PASSWORD"],
}

store = KSQLite(
    db_path="state.db",
    bootstrap_servers="broker:9093",
    producer_config=security,
    consumer_config=security,
)
```

Passing them to `consumer_config` alone leaves the producer unauthenticated, and
`start()` fails when the producer cannot connect:

```text
KSQLiteError: producer failed to start: ...
```

## Settings the admin client receives

The admin client gets `bootstrap_servers` plus only the security-relevant keys of
`consumer_config`: `security_protocol`, `ssl_context`, and any key beginning
`sasl_`. Nothing else in `consumer_config` reaches it.

The keys it accepts:

```text
security_protocol   sasl_mechanism         sasl_plain_username
ssl_context         sasl_kerberos_service_name   sasl_plain_password
                    sasl_kerberos_domain_name    sasl_oauth_token_provider
```

If a future `aiokafka` drops one of these, KSQLite filters it out rather than
passing an argument the constructor would reject.

## Use TLS

Build an `ssl_context` and pass it through both configs:

```python
from aiokafka.helpers import create_ssl_context

ssl_context = create_ssl_context(
    cafile="ca.pem",
    certfile="client.pem",
    keyfile="client.key",
)

security = {"security_protocol": "SSL", "ssl_context": ssl_context}

store = KSQLite(
    db_path="state.db",
    bootstrap_servers="broker:9093",
    producer_config=security,
    consumer_config=security,
)
```

For SASL over TLS, use `security_protocol="SASL_SSL"` with both the
`ssl_context` and the `sasl_*` keys.

## Keys you cannot set on the consumer

Three `consumer_config` keys are always overridden, because the rehydrate
consumer assigns partitions manually and must never join a group or commit
offsets:

| Key | Always |
|---|---|
| `group_id` | `None` |
| `enable_auto_commit` | `False` |
| `auto_offset_reset` | `"none"` |

Setting them has no effect. Everything else in `consumer_config` is passed
through.

On the producer side, `acks` defaults to `1` and you may raise it — `acks="all"`
is a reasonable choice on a replicated cluster. `acks=0` raises `ConfigError`,
because the append path needs the acked offset.

## Grant the right ACLs

| Client | Needs |
|---|---|
| Producer | `WRITE` on each changelog topic |
| Rehydrate consumer | `READ` on each changelog topic |
| Admin | `DESCRIBE` on the cluster; `DESCRIBE_CONFIGS` on each changelog topic |
| Admin | `CREATE` — only if you set `create_topics_retention_ms` |

Without `DESCRIBE_CONFIGS`, the `cleanup.policy` check cannot run. KSQLite warns
and proceeds. See
[How to provision changelog topics](provision-changelog-topics.md).

## See also

- [`KSQLite`](../reference/ksqlite.md) — how the client kwargs are merged
- [How to provision changelog topics](provision-changelog-topics.md)
