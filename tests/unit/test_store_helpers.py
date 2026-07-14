"""P8/P9 `_store` pure helpers: RH-21 (plan §5 P8)."""

from ksqlite import _store


def test_rh21_rehydrate_consumer_isolation_keys_pinned() -> None:
    """The three keys are present WITH their pinned values even when
    consumer_config tries to override each — aiokafka's own defaults are the
    catastrophic values (spec §7 step 2; the merge lives ABOVE the factory
    seam, F-13 re-pins at the facade).
    """
    merged = _store.merged_consumer_kwargs(
        bootstrap_servers="broker:9092",
        consumer_config={
            "group_id": "evil-group",
            "enable_auto_commit": True,
            "auto_offset_reset": "latest",
            "sasl_mechanism": "PLAIN",
        },
    )

    assert merged["group_id"] is None
    assert merged["enable_auto_commit"] is False
    assert merged["auto_offset_reset"] == "none"
    assert merged["sasl_mechanism"] == "PLAIN"  # pass-through survives
    assert merged["bootstrap_servers"] == "broker:9092"

    bare = _store.merged_consumer_kwargs(
        bootstrap_servers="broker:9092", consumer_config={}
    )
    assert bare["group_id"] is None
    assert bare["enable_auto_commit"] is False
    assert bare["auto_offset_reset"] == "none"
