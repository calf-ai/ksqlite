"""E2E suite configuration (fixtures land at P8/P10; plan §4.2, §5 P10).

The broker image pin below is a P0 deliverable (plan §5 P0; decisions log,
implementation-handoff item 2): the exact version the plan review's live
probes verified. Do not bump casually — DeleteRecords (E2E-08a) requires
Redpanda >= v24.x, and any other pin abandons the verified baseline.
"""

REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v24.2.20"
