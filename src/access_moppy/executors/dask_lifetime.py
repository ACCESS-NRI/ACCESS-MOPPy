"""Keep one long-lived Dask cluster as fast at its last file as at its first.

A batch job keeps one Dask client and its workers for every source partition
of a variable, so a 1000-year sub-daily job runs hundreds of thousands of
tasks on the same worker processes. Two things here bound what accumulates in
them.

**Span metric digests.** Every worker records fine performance metrics in
``Server.digests``, one ``Digest`` (three t-digests) per key. distributed keys
task-execution metrics by the span of the computation they belong to --
``("execute", span_id, task_prefix, activity, unit)`` -- and every
``compute()`` opens a new span, so each one adds ~28 entries that are never
removed. Every 5 s, ``Server._shift_counters`` walks all of them on the
worker's event loop while holding the GIL; on a 50-partition 6-hourly job
that walk grew from 1% to ~60% of each worker's CPU. The worker's
communication (receiving tasks, reporting results, sending data to other
workers) waits behind it, so partitions slowed from 150 s to 260 s even though
the tasks themselves did not get slower. :class:`SpanDigestTrimPlugin` drops
these entries every :data:`SPAN_DIGEST_TRIM_INTERVAL_S` seconds, which kept
that job flat and 25% faster overall, the same as recreating the cluster.

Only the dashboard's per-span metric history and the ``/metrics`` Prometheus
endpoint read the digests, and the latter reads only fixed-name entries
("task-duration", "tick-duration", ...), which the trim keeps.

**Optional cluster recreation.** ``dask_recreate_every: N`` in the batch
config closes the client and starts a fresh cluster after every N source
partitions (:func:`cluster_recreate_due`). The digest trim makes this
unnecessary for the slowdown above; it is an opt-in safeguard against
worker-side state nothing here bounds, at ~20 s per recreation.
"""

from __future__ import annotations

from collections.abc import Hashable, MutableMapping

from distributed import WorkerPlugin

#: How often each worker drops its span digests. Any value from ~10 s to a few
#: minutes keeps the 5-s digest walk below 1% of a worker's CPU; one minute
#: still leaves the dashboard the last minute of per-span metrics.
SPAN_DIGEST_TRIM_INTERVAL_S = 60


def is_span_digest(key: Hashable) -> bool:
    """Whether a digest key belongs to one computation's span.

    Task-execution metrics are keyed ``("execute", span_id, prefix, activity,
    unit)``. Entries with no span (``span_id`` ``None``) and the fixed-name
    digests are bounded, so they are kept.
    """
    return (
        isinstance(key, tuple)
        and len(key) > 1
        and key[0] == "execute"
        and key[1] is not None
    )


def trim_span_digests(digests: MutableMapping | None) -> int:
    """Delete the span digests from *digests*; returns how many were deleted.

    *digests* is a worker's ``Server.digests``, which is ``None`` when crick is
    not installed.
    """
    if not digests:
        return 0
    stale = [key for key in list(digests) if is_span_digest(key)]
    for key in stale:
        digests.pop(key, None)
    return len(stale)


class SpanDigestTrimPlugin(WorkerPlugin):
    """Drop each worker's span metric digests every *interval_s* seconds.

    Registered with ``client.register_plugin``; the scheduler re-applies it to
    any worker the nanny restarts. The trim runs on the worker's own event
    loop -- the thread that walks the digests -- so the two never overlap.
    """

    name = "moppy-span-digest-trim"

    def __init__(self, interval_s: float = SPAN_DIGEST_TRIM_INTERVAL_S) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self.interval_s = interval_s

    def setup(self, worker) -> None:
        from tornado.ioloop import PeriodicCallback

        callback = PeriodicCallback(
            lambda: trim_span_digests(worker.digests), self.interval_s * 1000
        )
        # The worker stops everything in periodic_callbacks when it closes.
        worker.periodic_callbacks[self.name] = callback
        callback.start()

    def teardown(self, worker) -> None:
        callback = worker.periodic_callbacks.pop(self.name, None)
        if callback is not None:
            callback.stop()


def cluster_recreate_due(partition_index: int, n_partitions: int, every) -> bool:
    """Whether to replace the Dask cluster after source partition *partition_index*.

    *every* is ``dask_recreate_every`` from the batch config: ``None`` (the
    default) never recreates; ``N`` recreates after every N partitions, but not
    after the last, when there is nothing left to run.
    """
    if every is None:
        return False
    if isinstance(every, bool) or not isinstance(every, int) or every <= 0:
        raise ValueError("dask_recreate_every must be a positive integer")
    return (partition_index + 1) % every == 0 and partition_index < n_partitions - 1
