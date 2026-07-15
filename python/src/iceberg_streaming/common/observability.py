"""Structured Streaming progress observability.

Python counterpart of ``com.aws.emr.common.StreamingProgressListener``. Registers a
:class:`pyspark.sql.streaming.listener.StreamingQueryListener` that logs a concise, grep-friendly,
``key=value`` line of progress metrics after every micro-batch (batch id, input rows, input/processed
rows-per-second, trigger/addBatch durations). These are the numbers you want to compare two runs
(for example Iceberg v2 vs v3) objectively instead of eyeballing the Spark UI.

Usage (after starting the query)::

    from iceberg_streaming.common.observability import attach_progress_listener
    attach_progress_listener(spark)
"""

from __future__ import annotations

import logging

log = logging.getLogger("iceberg_streaming.observability")

try:
    from pyspark.sql.streaming.listener import StreamingQueryListener

    class StreamingProgressListener(StreamingQueryListener):
        """Logs per-batch Structured Streaming progress metrics."""

        def onQueryStarted(self, event) -> None:  # noqa: N802 (PySpark API name)
            log.warning("[stream-progress] started name=%s id=%s", event.name, event.id)

        def onQueryProgress(self, event) -> None:  # noqa: N802
            p = event.progress
            duration = getattr(p, "durationMs", {}) or {}
            log.warning(
                "[stream-progress] name=%s batchId=%s inputRows=%s inputRps=%s processedRps=%s "
                "triggerExecutionMs=%s addBatchMs=%s numSources=%s",
                getattr(p, "name", None),
                getattr(p, "batchId", None),
                getattr(p, "numInputRows", None),
                _fmt(getattr(p, "inputRowsPerSecond", None)),
                _fmt(getattr(p, "processedRowsPerSecond", None)),
                duration.get("triggerExecution"),
                duration.get("addBatch"),
                len(getattr(p, "sources", []) or []),
            )

        # Present in newer PySpark; harmless if never called.
        def onQueryIdle(self, event) -> None:  # noqa: N802
            pass

        def onQueryTerminated(self, event) -> None:  # noqa: N802
            log.warning(
                "[stream-progress] terminated id=%s exception=%s",
                event.id,
                getattr(event, "exception", None),
            )

    def attach_progress_listener(spark) -> None:
        """Register a :class:`StreamingProgressListener` on the session's streaming manager."""
        spark.streams.addListener(StreamingProgressListener())
        log.warning("[stream-progress] listener attached")

except Exception as exc:  # pragma: no cover - only on an unexpected PySpark build
    _IMPORT_ERROR = exc

    def attach_progress_listener(spark) -> None:  # type: ignore[misc]
        log.warning("[stream-progress] listener unavailable on this PySpark build: %s", _IMPORT_ERROR)


def _fmt(value) -> str:
    if value is None:
        return "NaN"
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return str(value)
