package com.aws.emr.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

/**
 * A {@link StreamingQueryListener} that logs a concise, machine-parseable line of Structured
 * Streaming progress metrics after every micro-batch: batch id, input rows, input/processed
 * rows-per-second, batch duration and the phase breakdown (addBatch, queryPlanning, walCommit, ...).
 * These are the numbers you want when comparing two runs (for example Iceberg v2 vs v3) objectively
 * instead of eyeballing the Spark UI.
 *
 * <p>Attach it once, after starting the query(ies):
 *
 * <pre>{@code
 * StreamingProgressListener.attach(spark);
 * }</pre>
 *
 * <p>The line is prefixed with {@code [stream-progress]} so it is easy to grep out of the driver log,
 * and every field is {@code key=value} so it can be parsed into CSV/JSON downstream.
 */
public final class StreamingProgressListener extends StreamingQueryListener {

  private static final Logger log = LogManager.getLogger(StreamingProgressListener.class);

  private StreamingProgressListener() {}

  /** Register a fresh listener on the given session's streaming manager. */
  public static void attach(SparkSession spark) {
    spark.streams().addListener(new StreamingProgressListener());
    log.warn("[stream-progress] listener attached");
  }

  @Override
  public void onQueryStarted(QueryStartedEvent event) {
    log.warn("[stream-progress] started name={} id={}", event.name(), event.id());
  }

  @Override
  public void onQueryProgress(QueryProgressEvent event) {
    StreamingQueryProgress p = event.progress();
    // durationMs is a java.util.Map<String,Long> of phase -> millis (may be empty on idle batches).
    Object addBatch = p.durationMs() != null ? p.durationMs().get("addBatch") : null;
    Object triggerExec = p.durationMs() != null ? p.durationMs().get("triggerExecution") : null;
    log.warn(
        "[stream-progress] name={} batchId={} inputRows={} inputRps={} processedRps={} "
            + "triggerExecutionMs={} addBatchMs={} numSources={}",
        p.name(),
        p.batchId(),
        p.numInputRows(),
        fmt(p.inputRowsPerSecond()),
        fmt(p.processedRowsPerSecond()),
        triggerExec,
        addBatch,
        p.sources() == null ? 0 : p.sources().length);
  }

  @Override
  public void onQueryTerminated(QueryTerminatedEvent event) {
    log.warn(
        "[stream-progress] terminated id={} exception={}",
        event.id(),
        event.exception().isDefined() ? event.exception().get() : "none");
  }

  private static String fmt(double d) {
    return Double.isNaN(d) ? "NaN" : String.format("%.1f", d);
  }
}
