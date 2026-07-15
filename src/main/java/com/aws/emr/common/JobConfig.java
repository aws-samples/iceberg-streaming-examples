package com.aws.emr.common;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Shared, self-documenting configuration and {@link SparkSession} factory for every Spark example
 * in this repository.
 *
 * <p>All examples take order-independent {@code key=value} arguments. Every key is optional and has
 * a sensible default, so running with no arguments keeps the classic local development experience.
 * Besides the run environment (local, local on S3/S3 Tables, EMR), the arguments also parameterise
 * the <b>Iceberg table layout</b> (copy-on-write vs merge-on-read, format-version 2 vs 3, parquet
 * vs ORC vs Avro files, object-storage layout), the <b>source payload format</b> (protobuf, Avro or
 * JSON) and the <b>write behaviour</b> (dedup strategy, compaction strategy, trigger). One job
 * class therefore covers what used to be many near-identical classes.
 *
 * <h2>Arguments</h2>
 *
 * <pre>
 *   runtime=local|emr             where Spark runs (default: local -&gt; master local[*])
 *   catalog=local|glue|s3tables   Iceberg catalog / storage (default: local)
 *   warehouse=&lt;path|s3 uri|arn&gt;   catalog warehouse (default 'warehouse' for local;
 *                                 an s3://... URI for glue; the table bucket ARN for s3tables)
 *   checkpoint=&lt;path|s3 uri&gt;      structured streaming checkpoint base dir (default: tmp/);
 *                                 every job derives a per-query path under it
 *   bootstrap=&lt;host:port,...&gt;     Kafka bootstrap servers (default: localhost:9092)
 *
 *   -- table layout knobs (consumed by {@link #createTableDdl}) --
 *   table=&lt;name&gt;                  target table name (job-specific default)
 *   mode=cow|mor                  copy-on-write or merge-on-read row-level operations
 *   fv=2|3                        Iceberg format-version (default 3; v3 =&gt; deletion vectors)
 *   fileformat=parquet|orc|avro   data/delete file format (default parquet)
 *   objectstorage=true|false      Iceberg object-storage layout for S3 (default false)
 *   fanout=true|false             Spark fanout writers (default true)
 *   manifestmerge=true|false      Iceberg manifest merge-on-commit (default true)
 *
 *   -- streaming behaviour knobs --
 *   source=proto|avro|json        Kafka payload format for the telemetry jobs (default proto)
 *   topic=&lt;name&gt;                  Kafka topic (default: telemetry-&lt;source&gt;; CDC jobs have fixed topics)
 *   dedup=none|batch|merge|watermark   dedup strategy (job-specific default; legacy true/false accepted)
 *   compaction=none|inline|scheduled   compaction strategy (default none; legacy true/false accepted)
 *   trigger=&lt;seconds&gt;|availablenow     micro-batch trigger (job-specific default); availablenow
 *                                      drains the topic and stops - a streaming job run as a batch
 *   watermark=&lt;duration&gt;          event-time watermark delay for dedup=watermark (default '120 seconds')
 *   startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint (default latest)
 *   maxOffsetsPerTrigger=&lt;n&gt;      cap records per micro-batch (default unset -&gt; drain all)
 *   failOnDataLoss=true|false     Kafka failOnDataLoss (default: Kafka default true)
 *
 *   -- misc --
 *   descriptor=&lt;path&gt;             protobuf descriptor (default src/main/protobuf/VehicleTelemetry.desc)
 *   avro=&lt;path&gt;                   Avro .avsc schema (default src/main/avro/VehicleTelemetry.avsc)
 *   shuffle=&lt;n&gt;                   spark.sql.shuffle.partitions initial value; AQE coalesces
 *                                 (default: 200 local / 800 cloud)
 *   region=&lt;aws-region&gt;           AWS region for the Glue Schema Registry examples (default eu-west-1)
 * </pre>
 *
 * <h2>Iceberg v3</h2>
 *
 * Tables default to Iceberg format-version 3 (deletion vectors, row lineage, ...), switchable per
 * run with {@code fv=2} so v2 positional deletes and v3 deletion vectors can be A/B tested with the
 * same class.
 *
 * @author acmanjon@amazon.com
 */
public final class JobConfig {

  /** Default Iceberg table format version used by every example. */
  public static final String FORMAT_VERSION = "3";

  /** Database / namespace used by every example. */
  public static final String DATABASE = "bigdata";

  private static final Logger log = LogManager.getLogger(JobConfig.class);

  /** Where Spark runs. */
  public enum Runtime {
    LOCAL,
    EMR
  }

  /** Which Iceberg catalog / storage backend to use. */
  public enum Catalog {
    LOCAL,
    GLUE,
    S3TABLES
  }

  /** Row-level operation mode of the target table. */
  public enum Mode {
    COW,
    MOR
  }

  /** Iceberg data/delete file format of the target table. */
  public enum FileFormat {
    PARQUET,
    ORC,
    AVRO
  }

  /** Payload format of the Kafka topic consumed by the telemetry jobs. */
  public enum Source {
    PROTO,
    AVRO,
    JSON
  }

  /**
   * Deduplication strategy.
   *
   * <ul>
   *   <li>{@code NONE} - append everything as-is.
   *   <li>{@code BATCH} - drop exact duplicates of the event identity inside each micro-batch (one
   *       cheap shuffle, no target scan). Duplicates that split across micro-batches survive.
   *   <li>{@code MERGE} - {@code BATCH} plus a MERGE INTO against the recent target partitions, so
   *       re-deliveries that arrive in a later micro-batch are suppressed too (bounded replay
   *       suppression, not a global upsert).
   *   <li>{@code WATERMARK} - event-time watermark + {@code dropDuplicatesWithinWatermark} (only
   *       supported by the native-writer job; state is bounded by the watermark, and events older
   *       than the watermark are dropped entirely - see the job docs).
   * </ul>
   */
  public enum Dedup {
    NONE,
    BATCH,
    MERGE,
    WATERMARK
  }

  /** Compaction strategy: none, inline every N batches, or a scheduled background thread. */
  public enum Compaction {
    NONE,
    INLINE,
    SCHEDULED
  }

  private final Runtime runtime;
  private final Catalog catalog;
  private final String warehouse;
  private final String checkpointLocation;
  private final String bootstrapServers;
  private final int shufflePartitions;

  /**
   * All parsed {@code key=value} arguments; every example-specific option is read through the typed
   * accessors below instead of each job re-parsing {@code args} by hand.
   */
  private final Map<String, String> rawArgs;

  private JobConfig(
      Runtime runtime,
      Catalog catalog,
      String warehouse,
      String checkpointLocation,
      String bootstrapServers,
      int shufflePartitions,
      Map<String, String> rawArgs) {
    this.runtime = runtime;
    this.catalog = catalog;
    this.warehouse = warehouse;
    this.checkpointLocation = checkpointLocation;
    this.bootstrapServers = bootstrapServers;
    this.shufflePartitions = shufflePartitions;
    this.rawArgs = Map.copyOf(rawArgs);
  }

  /**
   * Parse the {@code key=value} program arguments into a {@link JobConfig}. Unknown keys are logged
   * and ignored so a typo never silently changes behaviour.
   */
  public static JobConfig fromArgs(String[] args) {
    Map<String, String> kv = new HashMap<>();
    if (args != null) {
      for (String arg : args) {
        if (arg == null || arg.isBlank()) {
          continue;
        }
        int eq = arg.indexOf('=');
        if (eq <= 0) {
          log.warn("Ignoring argument '{}' - expected key=value form. See usage:\n{}", arg, usage());
          continue;
        }
        kv.put(arg.substring(0, eq).trim().toLowerCase(), arg.substring(eq + 1).trim());
      }
    }

    Runtime runtime =
        "emr".equalsIgnoreCase(kv.getOrDefault("runtime", "local")) ? Runtime.EMR : Runtime.LOCAL;
    Catalog catalog = parseCatalog(kv.getOrDefault("catalog", "local"));

    String defaultWarehouse = catalog == Catalog.LOCAL ? "warehouse" : null;
    String warehouse = kv.getOrDefault("warehouse", defaultWarehouse);
    if (catalog != Catalog.LOCAL && (warehouse == null || warehouse.isBlank())) {
      throw new IllegalArgumentException(
          "catalog="
              + catalog.name().toLowerCase()
              + " requires a warehouse= argument (an s3:// URI for glue or a table bucket ARN for"
              + " s3tables).\n"
              + usage());
    }

    int defaultShuffle = runtime == Runtime.LOCAL ? 200 : 800;

    JobConfig cfg =
        new JobConfig(
            runtime,
            catalog,
            warehouse,
            kv.getOrDefault("checkpoint", "tmp/"),
            kv.getOrDefault("bootstrap", "localhost:9092"),
            Integer.parseInt(kv.getOrDefault("shuffle", Integer.toString(defaultShuffle))),
            kv);
    cfg.log();
    return cfg;
  }

  private static Catalog parseCatalog(String value) {
    switch (value.toLowerCase()) {
      case "local":
        return Catalog.LOCAL;
      case "glue":
      case "glue_catalog":
        return Catalog.GLUE;
      case "s3tables":
      case "s3table":
      case "s3tablesbucket":
        return Catalog.S3TABLES;
      default:
        throw new IllegalArgumentException("Unknown catalog '" + value + "'.\n" + usage());
    }
  }

  /** @return the Spark catalog name that backs {@link #DATABASE} for the selected catalog. */
  public String catalogName() {
    switch (catalog) {
      case GLUE:
        return "glue_catalog";
      case S3TABLES:
        return "s3tablesbucket";
      case LOCAL:
      default:
        return "local";
    }
  }

  /**
   * Build a configured {@link SparkSession} for the selected runtime and catalog. Iceberg SQL
   * extensions and the default catalog are always wired up so the examples can use plain SQL such as
   * {@code USE bigdata} and unqualified table names.
   */
  public SparkSession buildSession(String appName) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName(appName)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            // Adaptive Query Execution: we set a deliberately high initial shuffle partition count
            // and let AQE coalesce the small post-shuffle partitions back down at runtime, so we no
            // longer have to hand-tune spark.sql.shuffle.partitions for every job/cluster size.
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", Integer.toString(shufflePartitions));

    if (runtime == Runtime.LOCAL) {
      builder.master("local[*]");
    }

    String cat = catalogName();
    switch (catalog) {
      case LOCAL:
        // Hadoop file based catalog for pure local development, plus a Hive backed session catalog.
        builder
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog." + cat, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + cat + ".type", "hadoop")
            .config("spark.sql.catalog." + cat + ".warehouse", warehouse);
        break;
      case GLUE:
        // AWS Glue Data Catalog with Iceberg S3FileIO for the data files in Amazon S3.
        builder
            .config("spark.sql.catalog." + cat, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + cat + ".catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog." + cat + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog." + cat + ".warehouse", warehouse)
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.iceberg.data-prefetch.enabled", "true");
        break;
      case S3TABLES:
        // Amazon S3 Tables managed Iceberg catalog. The warehouse is the table bucket ARN.
        builder
            .config("spark.sql.catalog." + cat, "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog." + cat + ".catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog")
            .config("spark.sql.catalog." + cat + ".warehouse", warehouse)
            .config("spark.sql.iceberg.data-prefetch.enabled", "true");
        break;
      default:
        throw new IllegalStateException("Unhandled catalog " + catalog);
    }

    builder.config("spark.sql.defaultCatalog", cat);
    return builder.getOrCreate();
  }

  /**
   * Build a throughput-tuned Kafka structured-streaming source for the given topic. Centralising the
   * options here keeps every consumer job consistent and easy to tune in one place.
   *
   * <p>Tuning applied: {@code minPartitions} = the shuffle partition count (more read parallelism
   * than Kafka partitions; AQE coalesces downstream), large fetch/poll sizes and a big socket
   * receive buffer so each poll pulls big batches. No {@code maxOffsetsPerTrigger} is set by
   * default, so each micro-batch drains all currently available data (maximum throughput); pass
   * {@code maxOffsetsPerTrigger=} to bound batch size/latency.
   *
   * @return the raw Kafka source {@code DataFrame} (key/value/topic/partition/offset/timestamp)
   */
  public Dataset<Row> kafkaStream(SparkSession spark, String topic) {
    org.apache.spark.sql.streaming.DataStreamReader reader =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("subscribe", topic)
            // Offsets to start from on a *fresh* checkpoint (default: latest). A showcase often wants
            // earliest for a deterministic replay of a pre-loaded topic; expose it as a knob.
            .option("startingOffsets", startingOffsets())
            .option("minPartitions", Integer.toString(shufflePartitions))
            .option("kafka.fetch.min.bytes", "1048576") // 1 MiB
            .option("kafka.fetch.max.bytes", "104857600") // 100 MiB per fetch
            .option("kafka.max.partition.fetch.bytes", "10485760") // 10 MiB per partition
            .option("kafka.max.poll.records", "50000")
            .option("kafka.receive.buffer.bytes", "16777216"); // 16 MiB socket buffer
    String maxOffsets = arg("maxoffsetspertrigger", null);
    if (maxOffsets != null) {
      reader = reader.option("maxOffsetsPerTrigger", maxOffsets);
    }
    // failOnDataLoss defaults to Kafka's own default (true). On a demo topic with short retention,
    // set failOnDataLoss=false so an aged-out offset does not kill the query.
    String failOnDataLoss = arg("failondataloss", null);
    if (failOnDataLoss != null) {
      reader = reader.option("failOnDataLoss", failOnDataLoss);
    }
    return reader.load();
  }

  // --------------------------------------------------------------- typed argument accessors

  /**
   * Return the raw value of a {@code key=value} argument (case-insensitive key), or {@code def} if
   * it was not supplied.
   */
  public String arg(String key, String def) {
    String value = rawArgs.get(key.toLowerCase());
    return (value == null || value.isBlank()) ? def : value;
  }

  /** @return a boolean {@code key=value} argument, or {@code def} if not supplied. */
  public boolean argBool(String key, boolean def) {
    String value = arg(key, null);
    return value == null ? def : Boolean.parseBoolean(value);
  }

  /** Target table name ({@code table=}, default {@code def}). */
  public String table(String def) {
    return arg("table", def);
  }

  /** Iceberg format version ({@code fv=2|3}, default {@code def}). */
  public String formatVersion(String def) {
    String fv = arg("fv", def);
    if (!"2".equals(fv) && !"3".equals(fv)) {
      throw new IllegalArgumentException("fv must be 2 or 3, got: " + fv);
    }
    return fv;
  }

  /** Row-level operation mode ({@code mode=cow|mor}, default {@code def}). */
  public Mode mode(Mode def) {
    String v = arg("mode", null);
    if (v == null) {
      return def;
    }
    switch (v.toLowerCase()) {
      case "cow":
      case "copy-on-write":
        return Mode.COW;
      case "mor":
      case "merge-on-read":
        return Mode.MOR;
      default:
        throw new IllegalArgumentException("mode must be cow or mor, got: " + v);
    }
  }

  /** Iceberg data/delete file format ({@code fileformat=parquet|orc|avro}, default {@code def}). */
  public FileFormat fileFormat(FileFormat def) {
    String v = arg("fileformat", null);
    if (v == null) {
      return def;
    }
    try {
      return FileFormat.valueOf(v.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("fileformat must be parquet, orc or avro, got: " + v);
    }
  }

  /** Iceberg object-storage layout toggle ({@code objectstorage=true|false}, default {@code def}). */
  public boolean objectStorage(boolean def) {
    return argBool("objectstorage", def);
  }

  /** Kafka payload format of the telemetry topics ({@code source=proto|avro|json}, default proto). */
  public Source source() {
    String v = arg("source", "proto");
    try {
      return Source.valueOf(v.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("source must be proto, avro or json, got: " + v);
    }
  }

  /** Kafka topic ({@code topic=}, default {@code telemetry-<source>}). */
  public String topic() {
    return arg("topic", "telemetry-" + source().name().toLowerCase());
  }

  /**
   * Deduplication strategy ({@code dedup=none|batch|merge|watermark}, default {@code def}). The
   * legacy boolean values are still accepted: {@code true} maps to MERGE, {@code false} to NONE.
   */
  public Dedup dedup(Dedup def) {
    String v = arg("dedup", null);
    if (v == null) {
      return def;
    }
    switch (v.toLowerCase()) {
      case "none":
      case "false":
        return Dedup.NONE;
      case "batch":
        return Dedup.BATCH;
      case "merge":
      case "true":
        return Dedup.MERGE;
      case "watermark":
        return Dedup.WATERMARK;
      default:
        throw new IllegalArgumentException(
            "dedup must be none, batch, merge or watermark, got: " + v);
    }
  }

  /**
   * Compaction strategy ({@code compaction=none|inline|scheduled}, default {@code def}). The legacy
   * boolean values are still accepted: {@code true} maps to INLINE, {@code false} to NONE.
   */
  public Compaction compactionMode(Compaction def) {
    String v = arg("compaction", null);
    if (v == null) {
      return def;
    }
    switch (v.toLowerCase()) {
      case "none":
      case "false":
        return Compaction.NONE;
      case "inline":
      case "true":
        return Compaction.INLINE;
      case "scheduled":
        return Compaction.SCHEDULED;
      default:
        throw new IllegalArgumentException(
            "compaction must be none, inline or scheduled, got: " + v);
    }
  }

  /**
   * Streaming trigger ({@code trigger=<seconds>|availablenow}, default {@code defaultSeconds}).
   * {@code availablenow} turns the streaming job into a catch-up/backfill batch: it drains
   * everything available on the topic in bounded micro-batches and stops.
   */
  public Trigger trigger(int defaultSeconds) {
    String v = arg("trigger", null);
    if (v == null) {
      return Trigger.ProcessingTime(defaultSeconds, TimeUnit.SECONDS);
    }
    if ("availablenow".equalsIgnoreCase(v)) {
      return Trigger.AvailableNow();
    }
    try {
      return Trigger.ProcessingTime(Long.parseLong(v), TimeUnit.SECONDS);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("trigger must be a number of seconds or 'availablenow', got: " + v);
    }
  }

  /** Event-time watermark delay for {@code dedup=watermark} ({@code watermark=}, default 120s). */
  public String watermarkDelay() {
    return arg("watermark", "120 seconds");
  }

  /** Spark fanout-writer toggle ({@code fanout=true|false}, default {@code def}). */
  public boolean fanout(boolean def) {
    return argBool("fanout", def);
  }

  /** Iceberg automatic manifest merge-on-commit toggle ({@code manifestmerge=true|false}). */
  public boolean manifestMerge(boolean def) {
    return argBool("manifestmerge", def);
  }

  /** Kafka {@code startingOffsets} for a fresh checkpoint ({@code startingoffsets=}, default latest). */
  public String startingOffsets() {
    return arg("startingoffsets", "latest");
  }

  /** AWS region for the Glue Schema Registry examples ({@code region=}, default eu-west-1). */
  public String region() {
    return arg("region", "eu-west-1");
  }

  // --------------------------------------------------------------- table DDL builder

  /**
   * Build the {@code TBLPROPERTIES} map for the selected table knobs ({@code mode=}, {@code fv=},
   * {@code fileformat=}, {@code objectstorage=}, {@code fanout=}, {@code manifestmerge=}). This is
   * the single place the property recipe lives, so every example creates tables the same way and a
   * knob change never has to be repeated across jobs.
   *
   * <p>Format-specific compression properties are only emitted for the format actually in use (no
   * parquet tuning on an ORC table). Merge-on-read additionally hash-distributes the row-level
   * operation writes so output files stay clustered. Metadata janitor properties keep the
   * {@code metadata.json} log bounded on long-running streaming writers, and commit retries are
   * generous because streaming writes, MERGEs and compaction all race for optimistic commits.
   */
  public Map<String, String> tablePropertiesMap(Mode defaultMode, Map<String, String> overrides) {
    Mode mode = mode(defaultMode);
    FileFormat format = fileFormat(FileFormat.PARQUET);
    String fmt = format.name().toLowerCase();

    Map<String, String> p = new LinkedHashMap<>();
    p.put("table_type", "ICEBERG");
    p.put("format-version", formatVersion(FORMAT_VERSION));
    p.put("write.format.default", fmt);
    p.put("write.delete.format.default", fmt);
    switch (format) {
      case PARQUET:
        p.put("write.parquet.compression-codec", "zstd");
        p.put("write.parquet.compression-level", "7");
        p.put("write.parquet.row-group-size-bytes", "134217728"); // 128 MiB
        p.put("write.parquet.page-size-bytes", "1048576"); // 1 MiB
        break;
      case ORC:
        p.put("write.orc.compression-codec", "zstd");
        break;
      case AVRO:
        p.put("write.avro.compression-codec", "zstd");
        break;
    }
    String rowLevelMode = mode == Mode.MOR ? "merge-on-read" : "copy-on-write";
    p.put("write.delete.mode", rowLevelMode);
    p.put("write.update.mode", rowLevelMode);
    p.put("write.merge.mode", rowLevelMode);
    if (mode == Mode.MOR) {
      p.put("write.distribution-mode", "hash");
      p.put("write.delete.distribution-mode", "hash");
      p.put("write.update.distribution-mode", "hash");
      p.put("write.merge.distribution-mode", "hash");
    }
    if (objectStorage(false)) {
      p.put("write.object-storage.enabled", "true");
    }
    p.put("write.spark.fanout.enabled", Boolean.toString(fanout(true)));
    p.put("write.target-file-size-bytes", "536870912"); // 512 MiB
    // Keep the metadata.json log bounded on long-running streaming writers.
    p.put("write.metadata.delete-after-commit.enabled", "true");
    p.put("write.metadata.previous-versions-max", "100");
    // Streaming writes, MERGEs and compaction all commit optimistically against the same table:
    // give losing commits room to retry instead of failing the job.
    p.put("commit.retry.num-retries", "20");
    p.put("commit.retry.min-wait-ms", "250");
    p.put("commit.retry.max-wait-ms", "60000");
    p.put("commit.manifest-merge.enabled", Boolean.toString(manifestMerge(true)));
    p.put("compatibility.snapshot-id-inheritance.enabled", "true");
    if (overrides != null) {
      p.putAll(overrides);
    }
    return p;
  }

  /**
   * Render a full {@code CREATE TABLE IF NOT EXISTS} statement for the given columns and partition
   * spec, with {@code TBLPROPERTIES} derived from the table knobs (see {@link #tablePropertiesMap}).
   *
   * @param table table name (unqualified or fully qualified)
   * @param columnsDdl the column list, e.g. {@code "id bigint, ts timestamp"}
   * @param partitionDdl the partition transform list, e.g. {@code "hours(ts), bucket(16, id)"}
   * @param defaultMode the job's default row-level mode when {@code mode=} is not supplied
   * @param overrides extra/override table properties for job-specific needs (may be empty)
   */
  public String createTableDdl(
      String table,
      String columnsDdl,
      String partitionDdl,
      Mode defaultMode,
      Map<String, String> overrides) {
    String props =
        tablePropertiesMap(defaultMode, overrides).entrySet().stream()
            .map(e -> "'" + e.getKey() + "'='" + e.getValue() + "'")
            .collect(Collectors.joining(",\n            "));
    return String.format(
        """
        CREATE TABLE IF NOT EXISTS %1$s
              (%2$s)
              PARTITIONED BY (%3$s)
              TBLPROPERTIES (
            %4$s )
        """,
        table, columnsDdl, partitionDdl, props);
  }

  // --------------------------------------------------------------- checkpoints

  /**
   * Derive a per-query checkpoint path under {@link #checkpointLocation}, so multiple streaming
   * examples launched with the same (or default) {@code checkpoint=} do not collide on incompatible
   * state. Every streaming query must have its own checkpoint; a checkpoint encodes the query's
   * schema and offsets and cannot be shared between different queries.
   */
  public String checkpointFor(String queryName) {
    String base = checkpointLocation.endsWith("/")
        ? checkpointLocation.substring(0, checkpointLocation.length() - 1)
        : checkpointLocation;
    String safe = queryName.replaceAll("[^a-zA-Z0-9_.-]", "_");
    return base + "/" + safe;
  }

  private void log() {
    log.warn(
        "JobConfig -> runtime={}, catalog={} (spark catalog '{}'), warehouse={}, checkpoint={}, "
            + "bootstrap={}, shuffle.partitions={}, extra args={}",
        runtime.name().toLowerCase(),
        catalog.name().toLowerCase(),
        catalogName(),
        warehouse,
        checkpointLocation,
        bootstrapServers,
        shufflePartitions,
        rawArgs);
    if (runtime == Runtime.LOCAL && catalog == Catalog.LOCAL) {
      log.warn(
          "Running in pure local mode. Remember to clean the checkpoint dir '{}' if you want a"
              + " clean restart.",
          checkpointLocation);
    }
  }

  /** @return a human readable usage string describing every supported argument. */
  public static String usage() {
    return "Usage: [key=value ...]\n"
        + "  runtime=local|emr             where Spark runs (default: local)\n"
        + "  catalog=local|glue|s3tables   Iceberg catalog / storage (default: local)\n"
        + "  warehouse=<path|s3 uri|arn>   catalog warehouse (default 'warehouse' for local;\n"
        + "                                s3://... for glue; table bucket ARN for s3tables)\n"
        + "  checkpoint=<path|s3 uri>      streaming checkpoint base dir (default: tmp/)\n"
        + "  bootstrap=<host:port,...>     Kafka bootstrap servers (default: localhost:9092)\n"
        + "  table=<name>                  target table name (job-specific default)\n"
        + "  mode=cow|mor                  copy-on-write | merge-on-read (job-specific default)\n"
        + "  fv=2|3                        Iceberg format-version (default 3)\n"
        + "  fileformat=parquet|orc|avro   Iceberg data/delete file format (default parquet)\n"
        + "  objectstorage=true|false      Iceberg object-storage layout (default false)\n"
        + "  fanout=true|false             Spark fanout writers (default true)\n"
        + "  manifestmerge=true|false      Iceberg manifest merge-on-commit (default true)\n"
        + "  source=proto|avro|json        Kafka payload format (default proto)\n"
        + "  topic=<name>                  Kafka topic (default: telemetry-<source>)\n"
        + "  dedup=none|batch|merge|watermark  dedup strategy (job-specific default)\n"
        + "  compaction=none|inline|scheduled  compaction strategy (default none)\n"
        + "  trigger=<seconds>|availablenow    micro-batch trigger (job-specific default)\n"
        + "  watermark=<duration>          watermark delay for dedup=watermark (default '120 seconds')\n"
        + "  startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint\n"
        + "  maxOffsetsPerTrigger=<n>      cap records per micro-batch (default: unset -> drain all)\n"
        + "  failOnDataLoss=true|false     Kafka failOnDataLoss (default: Kafka default true)\n"
        + "  descriptor=<path>             protobuf descriptor (default src/main/protobuf/VehicleTelemetry.desc)\n"
        + "  avro=<path>                   Avro .avsc schema (default src/main/avro/VehicleTelemetry.avsc)\n"
        + "  shuffle=<n>                   spark.sql.shuffle.partitions (default: 200 local / 800 cloud)\n"
        + "  region=<aws-region>           Glue Schema Registry region (default eu-west-1)\n"
        + "Examples:\n"
        + "  (no args)                                             # local dev, hadoop catalog\n"
        + "  mode=mor fv=2 fileformat=orc dedup=merge              # MoR v2 ORC with MERGE dedup\n"
        + "  catalog=glue warehouse=s3://bucket/warehouse ...      # local Spark, data in S3 via Glue\n"
        + "  catalog=s3tables warehouse=arn:aws:s3tables:...  ...  # local Spark, S3 Tables bucket\n"
        + "  runtime=emr catalog=glue warehouse=s3://bucket/wh ... # EMR, data in S3 via Glue";
  }

  public Runtime runtime() {
    return runtime;
  }

  public Catalog catalog() {
    return catalog;
  }

  public String warehouse() {
    return warehouse;
  }

  public String checkpointLocation() {
    return checkpointLocation;
  }

  public String bootstrapServers() {
    return bootstrapServers;
  }

  /** Protobuf descriptor file path ({@code descriptor=}). */
  public String protoDescriptor() {
    return arg("descriptor", "src/main/protobuf/VehicleTelemetry.desc");
  }

  /** Avro schema (.avsc) file path ({@code avro=}). */
  public String avroSchemaFile() {
    return arg("avro", "src/main/avro/VehicleTelemetry.avsc");
  }

  public int shufflePartitions() {
    return shufflePartitions;
  }
}
