package com.aws.emr.common;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Shared, self-documenting configuration and {@link SparkSession} factory for every Spark example
 * in this repository.
 *
 * <p>Historically each example duplicated a large {@code if (args.length == N)} block to build the
 * Spark session for the different environments. That was error prone (there were copy/paste bugs
 * such as a warehouse being set to a class name, or a corrupted config key). This helper replaces
 * all of those blocks with a single, consistent implementation that supports the three run
 * scenarios we care about:
 *
 * <ol>
 *   <li><b>Local</b> &mdash; Spark runs in {@code local[*]} against a Hadoop file based Iceberg
 *       catalog under {@code ./warehouse}. This is the default when no arguments are supplied.
 *   <li><b>Local on top of Amazon S3 / S3 Tables</b> &mdash; Spark still runs in {@code local[*]}
 *       (great for debugging) but reads/writes Iceberg data in Amazon S3 through the AWS Glue Data
 *       Catalog ({@code catalog=glue}) or in an Amazon S3 Tables bucket ({@code catalog=s3tables}).
 *   <li><b>Amazon EMR on S3 / S3 Tables</b> &mdash; the same catalogs as above but with
 *       {@code runtime=emr}, so the master is inferred from the cluster instead of being forced to
 *       {@code local[*]}.
 * </ol>
 *
 * <h2>Arguments</h2>
 *
 * Arguments are passed as order-independent {@code key=value} pairs. Every key is optional and has a
 * sensible default, so running with no arguments keeps the classic local development experience.
 *
 * <pre>
 *   runtime=local|emr           where Spark runs (default: local)
 *   catalog=local|glue|s3tables Iceberg catalog / storage (default: local)
 *   warehouse=&lt;path|s3 uri|arn&gt;  catalog warehouse. Default 'warehouse' for local.
 *                               For glue use an s3://... URI, for s3tables the table bucket ARN
 *                               (arn:aws:s3tables:&lt;region&gt;:&lt;acct&gt;:bucket/&lt;name&gt;).
 *   checkpoint=&lt;path|s3 uri&gt;    structured streaming checkpoint dir (default: tmp/)
 *   bootstrap=&lt;host:port,...&gt;    Kafka bootstrap servers (default: localhost:9092)
 *   descriptor=&lt;path&gt;           protobuf descriptor file (default: Employee.desc)
 *   avro=&lt;path&gt;                 Avro .avsc schema file (default: ./src/main/avro/Employee.avsc)
 *   dedup=true|false            enable deduplication (default varies per job)
 *   compaction=true|false       enable async/periodic compaction (default: false)
 *   shuffle=&lt;n&gt;                 spark.sql.shuffle.partitions initial value, AQE coalesces (default: 200 local, 800 cloud)
 * </pre>
 *
 * <h2>Iceberg v3</h2>
 *
 * All tables created by the examples are Iceberg format-version 3 tables (see
 * {@link #FORMAT_VERSION}). v3 became production ready with Apache Iceberg 1.11.0 and brings
 * deletion vectors (used automatically by the merge-on-read examples), row lineage, the VARIANT
 * type, default column values and more.
 *
 * @author acmanjon@amazon.com
 */
public final class JobConfig {

  /** Iceberg table format version used by every example. */
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

  private final Runtime runtime;
  private final Catalog catalog;
  private final String warehouse;
  private final String checkpointLocation;
  private final String bootstrapServers;
  private final String protoDescriptor;
  private final String avroSchemaFile;
  private final boolean removeDuplicates;
  private final boolean compaction;
  private final int shufflePartitions;

  /**
   * All parsed {@code key=value} arguments, kept so example-specific options (such as {@code table},
   * {@code fv}, {@code fanout}, {@code manifestmerge} or the streaming knobs below) can be read
   * through the typed accessors instead of every job re-parsing {@code args} by hand.
   */
  private final Map<String, String> rawArgs;

  private JobConfig(
      Runtime runtime,
      Catalog catalog,
      String warehouse,
      String checkpointLocation,
      String bootstrapServers,
      String protoDescriptor,
      String avroSchemaFile,
      boolean removeDuplicates,
      boolean compaction,
      int shufflePartitions,
      Map<String, String> rawArgs) {
    this.runtime = runtime;
    this.catalog = catalog;
    this.warehouse = warehouse;
    this.checkpointLocation = checkpointLocation;
    this.bootstrapServers = bootstrapServers;
    this.protoDescriptor = protoDescriptor;
    this.avroSchemaFile = avroSchemaFile;
    this.removeDuplicates = removeDuplicates;
    this.compaction = compaction;
    this.shufflePartitions = shufflePartitions;
    this.rawArgs = Map.copyOf(rawArgs);
  }

  /**
   * Parse the {@code key=value} program arguments into a {@link JobConfig}. Unknown keys are logged
   * and ignored so a typo never silently changes behaviour.
   *
   * @param args the raw program arguments
   * @return an immutable configuration with defaults applied
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

    Runtime runtime = "emr".equalsIgnoreCase(kv.getOrDefault("runtime", "local")) ? Runtime.EMR : Runtime.LOCAL;
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
            kv.getOrDefault("descriptor", "Employee.desc"),
            kv.getOrDefault("avro", "./src/main/avro/Employee.avsc"),
            Boolean.parseBoolean(kv.getOrDefault("dedup", "false")),
            Boolean.parseBoolean(kv.getOrDefault("compaction", "false")),
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
   *
   * @param appName the Spark application name
   * @return a ready to use {@link SparkSession}
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
   * <p>Tuning applied:
   *
   * <ul>
   *   <li>{@code minPartitions} = the shuffle partition count, so the source is split into many more
   *       Spark tasks than there are Kafka partitions (more read parallelism; AQE then coalesces the
   *       downstream shuffle).
   *   <li>large {@code fetch.max.bytes} / {@code max.partition.fetch.bytes} / {@code max.poll.records}
   *       and a bigger socket {@code receive.buffer.bytes} so each poll pulls big batches.
   *   <li>{@code fetch.min.bytes} of 1 MiB so the broker returns fuller batches (bounded by the
   *       default {@code fetch.max.wait.ms}).
   * </ul>
   *
   * No {@code maxOffsetsPerTrigger} is set, so each micro-batch drains all currently available data
   * (maximum throughput); set it per job if you need rate limiting.
   *
   * @param spark the Spark session
   * @param topic the Kafka topic to subscribe to
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
    // Optional rate limit: cap the records pulled per micro-batch. Unset by default (drain all
    // available data for maximum throughput); set maxOffsetsPerTrigger= to bound batch size/latency.
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
   * it was not supplied. This is how example-specific options are read without every job re-parsing
   * {@code args}.
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

  /** Target table name for the parameterised examples ({@code table=}, default {@code def}). */
  public String table(String def) {
    return arg("table", def);
  }

  /** Iceberg format version for the parameterised examples ({@code fv=2|3}, default {@code def}). */
  public String formatVersion(String def) {
    return arg("fv", def);
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

  // --------------------------------------------------------------- checkpoints

  /**
   * Derive a per-query checkpoint path under {@link #checkpointLocation}, so multiple streaming
   * examples launched with the same (or default) {@code checkpoint=} do not collide on incompatible
   * state. Every streaming query must have its own checkpoint; a checkpoint encodes the query's
   * schema and offsets and cannot be shared between different queries.
   *
   * @param queryName a stable, unique name for the streaming query
   * @return {@code <checkpointLocation>/<queryName>}
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
            + "bootstrap={}, descriptor={}, avro={}, dedup={}, compaction={}, shuffle.partitions={}. "
            + "All tables are created as Iceberg format-version {} (v3).",
        runtime.name().toLowerCase(),
        catalog.name().toLowerCase(),
        catalogName(),
        warehouse,
        checkpointLocation,
        bootstrapServers,
        protoDescriptor,
        avroSchemaFile,
        removeDuplicates,
        compaction,
        shufflePartitions,
        FORMAT_VERSION);
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
        + "  runtime=local|emr           where Spark runs (default: local)\n"
        + "  catalog=local|glue|s3tables Iceberg catalog / storage (default: local)\n"
        + "  warehouse=<path|s3 uri|arn> catalog warehouse (default 'warehouse' for local;\n"
        + "                              s3://... for glue; table bucket ARN for s3tables)\n"
        + "  checkpoint=<path|s3 uri>    streaming checkpoint dir (default: tmp/)\n"
        + "  bootstrap=<host:port,...>   Kafka bootstrap servers (default: localhost:9092)\n"
        + "  descriptor=<path>           protobuf descriptor file (default: Employee.desc)\n"
        + "  avro=<path>                 Avro .avsc schema file (default: ./src/main/avro/Employee.avsc)\n"
        + "  dedup=true|false            enable deduplication (default: false)\n"
        + "  compaction=true|false       enable periodic compaction (default: false)\n"
        + "  shuffle=<n>                 spark.sql.shuffle.partitions initial value, AQE coalesces (default: 200 local / 800 cloud)\n"
        + "  startingOffsets=latest|earliest|{json}  Kafka start offsets on a fresh checkpoint (default: latest)\n"
        + "  maxOffsetsPerTrigger=<n>    cap records per micro-batch (default: unset -> drain all)\n"
        + "  failOnDataLoss=true|false   Kafka failOnDataLoss (default: Kafka default true)\n"
        + "  table=<name>                target table for parameterised examples (job-specific default)\n"
        + "  fv=2|3                      Iceberg format-version for parameterised examples (job-specific default)\n"
        + "  fanout=true|false           Spark fanout writers (job-specific default)\n"
        + "  manifestmerge=true|false    Iceberg manifest merge-on-commit (job-specific default)\n"
        + "Examples:\n"
        + "  (no args)                                             # local dev, hadoop catalog\n"
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

  public String protoDescriptor() {
    return protoDescriptor;
  }

  public String avroSchemaFile() {
    return avroSchemaFile;
  }

  public boolean removeDuplicates() {
    return removeDuplicates;
  }

  public boolean compaction() {
    return compaction;
  }

  public int shufflePartitions() {
    return shufflePartitions;
  }
}
