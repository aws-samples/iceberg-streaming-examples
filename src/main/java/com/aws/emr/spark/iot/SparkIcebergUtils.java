package com.aws.emr.spark.iot;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkIcebergUtils {

    private static final Logger log = LogManager.getLogger(SparkIcebergUtils.class);

    public static void main(String[] args) throws IOException, TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaIoTProtoBufDescriptor2Iceberg")
                .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type","hive")
                .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type","hadoop")
                .config("spark.sql.shuffle.partitions","50") //we are not using AQE then we need to tune this
                .config("spark.sql.catalog.local.warehouse","warehouse")
                .config("spark.sql.defaultCatalog","local")
                .getOrCreate();
        //in production you should configure this via env or spark configs
        spark.sparkContext().setLogLevel("WARN");

        // remember to config the tables or look the defaults to see what is going to be deleted
        spark.sql(
                """
                        CALL system.expire_snapshots(
                         table => 'employee'
                        )
                        """).show();
    }

}

