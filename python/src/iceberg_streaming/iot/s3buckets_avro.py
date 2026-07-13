"""MoR S3/S3 Tables ingest, Avro files, table ``employee_avro_uncompacted`` (see s3buckets_job)."""

from __future__ import annotations

import sys

from iceberg_streaming.iot import s3buckets_job


def main(argv: list[str] | None = None) -> None:
    s3buckets_job.run(
        argv if argv is not None else sys.argv[1:],
        "PySparkS3BucketsAvro",
        "employee_avro_uncompacted",
        "avro",
    )


if __name__ == "__main__":
    main()
