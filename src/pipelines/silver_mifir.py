"""ESMA MiFIR Transaction-Reporting Silver Layer.

Domain-driven silver layer on top of bronze ``mifir_raw``
(auth.016.001.01_ESMAUG_Reporting). Three tables:

* ``transaction`` — wide-flat fact table, one row per ``<Tx>`` element
  with ``action_type`` discriminator (NEW / CXL). ~135 scalars +
  ~15 array columns covering identification, buyer/seller flat,
  trade details, instrument + 6 underlying-instrument prefix groups,
  investment-decision person, executing person, additional attributes,
  audit.
* ``transaction_party`` — unified explode of Buyr.AcctOwnr +
  Buyr.DcsnMakr + Sellr.AcctOwnr + Sellr.DcsnMakr with side and
  party_role discriminators. ~18 cols.
* ``submission_file`` — MiFIR-specific envelope including UVHeader
  (UnaVista vendor wrapper) + full BizAppHeader (AppHdr top-level +
  Sender/Recipient OrgId + FIId blocks + 135-leaf Rltd related-
  message mirror). ~270 cols.

All inputs are supplied via ``spark.conf`` — see the MiFIR silver
pipeline ``configuration`` block in
``resources/bundle.mifir_resources.yml``.

Reference: docs/superpowers/specs/2026-05-12-mifir-silver-design.md
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --------------------------------------------------------------------------
# Pipeline configuration (set in resources/bundle.mifir_resources.yml under
# resources.pipelines.mifir_silver_pipeline.configuration).
# --------------------------------------------------------------------------

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema", RAW_SCHEMA)
BRONZE_TABLE_NAME = spark.conf.get("bronze_table")
REGULATION = spark.conf.get("regulation", "MIFIR")
ENABLE_FILENAME_REGEX = spark.conf.get("enable_filename_regex", "true").lower() == "true"

TBL_BRONZE = f"{CATALOG}.{RAW_SCHEMA}.{BRONZE_TABLE_NAME}"
TBL_TRANSACTION = f"{CATALOG}.{SILVER_SCHEMA}.transaction"
TBL_TRANSACTION_PARTY = f"{CATALOG}.{SILVER_SCHEMA}.transaction_party"
TBL_SUBMISSION_FILE = f"{CATALOG}.{SILVER_SCHEMA}.submission_file"


# --------------------------------------------------------------------------
# Filename regex extraction (customer-replaceable).
#
# Default MiFIR convention (e.g., 9795_20250729154019_3_sample_data.xml):
#   <client_id>_<YYYYMMDDhhmmss>_<sequence>_<rest>.xml
#
# TODO (customer): customers with a different filename convention should
# REPLACE THIS FUNCTION rather than editing the @dp.table definitions.
# The four output column names must stay the same so downstream consumers
# keep working; the extraction logic inside is yours to redefine.
#
# Set ENABLE_FILENAME_REGEX=false to skip extraction entirely (columns
# emit NULL while preserving the schema).
# --------------------------------------------------------------------------

_MIFIR_CLIENT_ID_PATTERN = r"^(\d+)_"
_MIFIR_TIMESTAMP_PATTERN = r"^\d+_(\d{14})_"
_MIFIR_SEQUENCE_PATTERN = r"^\d+_\d{14}_(\d+)_"


def _add_filename_regex_columns(df: DataFrame) -> DataFrame:
    """Add MiFIR filename-derived columns to a DataFrame with a ``file_name`` column.

    Returns the DataFrame with four columns appended:
    ``client_id_from_filename``, ``filename_timestamp``,
    ``filename_timestamp_parsed``, ``filename_sequence``.

    Default implementation parses the UnaVista MiFIR convention:
    ``<client_id>_<YYYYMMDDhhmmss>_<sequence>_<rest>.xml``.
    """
    if not ENABLE_FILENAME_REGEX:
        return (
            df
            .withColumn("client_id_from_filename", F.lit(None).cast("string"))
            .withColumn("filename_timestamp", F.lit(None).cast("string"))
            .withColumn("filename_timestamp_parsed", F.lit(None).cast("timestamp"))
            .withColumn("filename_sequence", F.lit(None).cast("int"))
        )
    return (
        df
        .withColumn(
            "client_id_from_filename",
            F.regexp_extract(F.col("file_name"), _MIFIR_CLIENT_ID_PATTERN, 1),
        )
        .withColumn(
            "filename_timestamp",
            F.regexp_extract(F.col("file_name"), _MIFIR_TIMESTAMP_PATTERN, 1),
        )
        .withColumn(
            "filename_timestamp_parsed",
            F.to_timestamp(
                F.regexp_extract(F.col("file_name"), _MIFIR_TIMESTAMP_PATTERN, 1),
                "yyyyMMddHHmmss",
            ),
        )
        .withColumn(
            "filename_sequence",
            F.regexp_extract(F.col("file_name"), _MIFIR_SEQUENCE_PATTERN, 1).cast("int"),
        )
    )


def _reporting_date(df: DataFrame) -> DataFrame:
    """Add a ``reporting_date`` DATE column derived from the filename timestamp.

    Falls back to the file modification time if the filename timestamp
    couldn't be parsed (e.g., a non-default filename convention with the
    regex toggle off).
    """
    return df.withColumn(
        "reporting_date",
        F.coalesce(
            F.to_date(F.col("filename_timestamp_parsed")),
            F.to_date(F.col("_file_modification_time")),
        ),
    )
