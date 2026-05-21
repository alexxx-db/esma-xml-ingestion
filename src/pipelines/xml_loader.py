# Databricks notebook source
# MAGIC %md
# MAGIC # ESMA XML Loader — Spark Declarative Pipeline (Bronze)
# MAGIC
# MAGIC Parameterised SDP source backing both the EMIR and MiFIR XML loader
# MAGIC pipelines. Reads XML files via Auto Loader, splits malformed rows into a
# MAGIC quarantine table enriched with an `lxml` XSD-validation error, and
# MAGIC produces a public `{prefix}_raw` streaming table with payload + header
# MAGIC metadata joined per file.
# MAGIC
# MAGIC All inputs are supplied via `spark.conf` — see the bundle pipeline
# MAGIC `configuration` block in `resources/bundle.{emir,mifir}_resources.yml`.

# COMMAND ----------

from __future__ import annotations

import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline configuration
# MAGIC
# MAGIC Values are set in `resources/bundle.*_resources.yml` under
# MAGIC `resources.pipelines.<name>.configuration`. All are resolved at import
# MAGIC time so the `@dp.table` decorators can reference them.

# COMMAND ----------

CATALOG = spark.conf.get("catalog")
RAW_SCHEMA = spark.conf.get("raw_schema")
TABLE_PREFIX = spark.conf.get("table_prefix")
LANDING_PATH = spark.conf.get("landing_path")
ROW_TAG = spark.conf.get("row_tag")
XML_SCHEMA_PYLD_PATH = spark.conf.get("xml_schema_pyld_path")
XML_SCHEMA_HDR_PYLD_METADATA_PATH = spark.conf.get("xml_schema_hdr_pyld_metadata_path")
XML_XSD_SCHEMA_PYLD_PATH = spark.conf.get("xml_xsd_schema_pyld_path")

# Pipeline-config values are always strings; "true"/"false" toggles row-level
# XSD enforcement (rowValidationXSDPath) in raw_xml_payload and the xsd_error
# UDF in quarantine. Default ON preserves production behavior.
ENABLE_XSD_VALIDATION = spark.conf.get("enable_xsd_validation", "true").lower() == "true"

# Toggle filename-regex extraction. Default "true" preserves the ESMA-
# specific naming-convention parsing for FileBatchIndex/FileBatchSize/
# FileVersion/ESMADate. Set to "false" for non-ESMA customers whose
# filenames don't match the `\d{6}-\d_\d{6}_` pattern; the four columns
# stay in the output schema but emit NULL passthrough.
ENABLE_FILENAME_REGEX = spark.conf.get("enable_filename_regex", "true").lower() == "true"

# Auto Loader cleanSource configuration. Controls the lifecycle of files
# that have been successfully processed by Auto Loader:
#   - OFF    : files remain in the landing path (default-safe)
#   - MOVE   : files are archived to CLEAN_SOURCE_MOVE_DEST after
#              CLEAN_SOURCE_RETENTION has elapsed since processing
#   - DELETE : files are deleted after the retention period
#
# Safety w.r.t. the downstream LXML re-read:
# The bronze `raw` table re-reads each file by path (via the lxml UDF
# `_extract_hdr_pyld_metadata`). `cloudFiles.cleanSource.retentionDuration`
# is the *waiting period after processing* before a file becomes a cleanup
# candidate — so as long as `raw` consumes a file within that window, the
# file is guaranteed to still exist at source. Default "7 days" gives
# ~1000× the typical inter-batch lag plus operational headroom for
# backfills / re-runs. moveDestination MUST be inside the same UC volume
# / external location as the source landing path.
CLEAN_SOURCE_MODE = spark.conf.get("clean_source_mode", "OFF").upper()
CLEAN_SOURCE_MOVE_DEST = spark.conf.get("clean_source_move_destination", "")
CLEAN_SOURCE_RETENTION = spark.conf.get("clean_source_retention", "7 days")

# Fully qualified table names — published to {catalog}.{raw_schema}.
TBL_RAW_XML_PAYLOAD = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_raw_xml_payload"
TBL_QUARANTINE = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_quarantine"
TBL_RAW = f"{CATALOG}.{RAW_SCHEMA}.{TABLE_PREFIX}_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filename regex extraction
# MAGIC
# MAGIC Customer-replaceable helper. Default implementation parses the ESMA
# MAGIC naming convention:
# MAGIC
# MAGIC `<6-digit batch index>-<1-digit batch size>_<6-digit YYMMDD>_*.xml`
# MAGIC
# MAGIC Customers with a different filename convention should REPLACE this
# MAGIC function rather than editing the `@dp.table` definitions. The four
# MAGIC output column names must stay the same so downstream silver consumers
# MAGIC keep working; the extraction logic inside is yours to redefine.
# MAGIC
# MAGIC The toggle `ENABLE_FILENAME_REGEX` (config key `enable_filename_regex`)
# MAGIC short-circuits this function to emit NULL passthrough — useful when
# MAGIC filenames don't match any convention and the columns are wanted as a
# MAGIC placeholder only.

# COMMAND ----------

_FILE_INDEX_PATTERN = r"\d\d\d\d\d\d-\d"
_ESMA_DATE_PATTERN = r"-\d\d\d\d\d\d_"


def _add_filename_regex_columns(df):
    """Add four filename-derived columns: FileBatchIndex, FileBatchSize, FileVersion, ESMADate."""
    if not ENABLE_FILENAME_REGEX:
        return (
            df
            .withColumn("FileBatchIndex", F.lit(None).cast("string"))
            .withColumn("FileBatchSize", F.lit(None).cast("string"))
            .withColumn("FileVersion", F.lit(None).cast("string"))
            .withColumn("ESMADate", F.lit(None).cast("string"))
        )
    return (
        df
        .withColumn(
            "FileBatchIndex",
            F.substring(
                F.regexp_extract(F.col("file_name"), _FILE_INDEX_PATTERN, 0),
                1, 3,
            ),
        )
        .withColumn(
            "FileBatchSize",
            F.substring(
                F.regexp_extract(F.col("file_name"), _FILE_INDEX_PATTERN, 0),
                4, 3,
            ),
        )
        .withColumn(
            "FileVersion",
            F.substring(
                F.regexp_extract(F.col("file_name"), _FILE_INDEX_PATTERN, 0),
                8, 1,
            ),
        )
        .withColumn(
            "ESMADate",
            F.concat(
                F.substring(
                    F.regexp_extract(F.col("file_name"), _ESMA_DATE_PATTERN, 0),
                    2, 2,
                ),
                F.lit("-"),
                F.substring(
                    F.regexp_extract(F.col("file_name"), _ESMA_DATE_PATTERN, 0),
                    4, 2,
                ),
                F.lit("-"),
                F.substring(
                    F.regexp_extract(F.col("file_name"), _ESMA_DATE_PATTERN, 0),
                    6, 2,
                ),
            ),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema loading
# MAGIC
# MAGIC The Spark JSON schemas (`pyld_schema.json`,
# MAGIC `hdr_pyld_metadata_schema.json`) and the row-tag XSD
# MAGIC (`row_tag_schema.xsd`) are pre-generated by the Schema Prep notebook
# MAGIC (`src/notebooks/0_1_xml_schema_xsd.py`) and live in a UC Volume next
# MAGIC to the source XSDs.

# COMMAND ----------

def _read_schema(file_path: str) -> StructType:
    """Load a Spark JSON schema file into a StructType."""
    with open(file_path, "r") as f:
        return StructType.fromJson(json.loads(f.read()))


# Loaded once at pipeline-start.
XML_PYLD_SCHEMA: StructType = _read_schema(XML_SCHEMA_PYLD_PATH)
XML_HDR_PYLD_METADATA_SCHEMA: StructType = _read_schema(XML_SCHEMA_HDR_PYLD_METADATA_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## XSD-validation UDF
# MAGIC
# MAGIC Used by the quarantine table only. The XSD schema object is compiled
# MAGIC once per Python worker per XSD path via `_xsd_cache` — Auto Loader's
# MAGIC per-row XSD validation already runs upstream; this UDF only fires on
# MAGIC the small minority of rows that already failed validation, where we
# MAGIC want a human-readable error to surface in the quarantine table.

# COMMAND ----------

_xsd_cache: dict = {}


def _get_xsd_schema(xsd_path: str):
    """Compile and cache an lxml XMLSchema per path, once per worker."""
    if xsd_path not in _xsd_cache:
        from lxml import etree
        with open(xsd_path, "rb") as f:
            _xsd_cache[xsd_path] = etree.XMLSchema(etree.XML(f.read()))
    return _xsd_cache[xsd_path]


@F.udf(returnType=StringType())
def xsd_error(xml_str: str, xsd_path: str) -> str:
    """Return a verbose XSD-validation error message, or 'XML is valid'."""
    from lxml import etree
    try:
        if xml_str is None:
            return "Invalid XML: input is null"
        schema = _get_xsd_schema(xsd_path)
        schema.assertValid(etree.fromstring(xml_str.encode("utf-8")))
        return "XML is valid"
    except Exception as e:
        return f"Invalid XML: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Header-extraction UDF
# MAGIC
# MAGIC Reads each XML file via `lxml.iterparse`, stops at the first row-tag
# MAGIC element, strips empty elements, and returns the header-only XML as a
# MAGIC string. Lenient on failure (returns `None`) — a `dp.expect` on the
# MAGIC parsed header struct is a deliberate follow-up.

# COMMAND ----------

def _strip_namespace(tag: str) -> str:
    """Strip ``{ns}name`` prefix from an lxml tag."""
    return tag.split("}")[-1] if "}" in tag else tag


def _remove_empty_elements(element) -> bool:
    """Recursively drop elements that have no children, attributes, or text."""
    children_to_remove = []
    for child in list(element):
        if _remove_empty_elements(child):
            children_to_remove.append(child)
    for child in children_to_remove:
        element.remove(child)
    has_children = len(list(element)) > 0
    has_attributes = bool(element.attrib)
    has_meaningful_text = (
        (element.text and element.text.strip())
        or (element.tail and element.tail.strip())
    )
    return not has_children and not has_attributes and not has_meaningful_text


def _extract_hdr_pyld_metadata(file_path: str, row_tag: str) -> str | None:
    """Return the header-only XML for a single file, stopping at row_tag."""
    from lxml import etree
    try:
        context = etree.iterparse(file_path, events=("start", "end"), recover=True)
        element_stack = []
        skip_depth = 0
        root = None
        found_row_tag = False

        for event, elem in context:
            tag_name = _strip_namespace(elem.tag)
            if event == "start":
                if tag_name == row_tag and not found_row_tag:
                    found_row_tag = True
                    elem.clear()
                    break
                should_skip = (skip_depth > 0) or (tag_name == row_tag)
                if should_skip:
                    skip_depth += 1
                else:
                    new_elem = etree.Element(elem.tag, attrib=elem.attrib)
                    new_elem.tag = _strip_namespace(new_elem.tag)
                    if element_stack:
                        element_stack[-1].append(new_elem)
                    else:
                        root = new_elem
                    element_stack.append(new_elem)
            elif event == "end":
                if skip_depth > 0:
                    skip_depth -= 1
                elif element_stack:
                    current_elem = element_stack.pop()
                    current_elem.text = elem.text
                    current_elem.tail = elem.tail
                elem.clear()

        if root is not None:
            _remove_empty_elements(root)
            return etree.tostring(root, encoding="unicode", pretty_print=True)
        return None
    except Exception as e:
        # Lenient: failure surfaces as null hdr_pyld_metadata downstream.
        print(f"Error processing {file_path}: {e}")
        return None


extract_hdr_pyld_metadata_udf = F.udf(_extract_hdr_pyld_metadata, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1 of 3 — `{prefix}_raw_xml_payload` (internal)
# MAGIC
# MAGIC Auto Loader reads XML files from the landing path. **All** rows (good
# MAGIC and corrupted) land here. Downstream tables filter on
# MAGIC `corrupted_record`.
# MAGIC
# MAGIC When `ENABLE_XSD_VALIDATION` is False, `rowValidationXSDPath` is
# MAGIC omitted so XSD pattern facets (e.g. LEI `[A-Z0-9]{18}[0-9]{2}`) are not
# MAGIC enforced at row level. Rows that still fail JSON-schema parsing
# MAGIC remain captured in `corrupted_record` and route to the quarantine
# MAGIC table.

# COMMAND ----------

@dp.table(
    name=TBL_RAW_XML_PAYLOAD,
    comment=(
        "Internal: raw XML payload rows from Auto Loader, BEFORE good/bad "
        "split. Includes corrupted_record + rescued_data. Downstream tables "
        f"{TBL_QUARANTINE} and {TBL_RAW} consume this."
    ),
    cluster_by_auto=True,
)
def raw_xml_payload():
    loader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", ROW_TAG)
        .option("columnNameOfCorruptRecord", "corrupted_record")
        .option("rescuedDataColumn", "rescued_data")
        .option("mode", "PERMISSIVE")
    )
    if ENABLE_XSD_VALIDATION:
        loader = loader.option("rowValidationXSDPath", XML_XSD_SCHEMA_PYLD_PATH)

    # cloudFiles.cleanSource — archive/delete processed files after the
    # retention period. OFF is a no-op (default-safe). MOVE/DELETE only
    # become active when the bundle config sets clean_source_mode
    # accordingly. See the CLEAN_SOURCE_* block above for the safety
    # argument vs the downstream LXML re-read.
    if CLEAN_SOURCE_MODE in ("MOVE", "DELETE"):
        loader = (
            loader
            .option("cloudFiles.cleanSource", CLEAN_SOURCE_MODE)
            .option(
                "cloudFiles.cleanSource.retentionDuration",
                CLEAN_SOURCE_RETENTION,
            )
        )
        if CLEAN_SOURCE_MODE == "MOVE":
            loader = loader.option(
                "cloudFiles.cleanSource.moveDestination",
                CLEAN_SOURCE_MOVE_DEST,
            )
    df = (
        loader
        .schema(XML_PYLD_SCHEMA)
        .load(LANDING_PATH)
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("file_name", F.col("_metadata.file_name"))
        .withColumn(
            "_file_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    # Defensive: with a user-supplied schema, some Spark/DBR builds don't
    # materialize the columnNameOfCorruptRecord column unless it's listed
    # in the schema. Add a NULL passthrough so downstream tables (which
    # filter on corrupted_record IS NOT NULL / IS NULL) keep working.
    if "corrupted_record" not in df.columns:
        df = df.withColumn("corrupted_record", F.lit(None).cast("string"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2 of 3 — `{prefix}_quarantine` (public)
# MAGIC
# MAGIC Bad rows from `raw_xml_payload` (`corrupted_record IS NOT NULL`),
# MAGIC enriched with a verbose XSD-validation error from the singleton-cached
# MAGIC lxml UDF. Public so Ops / data stewards can triage without touching
# MAGIC pipeline internals.
# MAGIC
# MAGIC When `ENABLE_XSD_VALIDATION` is False, the `xsd_error` UDF is skipped
# MAGIC (it would fail trying to read the XSD) and `xsd_validation_result` is
# MAGIC the literal `"XSD validation disabled"`; quarantined rows in that mode
# MAGIC come only from JSON-schema parse failures, not XSD pattern violations.

# COMMAND ----------

@dp.table(
    name=TBL_QUARANTINE,
    comment=(
        "Public: malformed XML rows that failed Auto Loader XSD validation, "
        "enriched with xsd_validation_result (human-readable lxml error)."
    ),
    cluster_by_auto=True,
)
def quarantine():
    bad_rows = (
        spark.readStream.table(TBL_RAW_XML_PAYLOAD)
        .filter(F.col("corrupted_record").isNotNull())
    )
    if ENABLE_XSD_VALIDATION:
        bad_rows = bad_rows.withColumn(
            "xsd_validation_result",
            xsd_error(F.col("corrupted_record"), F.lit(XML_XSD_SCHEMA_PYLD_PATH)),
        )
    else:
        bad_rows = bad_rows.withColumn(
            "xsd_validation_result",
            F.lit("XSD validation disabled"),
        )
    return bad_rows.select(
        "file_path",
        "file_name",
        "_file_modification_time",
        "_ingested_at",
        "corrupted_record",
        "rescued_data",
        "xsd_validation_result",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3 of 3 — `{prefix}_raw` (public)
# MAGIC
# MAGIC Self-joins payload rows with their per-file header metadata, deriving
# MAGIC **both** sides from a single readStream of `raw_xml_payload`. This
# MAGIC avoids the cross-flow coordination issue we hit with a separate
# MAGIC `file_hdr_metadata` streaming table — emit on the first triggered run,
# MAGIC not the second.
# MAGIC
# MAGIC The header subquery deduplicates on `file_path` so the lxml UDF fires
# MAGIC once per file per trigger, then applies `from_xml` and the filename
# MAGIC regex (`FileBatchIndex` / `FileBatchSize` / `FileVersion` /
# MAGIC `ESMADate`).
# MAGIC
# MAGIC Consumed downstream by the per-regime silver pipelines.

# COMMAND ----------

@dp.table(
    name=TBL_RAW,
    comment=(
        "Public: per-row payload joined with per-file header metadata "
        "(extracted via lxml from the source XML). Consumed by the "
        "per-regime silver pipelines (silver_emir.py, silver_mifir.py)."
    ),
    cluster_by_auto=True,
)
def raw():
    payload = (
        spark.readStream.table(TBL_RAW_XML_PAYLOAD)
        .filter(F.col("corrupted_record").isNull())
    )

    headers = (
        payload.select("file_path", "file_name", "_file_modification_time")
        .dropDuplicates(["file_path"])
        .select(
            "file_path",
            "file_name",
            "_file_modification_time",
            extract_hdr_pyld_metadata_udf(
                F.col("file_path"), F.lit(ROW_TAG)
            ).alias("_hdr_xml"),
        )
        .withColumn(
            "hdr_pyld_metadata",
            F.from_xml(F.col("_hdr_xml"), XML_HDR_PYLD_METADATA_SCHEMA),
        )
        .drop("_hdr_xml")
        .transform(_add_filename_regex_columns)
    )

    # Right-side alias avoids duplicate file_path / file_name /
    # _file_modification_time columns after the inner join.
    headers_aliased = headers.select(
        F.col("file_path").alias("_hdr_file_path"),
        "hdr_pyld_metadata",
        "FileBatchIndex",
        "FileBatchSize",
        "FileVersion",
        "ESMADate",
    )

    return (
        payload.join(
            headers_aliased,
            payload["file_path"] == headers_aliased["_hdr_file_path"],
            "inner",
        )
        .drop("_hdr_file_path", "corrupted_record", "rescued_data")
    )
