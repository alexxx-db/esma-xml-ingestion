# Databricks notebook source
# MAGIC %md
# MAGIC # XML File Ingestion Pipeline
# MAGIC 
# MAGIC **Notice:** This code is shared as a guide and should not be deployed directly into production on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Libraries

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

from pyspark.errors import PySparkException
from pyspark.sql.functions import col, current_timestamp, regexp_extract, substring, concat, lit, udf, from_xml
from pyspark.sql.types import StringType, StructType
from pyspark.sql import DataFrame
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters for the Notebook

# COMMAND ----------

dbutils.widgets.text("catalog","esma")
dbutils.widgets.text("raw_schema","emir_raw")

dbutils.widgets.text("xml_schema_pyld_path","/Volumes/esma/default/regulatory_data/schemas/emir/pyld_schema.json")
dbutils.widgets.text("xml_schema_hdr_pyld_metadata_path","/Volumes/esma/default/regulatory_data/schemas/emir/hdr_pyld_metadata_schema.json")
dbutils.widgets.text("xml_xsd_schema_pyld_path","/Volumes/esma/default/regulatory_data/schemas/emir/row_tag_schema.xsd")

dbutils.widgets.text("landing_path", "/Volumes/esma/default/regulatory_data/emir/landing/")
dbutils.widgets.text("checkpoint_path", "/Volumes/esma/default/regulatory_data/emir/checkpoints/")
dbutils.widgets.text("processed_path", "/Volumes/esma/default/regulatory_data/emir/processed/")

dbutils.widgets.text("files_per_trigger", "16")
dbutils.widgets.text("row_tag", "Stat")

dbutils.widgets.text("table_prefix", "emir_")

# COMMAND ----------

# Retrieve parameters - Updated for new job parameter structure
catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")

xml_schema_pyld_path = dbutils.widgets.get("xml_schema_pyld_path")
xml_schema_hdr_pyld_metadata_path = dbutils.widgets.get("xml_schema_hdr_pyld_metadata_path")
xml_xsd_schema_pyld_path = dbutils.widgets.get("xml_xsd_schema_pyld_path")

landing_path = dbutils.widgets.get("landing_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
processed_path = dbutils.widgets.get("processed_path")

files_per_trigger = int(dbutils.widgets.get("files_per_trigger"))
row_tag = dbutils.widgets.get("row_tag")

table_prefix = dbutils.widgets.get("table_prefix")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Reader Function

# COMMAND ----------

def readSchema(file: str) -> StructType:
    """
    Reads a JSON schema from a file and returns a Spark StructType object.

    Args:
        file (str): Path to the JSON schema file.

    Returns:
        StructType: Spark schema object parsed from the JSON file.
    """
    with open(file, 'r') as f:
        schema_json = f.read()
    schema = StructType.fromJson(json.loads(schema_json))
    return schema

# COMMAND ----------

xml_pyld_schema = readSchema(xml_schema_pyld_path)
xml_hdr_pyld_metadata_schema = readSchema(xml_schema_hdr_pyld_metadata_path)

# COMMAND ----------

#TODO: Add cloudFiles.cleanSource and cloudFiles.moveDestination

raw_pyld_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "xml")
    .option("rowTag", row_tag)
    .option("rowValidationXSDPath", xml_xsd_schema_pyld_path)
    .option("columnNameOfCorruptRecord","corrupted_record")
    .option("rescuedDataColumn", "rescued_data")
    .option("mode", "PERMISSIVE")
    # .option("cloudFiles.maxFilesPerTrigger", files_per_trigger)
    .schema(xml_pyld_schema)
    .load(landing_path)            
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## XML Header and Payload Metadata Extraction UDF

# COMMAND ----------

from lxml import etree
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def strip_namespace(tag: str) -> str:
    """Remove XML namespace from tag."""
    return tag.split('}')[-1] if '}' in tag else tag

def remove_empty_elements(element):
    children_to_remove = []
    for child in list(element):
        if remove_empty_elements(child):
            children_to_remove.append(child)
    for child in children_to_remove:
        element.remove(child)
    has_children = len(list(element)) > 0
    has_attributes = bool(element.attrib)
    has_meaningful_text = (element.text and element.text.strip()) or (element.tail and element.tail.strip())
    is_completely_empty = not has_children and not has_attributes and not has_meaningful_text
    return is_completely_empty

def extract_hdr_pyld_metadata(file_path: str, row_tag: str = "Stat") -> str:
    try:
        context = etree.iterparse(file_path, events=("start", "end"), recover=True)
        element_stack = []
        skip_depth = 0
        root = None
        found_row_tag = False
        
        for event, elem in context:
            tag_name = strip_namespace(elem.tag)
            
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
                    new_elem.tag = strip_namespace(new_elem.tag)

                    if element_stack:
                        element_stack[-1].append(new_elem)
                    else:
                        root = new_elem
                    element_stack.append(new_elem)
            elif event == "end":
                if skip_depth > 0:
                    skip_depth -= 1
                else:
                    if element_stack:
                        current_elem = element_stack.pop()
                        current_elem.text = elem.text
                        current_elem.tail = elem.tail
                elem.clear()
        
        if root is not None:
            remove_empty_elements(root)
            return etree.tostring(root, encoding="unicode", pretty_print=True)
        return None
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None

extract_hdr_pyld_metadata_udf = udf(extract_hdr_pyld_metadata, StringType())
# extract_hdr_pyld_metadata("/Volumes/users/matthew_moorcroft/mifir/lseg_landing/3_sample_data.xml", "Tx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autoloader Metadata Information Columns

# COMMAND ----------

raw_pyld_df = (
  raw_pyld_df
  .withColumn("source_metadata", col("_metadata"))
  .withColumn("file_path", col("source_metadata.file_path"))
  .withColumn("file_name", col("source_metadata.file_name"))
  .withColumn("inserted_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF Call to Extract Metadata

# COMMAND ----------

distinct_file_paths_df = raw_pyld_df.select("file_path", "file_name").distinct()
hdr_pyld_metadata_df = (distinct_file_paths_df.withColumn(
  "hdr_pyld_metadata", 
  from_xml(
    extract_hdr_pyld_metadata_udf(col("file_path"), lit(row_tag)), 
    xml_hdr_pyld_metadata_schema
    )
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zip File Table Regex Extraction

# COMMAND ----------

#TODO: Join hdr_pyld_metadata_df with the table that contains file_name and zip_name to run regex on that before the join

file_index_pattern = r"\d\d\d\d\d\d-\d"
esma_date_pattern = r"-\d\d\d\d\d\d_"

hdr_pyld_mtdt_rgx_df = (hdr_pyld_metadata_df
    .withColumn(
        "FileBatchIndex",
        substring(regexp_extract(col("file_name"), file_index_pattern, 0),1,3)
    )
    .withColumn(
        "FileBatchSize",
        substring(regexp_extract(col("file_name"), file_index_pattern, 0),4,3)
    )
    .withColumn(
        "FileVersion",
        substring(regexp_extract(col("file_name"), file_index_pattern, 0),8,1)
    )
    .withColumn(
        "ESMADate",
        concat(
            substring(regexp_extract(col("file_name"), esma_date_pattern, 0), 2, 2), lit('-'),
            substring(regexp_extract(col("file_name"), esma_date_pattern, 0), 4, 2), lit('-'),
            substring(regexp_extract(col("file_name"), esma_date_pattern, 0), 6, 2)
        )
    )
).drop("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Header and Payload Metadata Join with Payload

# COMMAND ----------

hdr_pyld_df = raw_pyld_df.join(hdr_pyld_mtdt_rgx_df, on="file_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Name and Checkpoint Definition

# COMMAND ----------

raw_table = f"{table_prefix}_raw"
raw_table_checkpoint = f"{checkpoint_path}/{raw_table}_raw_table_checkpoint"

# COMMAND ----------

#TODO: Only to be used during testing, must be removed after

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{raw_schema}.{raw_table}")
dbutils.fs.rm(raw_table_checkpoint, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Table Write

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{raw_schema}")

# COMMAND ----------

try:
  (hdr_pyld_df.writeStream
    .trigger(availableNow=True)
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", raw_table_checkpoint) 
    .toTable(f"{catalog}.{raw_schema}.{raw_table}")
  )

except PySparkException as ex:
  print("Error Class       : " + ex.getErrorClass())
  print("Message parameters: " + str(ex.getMessageParameters()))
  print("SQLSTATE          : " + ex.getSqlState())
  print(ex)
