# ESMA XML Ingestion - User Guide

**Version:** 1.0.0  
**Last Updated:** October 2025  

---

## Important Notice

This solution is provided as a **Databricks Solution Accelerator** - a reference implementation designed to help customers quickly build production-ready data pipelines for ESMA-compliant XML ingestion.

---

## Table of Contents

1. [Why This Solution?](#why-this-solution?)  
2. [Overview](#overview)  
3. [Architecture](#architecture)  
4. [Component Guide](#component-guide)  
5. [Processing Flow](#processing-flow)  
6. [Usage Instructions](#usage-instructions)
   - [Setting Up a New Regulation Type](#setting-up-a-new-regulation-type)
   - [Running Production Workloads](#running-production-workloads)
7. [Productionizing with Databricks Asset Bundles](#productionizing-with-databricks-asset-bundles)
   - [Quick Start](#quick-start)
   - [Adding a New Regulation](#adding-a-new-regulation)
   - [CI/CD Integration](#cicd-integration)
   - [Production Best Practices](#production-best-practices)
8. [Configuration](#configuration)  
9. [Troubleshooting](#troubleshooting)

## Why This Solution? {#why-this-solution?}

### The Regulatory Challenge

Financial institutions operating under ESMA (European Securities and Markets Authority) regulation are required to submit or consume XML-based transaction reports. These XML files present significant technical challenges that have led to fragile, unscalable ingestion pipelines across the industry.

### Key Challenges

Organizations processing ESMA-compliant XML files face several critical issues:

| Challenge | Impact |
| :---- | :---- |
| **Deeply Nested Schemas** | Complex ISO 20022 structures make parsing and flattening extremely difficult |
| **Schema Evolution** | Non-uniform or evolving schema definitions break existing pipelines |
| **Large File Sizes** | Files up to 2GB+ create memory pressure and force workarounds |
| **Lack of Tooling** | No industry-standard solutions available |
| **Compliance Requirements** | Must validate against XSD schemas and maintain audit trails |

### What Makes This Solution Different

This accelerator provides a **unified, production-grade pipeline** built on Databricks that addresses all these challenges:

**Technical Advantages**:

- **Native Spark Processing**: Distributed XML parsing handles files of any size  
- **XSD-Driven Validation**: Built-in schema validation ensures compliance  
- **Auto Loader Integration**: Efficient file discovery and incremental processing  
- **Schema Evolution**: Automatic handling of schema changes  
- **Unity Catalog**: Built-in governance, lineage, and access control

**Operational Benefits**:

- **Standardized Approach**: Reusable across EMIR, MiFIR, and other ESMA regulations  
- **Production-Ready**: Battle-tested with CBI  
- **Observable**: Full monitoring and debugging capabilities  
- **Maintainable**: Clear separation of concerns, modular design

## Overview {#overview}

This solution provides a three-stage pipeline for ingesting and processing complex XML files that comply with ESMA regulatory requirements. The pipeline transforms deeply nested XML structures into flattened, queryable Delta tables suitable for analytics and reporting.

### Key Capabilities

- **Schema Processing**: Convert XSD schemas to Spark-compatible JSON schemas  
- **Streaming Ingestion**: Process XML files as they arrive using Auto Loader  
- **Metadata Extraction**: Separate header/metadata from payload data  
- **Automatic Flattening**: Recursively flatten nested structures and arrays  
- **Delta Lake Storage**: Store processed data in Unity Catalog tables

## Architecture {#architecture}

### Three-Stage Pipeline

```
Stage 0: Schema Preparation
    └── Convert XSD to JSON schemas
    └── Create specialized schemas for payload and metadata
    └── Generate row tag schemas for validation

Stage 1: XML Ingestion
    └── Stream XML files using Auto Loader
    └── Extract header and payload metadata
    └── Parse payload data using row tags
    └── Write to raw Delta table

Stage 2: Flattening and Transformation
    └── Read from raw table
    └── Flatten nested structures
    └── Explode arrays into separate tables
    └── Create foreign key relationships
    └── Write to bronze Delta tables
```

### Data Flow Diagram

![][image1]

## Component Guide {#component-guide}

### 0\_1\_xml\_schema\_xsd.py \- Schema Processing Notebook

**Purpose**: Prepares schemas required for XML parsing and validation.

**Key Functions**:

1. **XSD to JSON Conversion** (Scala)  
     
   - Reads XSD files from configured paths  
   - Uses Spark's `XSDToSchema` to convert to JSON  
   - Outputs JSON schema files to the schemas directory

   

2. **Specialized Schema Creation** (Python)  
     
   - Creates `pyld_schema.json`: Contains only the row tag structure for payload parsing  
   - Creates `hdr_pyld_metadata_schema.json`: Contains header and metadata fields (excluding row tags)  
   - Uses explicit schema mappings for reliable field replacement

   

3. **Row Tag XSD Generation** (Python)  
     
   - Extracts specific row tag definitions from payload XSD  
   - Creates standalone XSD for row-level validation  
   - Compatible with Spark XML reader validation

**Parameters**:

| Parameter | Description | Example |
| :---- | :---- | :---- |
| `schemas_path` | Output directory for generated schemas | `/Volumes/esma/default/regulatory_data/emir/schemas/` |
| `master_xsd_path` | Path to master XSD file | `/Volumes/esma/default/regulatory_data/emir/xsd/master_schema.xsd` |
| `payload_xsd_path` | Path to payload XSD file | `/Volumes/esma/default/regulatory_data/emir/xsd/payload_schema.xsd` |
| `row_tag` | XML element name for row-level data | `Stat` (EMIR) or `Tx` (MiFIR) |
| `schema_mappings_json` | JSON array of field mappings | See Configuration section |

**Schema Mappings Format**:

```json
[
  {
    "field": "Hdr",
    "file_path": "/path/to/header.xsd"
  },
  {
    "field": "Pyld",
    "file_path": "/path/to/payload.xsd",
    "payload": true
  }
]
```

**Output Files**:

- `master_schema.json`: Full schema in JSON format  
- `payload_schema.json`: Payload schema in JSON format  
- `pyld_schema.json`: Row tag structure only  
- `hdr_pyld_metadata_schema.json`: Header and metadata fields  
- `row_tag_schema.xsd`: Row tag validation schema

**Logic Flow**:

1. Parse schema mappings from job parameters  
2. Convert each XSD file to JSON using Scala  
3. Combine master schema with mapped field schemas  
4. Extract row tag schema from payload  
5. Create metadata schema by removing row tag fields  
6. Generate standalone row tag XSD for validation  
7. Validate all output schemas

### 1\_xml\_file\_loader\_body.py \- XML Ingestion Notebook

**Purpose**: Streams XML files, extracts metadata, and writes to raw Delta tables.

**Key Functions**:

1. **Auto Loader Configuration**  
     
   - Uses Databricks Auto Loader with XML format  
   - Applies row tag for record-level parsing  
   - Enables XSD validation with `rowValidationXSDPath`  
   - Handles corrupt records with `corrupted_record` column  
   - Captures unparsed data with `rescued_data` column

   

2. **Metadata Extraction UDF** (`extract_hdr_pyld_metadata`)  
     
   - Parses XML file using `lxml.etree.iterparse`  
   - Extracts all content before the first row tag occurrence  
   - Removes empty elements for cleaner metadata  
   - Returns XML string of header and metadata sections

   

3. **File Metadata Enrichment**  
     
   - Adds Auto Loader metadata columns (`_metadata`)  
   - Extracts file path and file name  
   - Adds ingestion timestamp

   

4. **Regex-Based File Information Extraction**  
     
   - Extracts file batch index from filename  
   - Extracts file batch size from filename  
   - Extracts file version from filename  
   - Parses ESMA date from filename pattern

   

5. **Header/Payload Join**  
     
   - Joins distinct file metadata with payload records  
   - Associates header information with each transaction

**Parameters**:

| Parameter | Description | Example |
| :---- | :---- | :---- |
| `catalog` | Target Unity Catalog | `esma` |
| `raw_schema` | Target schema for raw tables | `emir_raw` |
| `xml_schema_pyld_path` | Path to payload JSON schema | `/Volumes/.../pyld_schema.json` |
| `xml_schema_hdr_pyld_metadata_path` | Path to metadata JSON schema | `/Volumes/.../hdr_pyld_metadata_schema.json` |
| `xml_xsd_schema_pyld_path` | Path to row tag XSD | `/Volumes/.../row_tag_schema.xsd` |
| `landing_path` | Directory containing XML files | `/Volumes/.../landing/` |
| `checkpoint_path` | Checkpoint directory for streaming | `/Volumes/.../checkpoints/` |
| `processed_path` | Directory for processed files | `/Volumes/.../processed/` |
| `files_per_trigger` | Max files per micro-batch | `16` |
| `row_tag` | Row tag element name | `Stat` |
| `table_prefix` | Prefix for table names | `emir_` |

**Logic Flow**:

1. Load payload and metadata schemas from JSON files  
2. Configure Auto Loader stream with:  
   - XML format and row tag  
   - XSD validation path  
   - Corrupt record handling  
   - Rescued data column  
3. Read XML files as structured DataFrame  
4. Add Auto Loader metadata columns (file path, name, timestamp)  
5. Extract header/payload metadata for distinct files using UDF  
6. Parse metadata XML string into structured columns  
7. Extract file information using regex patterns  
8. Join payload data with header/metadata on file path  
9. Write combined data to raw Delta table with checkpoint

**Key UDF Logic** (`extract_hdr_pyld_metadata`):

```py
# Iterative parsing approach for memory efficiency
context = etree.iterparse(file_path, events=("start", "end"))

# Skip row tag elements, capture everything else
for event, elem in context:
    if tag == row_tag:
        break  # Stop at first row tag
    else:
        # Build clean XML tree without row tags
        # Remove empty elements
        # Return as XML string
```

**Output Table Schema**:

```
raw_table (e.g., emir_raw)
├── source_metadata: struct (Auto Loader metadata)
├── file_path: string
├── file_name: string
├── inserted_at: timestamp
├── hdr_pyld_metadata: struct (parsed header/metadata)
│   ├── Header fields
│   └── Metadata fields
├── FileBatchIndex: string
├── FileBatchSize: string
├── FileVersion: string
├── ESMADate: string
├── [Payload columns from pyld_schema]
├── corrupted_record: string (for invalid records)
└── rescued_data: string (for unparsed data)
```

### 2\_flatten\_explode\_table.py \- Flattening and Transformation Notebook

**Purpose**: Flattens nested structures and explodes arrays into normalized bronze tables.

**Key Functions**:

1. **Recursive Schema Flattening** (`generate_flat_schemas`)  
     
   - Recursively processes struct and array types  
   - Flattens nested structs by concatenating field names  
   - Explodes arrays into separate child tables  
   - Creates surrogate keys (`_sk`) using MD5 hash  
   - Maintains parent-child relationships with foreign keys

   

2. **Surrogate Key Generation**  
     
   - Uses MD5 hash of content for unique identification  
   - Includes parent foreign key if available  
   - Includes array position for uniqueness  
   - Ensures reproducible keys across runs

   

3. **Foreign Key Relationships**  
     
   - Creates `_parent_fk_{parent_table}` columns  
   - Links child tables to parent via surrogate key  
   - Enables relational queries across flattened tables

   

4. **Streaming Table Creation** (`create_all_flattened_tables`)  
     
   - Creates catalog and schema if not exists  
   - Writes each flattened DataFrame to Delta table  
   - Uses streaming writes with checkpoints  
   - Enables schema evolution with `mergeSchema`

**Parameters**:

| Parameter | Description | Example |
| :---- | :---- | :---- |
| `catalog` | Target Unity Catalog | `esma` |
| `raw_schema` | Source schema (raw tables) | `emir_raw` |
| `bronze_schema` | Target schema (flattened tables) | `emir_bronze` |
| `table_prefix` | Prefix for table names | `emir_` |
| `checkpoint_path` | Checkpoint directory | `/Volumes/.../checkpoints/` |
| `files_per_trigger` | Max files per micro-batch | `16` |

**Logic Flow**:

1. Read from raw Delta table as stream  
2. Call `generate_flat_schemas` with base table name  
3. Recursively process schema:  
   - Identify simple fields, structs, and arrays  
   - Flatten structs by concatenating paths  
   - Explode arrays with position tracking  
   - Generate surrogate keys for each table  
   - Create foreign key columns for child tables  
4. Return list of \[table\_name, dataframe\] pairs  
5. Call `create_all_flattened_tables` to write:  
   - Create catalog and schema if needed  
   - Write each DataFrame to Delta with streaming  
   - Configure checkpoints for fault tolerance  
   - Enable schema merging for evolution

**Flattening Algorithm** (`generate_flat_schemas`):

```py
def generate_flat_schemas(schema, df, parent_name, df_name, 
                          parent_sk_col, parent_table_name, depth):
    # Step 1: Generate surrogate key from content hash
    hash_components = [all non-array columns]
    sk = md5(concat(hash_components))
    
    # Step 2: Process simple fields (strings, numbers, etc.)
    flat_cols = [fields that are not struct or array]
    
    # Step 3: Process nested structs
    for struct_field:
        flatten_name = parent.field.subfield  # e.g., "Hdr_AppHdr_Fr"
        add to select expressions
    
    # Step 4: Create current table DataFrame
    df_struct = df.select(file_name, sk, parent_fk, all_flat_cols)
    add to result list
    
    # Step 5: Process arrays (recursive)
    for array_field:
        child_table_name = array_path
        df_child = df.explode(array_field with position)
        recursively call generate_flat_schemas(df_child)
        add child results to list
    
    return list of all tables
```

**Output Table Structure**:

```
Example for EMIR data with nested structure:

emir_base
├── file_name: string
├── _sk: string (MD5 surrogate key)
├── [all flat fields from root]
├── [flattened struct fields]
└── [no arrays - they become child tables]

emir_Pyld_TxRpt (child of base)
├── file_name: string
├── _sk: string
├── _parent_fk_base: string (FK to parent)
├── array_pos: integer (position in array)
├── [all flat fields from TxRpt]
└── [no nested arrays]

emir_Pyld_TxRpt_OthrPty (child of TxRpt)
├── file_name: string
├── _sk: string
├── _parent_fk_Pyld_TxRpt: string
├── array_pos: integer
└── [all flat fields from OthrPty]
```

### util/xsd\_processor.py \- Schema Processing Utilities

**Purpose**: Provides utility functions for XSD and schema manipulation.

**Main Functions**:

#### 1\. `create_specialized_schemas()`

Creates two specialized schemas from master and mapped schemas:

**Parameters**:

- `master_json_path`: Path to master JSON schema  
- `schema_mappings`: List of field mappings with paths  
- `row_tag_name`: Row tag element name (e.g., "Stat", "Tx")  
- `output_folder`: Directory for output schemas  
- `validate_schemas`: Whether to validate outputs (default: True)

**Returns**:

```py
{
    "success": True/False,
    "pyld_schema_path": "/path/to/pyld_schema.json",
    "metadata_schema_path": "/path/to/hdr_pyld_metadata_schema.json",
    "schema_mappings": {"field1": "loaded", ...},
    "validation_results": {...}
}
```

#### 2\. `create_row_tag_xsd()`

Creates standalone XSD for specific row tag validation:

**Parameters**:

- `payload_xsd_path`: Path to payload XSD file  
- `row_tag_name`: Row tag element name  
- `output_path`: Output path for row tag XSD  
- `validate_output`: Whether to validate output (default: True)

**Returns**:

```py
{
    "success": True/False,
    "output_path": "/path/to/row_tag_schema.xsd",
    "row_tag_element": "Stat",
    "row_tag_type": "StatType",
    "namespace_used": "no-namespace (Auto Loader compatible)"
}
```

## Processing Flow {#processing-flow}

### Complete End-to-End Flow

```
1. PREPARATION (Run Once)
   ├── Upload XSD files to Unity Catalog volume
   ├── Configure schema_mappings parameter
   └── Run Stage 0: Schema Processing
       ├── Convert XSD to JSON
       ├── Create specialized schemas
       └── Generate row tag XSD

2. INGESTION (Continuous)
   └── Run Stage 1: XML Ingestion
       ├── Auto Loader monitors landing directory
       ├── Parses XML files with row tag
       ├── Validates against row tag XSD
       ├── Extracts header/metadata
       ├── Parses payload data
       ├── Joins metadata with payload
       └── Writes to raw Delta table

3. TRANSFORMATION (Continuous)
   └── Run Stage 2: Flattening
       ├── Reads from raw table (stream)
       ├── Flattens nested structs
       ├── Explodes arrays to child tables
       ├── Creates surrogate keys
       ├── Establishes foreign key relationships
       └── Writes to bronze Delta tables

4. ANALYTICS (Ad-hoc)
   └── Query flattened bronze tables
       ├── Simple SELECT statements
       ├── JOIN child tables via foreign keys
       └── Use standard SQL for analysis
```

### Data Transformation Example

**Input XML**:

```xml
<Root>
  <Hdr>
    <CreDtTm>2024-01-01T10:00:00</CreDtTm>
  </Hdr>
  <Pyld>
    <Stat>
      <TxId>TXN001</TxId>
      <Ctrpty>
        <Id>PARTY1</Id>
        <Nm>Party One</Nm>
      </Ctrpty>
      <OthrPty>
        <Id>PARTY2</Id>
      </OthrPty>
      <OthrPty>
        <Id>PARTY3</Id>
      </OthrPty>
    </Stat>
  </Pyld>
</Root>
```

**After Stage 1 (Raw Table)**:

```
Row:
  file_path: /Volumes/.../file.xml
  file_name: file.xml
  hdr_pyld_metadata.Hdr.CreDtTm: 2024-01-01T10:00:00
  TxId: TXN001
  Ctrpty.Id: PARTY1
  Ctrpty.Nm: Party One
  OthrPty: [
    {Id: PARTY2},
    {Id: PARTY3}
  ]
```

**After Stage 2 (Bronze Tables)**:

Table: `base`

```
file_name | _sk    | Hdr_CreDtTm          | TxId   | Ctrpty_Id | Ctrpty_Nm
----------|--------|----------------------|--------|-----------|----------
file.xml  | hash1  | 2024-01-01T10:00:00  | TXN001 | PARTY1    | Party One
```

Table: `base_OthrPty`

```
file_name | _sk    | _parent_fk_base | array_pos | Id
----------|--------|-----------------|-----------|--------
file.xml  | hash2  | hash1           | 0         | PARTY2
file.xml  | hash3  | hash1           | 1         | PARTY3
```

## Usage Instructions {#usage-instructions}

### Setting Up a New Regulation Type

1. **Prepare XSD Files**  
     
   - Upload the XSD files into a Volumes location in Unity Catalog

2. **Configure Schema Mappings**

```py
schema_mappings = [
    {
        "field": "Hdr",
        "file_path": "/Volumes/.../header.xsd"
    },
    {
        "field": "Pyld",
        "file_path": "/Volumes/.../payload.xsd",
        "payload": True  # Mark as payload field
    }
]
```

3. **Run Schema Processing**  
     
   - Open `0_1_xml_schema_xsd.py`  
   - Set parameters:  
     - `schemas_path`: Output directory  
     - `master_xsd_path`: Master XSD path  
     - `payload_xsd_path`: Payload XSD path  
     - `row_tag`: Row tag element name  
     - `schema_mappings_json`: JSON string of mappings  
   - Run all cells  
   - Verify output schemas are created

   

4. **Configure Ingestion Job**  
     
   - Open `1_xml_file_loader_body.py`  
   - Set parameters:  
     - `catalog`: Target catalog name  
     - `raw_schema`: Raw schema name  
     - `xml_schema_pyld_path`: Path to `pyld_schema.json`  
     - `xml_schema_hdr_pyld_metadata_path`: Path to `hdr_pyld_metadata_schema.json`  
     - `xml_xsd_schema_pyld_path`: Path to `row_tag_schema.xsd`  
     - `landing_path`: XML files directory  
     - `checkpoint_path`: Checkpoint directory  
     - `row_tag`: Row tag element name  
     - `table_prefix`: Table name prefix

   

5. **Configure Flattening Job**  
     
   - Open `2_flatten_explode_table.py`  
   - Set parameters:  
     - `catalog`: Same as ingestion  
     - `raw_schema`: Same as ingestion  
     - `bronze_schema`: Bronze schema name  
     - `table_prefix`: Same as ingestion  
     - `checkpoint_path`: Checkpoint directory

   

6. **Test with Sample Files**

```py
# Copy sample XML to landing (adls location)
dbutils.fs.cp("file:/local/sample.xml", landing_path)

# Run ingestion job
# Check raw table
spark.sql(f"SELECT * FROM {catalog}.{raw_schema}.{table_prefix}_raw LIMIT 10")
```

### Running Production Workloads

1. **Configure Auto Loader Options**

```py
# In 1_xml_file_loader_body.py
.option("cloudFiles.useManagedFileEvents", "true")  # For file events
.option("cloudFiles.maxFilesPerTrigger", files_per_trigger)
.option("cloudFiles.cleanSource", "MOVE")  # Optional: archive processed
.option("cloudFiles.cleanSource.moveDestination", processed_path)
```

2. **Set Trigger Mode**

```py
# For continuous processing
.trigger(processingTime='5 minutes')

# For batch processing
.trigger(availableNow=True)
```

3. **Monitor Jobs**  
     
   - Check Databricks Jobs UI for run status  
   - Monitor streaming query progress  
   - Review checkpoint directories for state  
   - Check raw and bronze tables for data

## Productionizing with Databricks Asset Bundles {#productionizing-with-databricks-asset-bundles}

### Overview

This solution uses **Databricks Asset Bundles (DABs)** for Infrastructure-as-Code deployment. DABs enable:

- Version control for all pipeline components  
- Multi-environment deployment (dev/prod)  
- CI/CD integration  
- Easy extension for new regulations

### Quick Start

**Install CLI:**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks configure
```

**Deploy:**

```bash
# Development
databricks bundle deploy -t dev
databricks bundle run EMIR_XML_Processing -t dev

# Production
databricks bundle deploy -t prod
databricks bundle run EMIR_XML_Processing -t prod
```

### Bundle Structure

```
esma_xml_ingestion/
├── databricks.yml                      # Main configuration
├── resources/
│   ├── bundle.emir_resources.yml      # EMIR jobs
│   ├── bundle.mifir_resources.yml     # MiFIR jobs
│   ├── bundle.variables.yml           # Variables
│   └── bundle.new-type_resources.yml.template  # Template for new regulations
└── src/                                # Notebooks
```

### Adding a New Regulation

**Step 1:** Copy template

```bash
cp resources/bundle.new-type_resources.yml.template \
   resources/bundle.sftr_resources.yml
```

**Step 2:** Edit file - replace `TEMPLATE` with `SFTR` and `template` with `sftr`

**Step 3:** Add variables to `resources/bundle.variables.yml`:

```yaml
sftr_row_tag:
  default: "FinancingTransaction"
sftr_landing_path:
  default: "${var.volume_path}/sftr/landing/"
sftr_schemas_path:
  default: "${var.volume_path}/sftr/schemas/"
```

**Step 4:** Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run SFTR_Schema_Creation -t dev
```

### CI/CD Integration

**GitHub Actions:**

```yaml
name: Deploy Pipeline
on:
  push:
    branches: [main, develop]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
          TARGET=${{ github.ref == 'refs/heads/main' && 'prod' || 'dev' }}
          databricks bundle deploy -t $TARGET
```

### Production Best Practices

1. **Use Databricks secrets** for credentials (never hardcode)
2. **Add job schedules** for automated runs
3. **Configure alerts** for failures
4. **Tag releases** before production deployments
5. **Override variables** per environment:

```bash
databricks bundle deploy -t prod \
  --var catalog="esma_prod" \
  --var emir_max_concurrent_runs=20
```

### Common Commands

```bash
# Validate
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t prod

# Run job
databricks bundle run EMIR_XML_Processing -t dev

# List jobs
databricks jobs list | grep EMIR
```

### Troubleshooting

**Validation errors:** Check YAML syntax and variable references  
**Deployment fails:** Run with `--debug` flag  
**Job not found:** Verify deployment completed successfully

For detailed DAB documentation, see [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)

## Configuration {#configuration}

### Job Parameters Reference

**Schema Processing Job (Stage 0\)**:

```
schemas_path: "/Volumes/esma/default/regulatory_data/emir/schemas/"
master_xsd_path: "/Volumes/esma/default/regulatory_data/emir/xsd/master_schema.xsd"
payload_xsd_path: "/Volumes/esma/default/regulatory_data/emir/xsd/payload_schema.xsd"
row_tag: "Stat"
schema_mappings_json: '[
  {"field": "Hdr", "file_path": "/Volumes/.../header.xsd"},
  {"field": "Pyld", "file_path": "/Volumes/.../payload.xsd", "payload": true}
]'
```

**Ingestion Job (Stage 1\)**:

```
catalog: "esma"
raw_schema: "emir_raw"
xml_schema_pyld_path: "/Volumes/esma/default/regulatory_data/emir/schemas/pyld_schema.json"
xml_schema_hdr_pyld_metadata_path: "/Volumes/esma/default/regulatory_data/emir/schemas/hdr_pyld_metadata_schema.json"
xml_xsd_schema_pyld_path: "/Volumes/esma/default/regulatory_data/emir/schemas/row_tag_schema.xsd"
landing_path: "/Volumes/esma/default/regulatory_data/emir/landing/"
checkpoint_path: "/Volumes/esma/default/regulatory_data/emir/checkpoints/"
processed_path: "/Volumes/esma/default/regulatory_data/emir/processed/"
files_per_trigger: "16"
row_tag: "Stat"
table_prefix: "emir_"
```

**Flattening Job (Stage 2\)**:

```
catalog: "esma"
raw_schema: "emir_raw"
bronze_schema: "emir_bronze"
table_prefix: "emir_"
checkpoint_path: "/Volumes/esma/default/regulatory_data/emir/checkpoints/"
files_per_trigger: "16"
```

### Schema Mappings Examples

**EMIR Configuration**:

```json
[
  {
    "field": "Hdr",
    "file_path": "/Volumes/esma/default/regulatory_data/emir/xsd/BusinessApplicationHeader.xsd"
  },
  {
    "field": "Pyld",
    "file_path": "/Volumes/esma/default/regulatory_data/emir/xsd/EMIR_Refit_auth_107_001_01.xsd",
    "payload": true
  }
]
```

## Troubleshooting {#troubleshooting}

### Common Issues and Solutions

#### 1\. Schema Conversion Fails

**Symptom**: XSD to JSON conversion errors in Stage 0

**Causes**:

- XSD file not found  
- Invalid XSD syntax  
- Unsupported XSD features

**Solutions**:

```py
# Verify XSD file exists
dbutils.fs.ls(master_xsd_path)

# Check XSD is valid XML
with open(master_xsd_path, 'r') as f:
    content = f.read()
    print(content[:500])  # Check first 500 chars

# Check Scala conversion logs
# Look for specific error messages in notebook output
```

#### 2\. Row Tag Not Found

**Symptom**: "Row tag 'X' not found in payload XSD"

**Causes**:

- Incorrect row tag name  
- Row tag defined in different structure  
- Row tag in nested complex type

**Solutions**:

```py
# List all element names in XSD
import xml.etree.ElementTree as ET
tree = ET.parse(payload_xsd_path)
root = tree.getroot()
elements = [elem.get('name') for elem in root.iter() 
            if elem.tag.endswith('element')]
print("Available elements:", elements)

# Check payload structure
# Look in Document -> SubElements -> Row Tag
```

#### 3\. Corrupt Records in Raw Table

**Symptom**: `corrupted_record` column has values

**Causes**:

- XML doesn't match row tag XSD  
- Malformed XML syntax  
- Encoding issues

**Solutions**:

```sql
-- Check corrupt records
SELECT file_name, corrupted_record
FROM esma.emir_raw.emir_raw
WHERE corrupted_record IS NOT NULL;

-- Check rescued data
SELECT file_name, rescued_data
FROM esma.emir_raw.emir_raw
WHERE rescued_data IS NOT NULL;
```

```py
# Disable validation temporarily to debug
.option("rowValidationXSDPath", "")  # Remove validation

# Check raw XML content
dbutils.fs.head(landing_path + "problem_file.xml")
```

#### 4\. UDF Extraction Fails

**Symptom**: Null values in `hdr_pyld_metadata` column

**Causes**:

- XML file doesn't contain header section  
- Row tag not found in file  
- File encoding issues

**Solutions**:

```py
# Test UDF directly
test_path = "/Volumes/.../sample.xml"
result = extract_hdr_pyld_metadata(test_path, "Stat")
print(result)

# Check if row tag exists in file
with open(test_path, 'r') as f:
    content = f.read()
    if '<Stat' in content or '<Stat>' in content:
        print("Row tag found")
    else:
        print("Row tag not found - check file structure")
```

#### 5\. Missing Foreign Keys in Bronze Tables

**Symptom**: Child tables exist but `_parent_fk` columns are null

**Causes**:

- Parent surrogate key not generated correctly  
- Join condition issue in flattening logic

**Solutions**:

```sql
-- Check if parent keys exist
SELECT COUNT(*), COUNT(_sk)
FROM esma.emir_bronze.emir_base;

-- Check foreign key values
SELECT _parent_fk_base, COUNT(*)
FROM esma.emir_bronze.emir_base_OthrPty
GROUP BY _parent_fk_base;
```

```py
# Debug surrogate key generation
df_debug = df.select(*hash_components)
df_debug.display()

# Verify hash is consistent
df_debug_with_key = df_debug.withColumn("_sk", md5(concat(*hash_components)))
df_debug_with_key.display()
```

#### 6\. Empty Bronze Tables

**Symptom**: Bronze tables created but contain no data

**Causes**:

- Raw table is empty  
- Flattening checkpoint issue  
- Schema incompatibility

**Solutions**:

```sql
-- Check raw table
SELECT COUNT(*)
FROM esma.emir_raw.emir_raw;

-- Check bronze table
SELECT COUNT(*)
FROM esma.emir_bronze.emir_base;
```

```py
# Clear checkpoints and reprocess
dbutils.fs.rm(checkpoint_path + "/emir_base", recurse=True)

# Check schema compatibility
raw_df = spark.read.table(f"{catalog}.{raw_schema}.{raw_table}")
raw_df.printSchema()
```

#### 7\. Performance Issues

**Symptom**: Processing is slow or times out

**Causes**:

- Large XML files  
- Too many files per trigger  
- Insufficient compute resources

**Solutions**:

```py
# Reduce files per trigger
files_per_trigger = 4  # Lower from 16

# Enable file events for better performance
.option("cloudFiles.useNotifications", "true")

# Use larger cluster or serverless compute
# Adjust in Databricks job configuration

# Monitor streaming query
query.lastProgress  # Check processing rate
query.status  # Check current state
```

#### 8\. Schema Evolution Errors

**Symptom**: "Schema mismatch" errors when writing to tables

**Causes**:

- XSD schema changed  
- New fields added to XML  
- Incompatible data types

**Solutions**:

```py
# Enable schema merging
.option("mergeSchema", "true")

# Evolve table schema
spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.{table}
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Recreate schemas from updated XSD
# Re-run Stage 0: Schema Processing
```

## Best Practices

1. **Use File Events**: Enable managed file events for faster file discovery  
2. **Tune Micro-Batches**: Adjust `files_per_trigger` based on file size  
3. **Use Liquid Clustering**: Create bronze tables by common filter columns  
4. **Monitor Corrupt Records**: Alert on `corrupted_record` column values  
5. **Check Rescued Data**: Review `rescued_data` for unparsed content  
6. **Test Schema Changes**: Validate new XSD versions before deployment  
7. **Use Unity Catalog**: Store data in Unity Catalog volumes

## Appendix

### Additional Resources

- [Databricks Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)  
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)  
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)

---

## Version History

### Version 1.0.0 (October 2025)

**Initial Release**

- Complete three-stage XML ingestion pipeline (Schema Processing, XML Ingestion, Flatten & Explode)
- Support for EMIR and MiFIR regulations
- Databricks Asset Bundles (DABs) configuration for deployment
- Unity Catalog integration for data governance
- Auto Loader with XML schema validation
- Automated flattening and exploding of nested XML structures
- Production-ready streaming architecture
- Comprehensive documentation and user guide

**Key Features:**
- XSD to JSON schema conversion with specialized payload and metadata schemas
- Row-level XSD validation during ingestion
- Automated surrogate key generation and parent-child relationships
- Schema evolution support with `mergeSchema` capability
- Corrupt record handling and rescued data columns
- Checkpoint-based streaming for exactly-once processing
- CI/CD integration support (GitHub Actions, Azure DevOps)

**Known Limitations:**
- Requires manual XSD file placement and schema mapping configuration
- Bundle configuration must be customized for each deployment environment
- Large XSD files may require performance tuning
- Schema evolution requires re-running Stage 0 for updated XSD files

[image1]: <data:image/png;base64,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T5o0qQWjB8g9luvF6lWapqkGPOzixo2rGusxY8bArTJRo34UNnADASF27Ni4VAb+MacBZ1igAY10oHYEEyNGdFseDD/whzgRDkKBpmmqEY54YbdgwQIZNHiwVKhYQU1wR96Rb7ghLO6hJoV0cA97hMUIA+5hn0YvO+xxb+QJ5Y0WLRqslEmcOJE644AwCB9HLxuuDTvEg3CapgncYW+fd9xDoDDcUB77csaKFdMWDn7hhnyhDKgHI2+4hjsMuEHAMfIBOxoScE8CLDUJkAAJkAAJkIB3AqYVQLxn1Cr32OwFqkkDBwyQ7+3mpFgl/8wnCZAACZAACbgEARaCBEjAtAQogARR1WiaptSpgih6RksCJEACJEACJEACJEACpiTwtUxRAPkaIbqTAAmQAAmQAAmQAAmQAAk4jAAFEIehZEQk4J0A70mABEiABEiABEiABLwToADinQjvSYAESIAErE+AJSABEiABEjAtAQogpq0aZowESIAESIAESIAErEeAOSaBrxGgAPI1QnQnARIgARIgARIgARIgARJwGAEKIA5D6T0i3pMACZAACZAACZAACZAACXgnQAHEOxHekwAJWJ8AS0ACLkzg1KlTsmrVKptZt26d7dre3szXu3btktevX7twLbFoJEACvhGgAOIbHbqRAAmQAAmQgIkITJs2TZYuXSZp06a1mZUrV0nyFCls9/Zuzrj2S5q3bt2SevXry7t370xEl1khARIILgIUQIKLNNMhARIgARIggUASOHnypHh69pX06dPbTKLEiSR9unS2e3s3s15Xq1ZNvvv2W7ly5UogiTA4CZCAHQHLXFIAsUxVMaMkQAIkQALuTiB+ggTi4eHhEhiSJk0qz549c4mysBAkQAL+I0ABxH+86NsKBJhHEiABEiABEiABEiAB0xKgAGLaqmHGSIAESMB6BJjjoCXwXB8xuHTpktibJ0+e6Pd/6carvb0fM17fvnMnaGExdhIgAdMSoABi2qphxkiABEiABEjAK4F79+7J4cNHbOaP//1Prl69Kv87+j+bnb27ma8vXbzotXC8CywBhicByxCgAGKZqmJGSYAESIAE3J1A4iRJpHr1ajZTtUoVyZAhg1SpXNlmZ+9u5uvcuXO7e3Wy/CTgtgRcTwBx26pkwUmABEiABEiABEiABEjA/AQogJi/jphDErAMAWaUBEiABEiA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9SZRokSyY8cOGTxooFSvXl36enoGetQGZYWg49c80B8JkAAJkAAJBBcBqmAFF2mm4woETCyAuALeL5cBowtfdvWbS926dWXv3r0yZeo0KVKkiFy5ciVAjXyoc/3y66+SIUNGyZkzpxI8IOT4LRe++4oSJYpSx/qpcWOJGTOWLjSd9D2AL65FixYVCiC+AKITCZAACZCA0whQBctp6JmwBQlQALFgpXnPct8+vWXbtm2yf/9+iRgxosyZM0du3bolUKX6Uo8M7DF/A35jx44jifURizNnTkvKlCm9R++Qewg0Dx7cl40bN0nNmjUFoy0OiZiRBA0BxkoCJEACJOAvAlTB8hcuenZzAhRAnPQAQDhwdNLVq1dXG/VVrFhRDh0+LGnSppUIESJI+QoVZPz48TJ5yhRlunXrpuwbNWokZcqUkXv37krGjBkdnR0f4+vSpbMMHz5cGjRo4KO7b5avX7/2zZluJEACJOASBFgIaxJAx541c85ck0DwE6AAEvzMVYoTJkxQ56A4QO3px7Jl5dLFi0ogWbN6tbRp00ZaNG+uzNChQ5X9okWLAjzHIzD5xmjIkydPJHHixP6KBittcUK7v5DRMwmQAAmQQDAQgPABFSycgyE5JhF0BBhzMBGgABJMoJmMVwKYywFByast70iABEiABEjAWgQeP34snTp1kmnTpkmrVq3k7t271ioAc0sCTiBAAcQJ0E2fJDNIAiRAAiRAAiTgJwLoTPv36TM5cfJPefvunVM0C/yUUXoiARMRoABiospgVkiABEiABEiABKxHIFfOnPLixQspX66c9TLPHJOAEwhQAHECdCZJAiRAAiRAAiRgOgIBzlCRIoXlypXL4ogl9gOcCQYkAQsRoADipMr68MFJCTNZEiABEiABEiABhxLAoiqdO3WScOHCOTReRkYCrkrgcwHEVUtqsnJ16NDeZDkyf3b69esn4cOHN39GmUMSIAESIAG3I4DVJt2u0CwwCQSQAAWQAIJjMBIICgKMkwRIgARIwJoENE2zZsaZaxJwAgEKIE6AziRJgARIgARMR4AZIgESIAESCCYCFECCCbT3ZH7/fb93K95/hcCRI0fk9evXX/FFZxIgARIgARIgAWsRYG7djQAFECfV+MGDB5yUsnWTXb9+vbx9+9a6BWDOSYAESIAESIAESIAEhAKIiR4CZoUESIAESIAESIAESIAEXJ0ABRAL1fC7d+9kyZIl0q1bN0ub0aPHyL179yxEnll1AwIsIgmQAAmQAAmQQDARoAASTKAdkUz79u0lTdq0MnToUEubpk2bSOPGP8mrV68cgYVxkAAJkAAJWJoAM08CJOBuBCiAWKjGY8aMKd99+62FcuxzViNEiCAVKlaQK1eu+OyBtiRAAiRAAiRAAiRAAkFPwEkpUABxEviAJBsyVKiABDNlmPDhwnEExJQ1w0yRAAmQAAmQAAmQQNASoAAStHy/GDvUqb7oSAcfCQThTug+pkdLEiABEiABEiABEiABxxOgAOJ4poyRBEiABEjAzwTokQRIgARIwN0IUABxtxpneUmABEiABEiABEgABGhIwEkEKIA4CfyOHTuclLJ1k927d6+8fv3augVgzkmABEiABEiABEiABLgRoYg45TE4duyYU9K1cqJbtmyRt2/fWrkIzDsJkAAJkAAJkAAJuD0BjoC4/SNAACTgTAJMmwRIgARIgARIwN0IUABxtxpneUmABEiABEgABGhIgARIwEkEKIA4CTyTJQESIAESIAESIAEScE8C7l5qCiDu/gSw/CRAAiRAAiRAAiRAAiQQjAQogAQjbPukNC3w6F++fCnv379X0b57906tEIVVot68eaPscMCkbfj78OGDjzuPw/7ps2fy9OlTFR5hvmZevXolCPc1f/buyIP9/cdrHkmABEjA/ATOnz8v8eInkHjx4tnMjRs3bBkfMGCAzd7ez4gRI2x+/vrrLx/9wL9f4qpUqZItrjNnznwxrnv37tn89enTx0d/1atXt/nBBfLgk3ny5AmclflSXHXq1lXuxsGneGBnH1e3bt18zFfjxo2NaNQZ4Xwyo0aNUu74Dn0prhYtWio/OMCfT/HAbsWKn+FFmfXr1/uYL/u48H1FOJ8MwquI9MOCBQt9jKtDhw6668f/vsWFRVc++hJBeX1Kzz6uFy9e+Jgewu3fv9+ISkaOHPlFf4Yn1BXC+WSOHDliePNTXPCM582nuI4fPw5nZfySL3jE78CnuPCbgDuMX+MqUaKEjyyGDBmCaJT5UlyZs2RR7sahYMGCtrhixowpc+bMtbXPDD88eyUQ+Faw1/h450cCbdu28aPPL3vbuHGjjBo9WgkDHTt2FHzgBg0aJD/99JMt0OgxY6R27dqClxM+IDaHTxewL16smCxevFj9eIwPIV7YMPBmnI1rvPSMjxzcYOAGg2sYXMPgGgZhcB8Y069fPwkfPnxgomBYEiABgwDPfiKAzhm8v27+c0Nu3rxpMwkSJLCF7927t83e3k/nzp1tfpInT+6jH/j3S1wrV660xZUuXbovxoXGj+Gxf//+PvpbunSp4UWdkQefTOTIkZU7Dl+Ka8H8+XC2GZ/igZ19XEOHDvUxXzNnzrTFgwuE88ngewd3TdPkS3FNnjwJXpTRNM3H9BB3lSqVlR8cypYt66M/+7hChQrlox/EhfCIB6ZOndo++hutf7PhDuNbXMX07zL8wKC8iN+7sY8rXLhwPqaHMLly5UI0ynTq1OmL/pQH/YC6QjifTLZs2XQfH//7JS74xPPmU1yZM2eGszJ+jQu/A5/iwm9CRaQf/BrXpk2bfGTRvXt3PZaP/78U1/Fjxz56+HTcuXOnLS60j969fycbNmz45MqTTwQogPhEJRjsNE0LdCoVKlSQC+cvyLp16yR69OiSNm1aFSce/ufPnyvB5M8//5RYsWMr+y8d0qdPL02aNJE//vhDpk+fLtevX5e8efNKmTJl5Jk+OlK8eHHB3507dwQfWlzjxYlelW+++UaaNW+ulse9deuWFClSREqXLi2PHj0S9KI0bvyT1KxZ08+jK4ibhsvIgYsAABAASURBVARIgATMQuDGjX+kWbNmZskO8+EiBFgM1yZQS2/3HDx40LULGcjSUQAJJEBnBx82bKhg+L+bPqRt5AW9buglOHDggBQqWEju6oKD4ebT+f79+4J9SXr16i0YKg0TJoz8+uuv0qBBA4FUnz17djl79qzMnTtPGjZsqKLAqMbq1avl9OnT0lD3t2jRIgkbNqzADn7GjRsnGMbs0aO7zJ49Wxdq/lNXUBHwQAIkQAIWIKBpmi2X//zzj6AjBgYdPXgP2hz1i4cPH+rH//5DxQZqsf/ZeL2C2+3btwUG72Go0mJU2qsvUZ1JSMsw3t19ukde4N8nt8DaIY9Q7w1sPH4Nj1Eo+EV5zl+4gEuHGsTr0AgZmdsTCB06tNsz+BoAJwogX8uaa7tDfSqwJcRLE6oBw4cPt41MIE4MRa5Zu06WLVsm1apVhZWvJmTIkBIrViyZMWO6IOyYMWNk/ISJEvvTyEmrVq10txly9+4dgRoBIsM8kMePP+oHR4kSVQkoED4wQhIpUiR4kX///VeiRYsmEGhixYqp7AJzwHApPtiBiYNhSYAESCCgBM6ePSfTpk0XdPCcO3dejfyiIY53MQw6fzAvDwZpQF8enTi4hnBh2OMeBm4T9HftqVOn5a+//haowC5avBhOYu9/+/btsl/vUILqycKFC20CCdJWnvUD4rYPgxFpuHu3172quJFfuOHeMLhHGNwb7sY97GBw//PKlWKvc2/YIzyukQ+Exxn3uIYbwuLeMLiHG+7hF35gcA+Da/jp2bMnbpWgNkfv0IJf7+FwD3vlUT/gHmH1S/Xf+70RNwS/SZMmUV9fUeKBBIKPAAWQ4GPtJaWz+oiCF4sA3EC/MF/+/FK4cGEVGipUHh4eomma5Pj+e8E1RiUgYMDDpUuXZPPmzbJWF07w8oUdDNS3oIMMHVLc46WdLWtWgSAC4SF+/Phy7tw5yZTpo74m1K8Qt4dHCMGLu337dkqF6++//5aiRYsK9HiRZtWqVQWT9yAgXbt2DVEHykDlC3kLVCQM/JEAjyRAAn4iECpUSIkSJYryW7hwIfn22yxKRTVPntyyY8cOSZM2nTRo2FA16uGpSZOmSg314sWLuFUG778CBQpIxYqVlJ64svx0QDz58+eTrFm/U+9uWMN/zpy5VDoQJAYPHizlfvxRatWqJS1btpRDhw7JvHnzJEmSJIJ5AGhc9+rdWypXriwVKlZUC45MmzZNCSo/NWmiVGOrVa+h8ohRa8xVaNCgoXpvIz0YdCoVKlRIsmXLLkePHpMLFy5Kk6ZNJXHixLJq1Sp4EejyI81t27ape+Pw+++/C0bKS5UurYSypnq4jh07CdJBeoirhZ5vxPXzzz+rYPh+4X7YsGEqX/hOYL4i1H+hQvzgwQOpUrWaisP4Xv3660apVr26YD7j48ePlWovdPTRoYc5Kt/p3601a9aoco8fP16SJk2qhEXw6dqtmyrb2rVrBfE3bdpM5bl9hw4CnX+qy6hq4cGBBKJGjerA2FwvKgogFq7TH374Qak/oQiYiJcxY0b1IsUkxM6dOwnsQoQIIfPnzxcIFzjny5dPihUrKrBHOEzqnjBhAi5tBh8CfBTxocBHE2oEMWLEkHLlflR+sLIL0pg8ebLUqVNHzUFJliyZ4ANQsGBB9ZHq0qWL5NeFowUL5gsm0P32228qLA8kQAIkYCUCeLfhvelTnvPkySP79u6RsGHCqBEM+Bk2bKh6J3p69sOtMmgw433Zq1dPpTKrLD8dGjZqrHfuZJEePXp8shE1+rxp00ZB4xrvZ8xBgZorRrVbt24tSfWGNUaE9+zZI6dOndKFhQtqpBmCQs4cOXQB4qjgvY0Ir1+/IRhpKawLTxCKRo4arRrlgwYNhLPNhAoVSqnQzp8/T+9YmqjsE+idT1A7g8ADQWjfvn1qlCbOp9Fx5Uk/ZMmSRSAEwP+xY8cE6icdO3ZQ6UBFWPcikfWRccSFvGAkBzx2794t796/l127dsmJEydk1qxZMnHiRIGA069fP5kwfpzeYbZW0LmFOI4fPyZpUqfGpReDvGuaJsf1tFetWq3mIF6+fFnQaYU5jBiJChsmrC5IrZQlS5cp92jRoytOI0eMkLp164r9RG0vkfOGBAJAwEPvDO5stwhFAKJw+SAUQCxcxXjJG9nXNE299I0XsaZpgmu4w5+maWoFKQgcMLA3DNyNa+OMVTDwA8JIxqJFi6WJ3osGO7gjXk37qBcNO9zDHgb3CGfY4QzjUxrwT0MCJEACViKA3nQjvy1btpIrV69KhgwZDSv1HtY0Tf59+q/NDot54L0bN25cL4IGPMybO0dOn/5TdRi9fPlKNP3f7DlzVIM+Var/GttGujhjJDhkyFBK6IDaK0aw8Z7VNE0fSckqGEFB3DAx9c4jnBMlTKhGDOLHiyuapqeiG9gb5qA+qoKOo1ixYhlWEj/+x5W+MAL07NlzVTY4ohw4G6Zdu3aCEXYsQAI7TdPUCDy+BQn0dEX/w7V+UnMFoSqF0XV0jGGy7nfffScJEyVSYSJGjKRWbUQZoc5rlBcjNFALRhjEg1ERqPniGiaeLizhHD58OFU+dMD9ro/MYFQF6cWMGUPxGjJ4kOA7FduunAhH4zYEWFCTEAhhknwwGyYloGma1K9fT9DTZ9IsMlskQAIkEGQEnj9/Iej9NxLQNE015NEwRmP3zOkz4unZ13AWrPxXqXJlad2qlWoIa5ommEcHVZ/x+mgzeuMNz5qmKZWl/+4/XoUMGVIwyb1p0yYqDrx/x44dq64xaoDG9/v372TFihVqFNoY0UZoTdNsI9yi/3l4eP3MQy0qR46cSh3MQ++l1b2o/6H1ERCUFfMKNU1TdiE+hdU0TVKkSK6EjCFDhuqjOANVXpQn/QDhB3Ni2rVrr9+JUoFq1qy5WgI+Z86cym7q1KnSt29fuaoLbAl1oSRFihSC+SzNmjVTKlgh9NF65VE/QPjASM+Pn9TOkugjPuChaR/zVa1aNanfoIF06dJV9/3xPxY+6dGzp+p4ixAhgrRo0UKxTZgwkeTQR4VmzpwlUOGCuhXSevX6lQqIeYpQLbMX2pQDDyQQCAL4jV7WR+ECEYXLB/X6ZnL54rKAigAPJEACJEACfiJw/fp1sRcasDx51qxZVQMcakTooEHjOHXqVGr+29KlS2TtmjVqPhwa38WKFVPqqOvXrZPhw4ZJ+fLlbenCrVSpUrb7lClTSqNGDeWnxo2lnT6qgB78gQMH6iMR8dWICEYn9u7dK4n00YLFixcrP1DDwqhAj097FyDOSpUqCebnYeQZqrdIAPmGmi7mBW7dukWw6Ag2dIMbDMq0ePEiWbJkie42Q1Ae5ANu3bp1w0kwv6J7927y4MF9fdQng7LDAWk1bNhA/v77L8n2aZ+I5cuXyfLly6WynpfXemN/0KDBArUq2CEMNnjr2rWrQJUMKr7Dhg5VIyxIF0u3Y1n5rVu3ClSBp+nCC8LVr18fQSVTpkyybu1amTVrppqriMVJ5uqjRoMHDRKorEEgmjVrlpozM2nSRFVXJ04cl59+aqxUhDEa1fOTyhv8YiI6mKrIeSABBxD48OGD+h05ICqXjYICiJOqFi89JyVt2WShBqBpH3vALFsIZtztCRCAaxPQNHO/o1KnTi2YiH3hwgUxVpdyVI0YIyoYobD/xkHAyJsvr7+TMeJDwPbt2ytBDNfeTapUqQRqaN7teU8CJGBeAhRAnFQ3GJJ3UtKWTRYfIPT0WbYAzDgJkAAJOJkA3qGN9RGWihUrKnWloMgOFi/RtP8EMYy0ZMr43zwZR6eZJk0aQRp+iJdeSIAETEKAAohJKoLZ+DoBTfvvg/Z13/RBAiRAAiRAAiRAAiRgDgJec0EBxCuPYLvDJLtgS8xFEsJKJi5SFBaDBEiABEiABEiABNyWAAUQJ1X9iBEjnZSydZPFPiPPnz+3bgFEhJknARKwFgEPjxBqcrS1cs3ckgAJOJsAlpp2dh7MnD4FECfVTtiwYZyUMpMlARIgAbckEKBCY7lYbGYXoMAMRAIk4JYEPDw8pE+fPm5Zdr8WmgKIX0nRHwmQAAmQAAmQAAmQQAAIMAgJeCVAAcQrD96RAAmQAAmQgI0A5p69efPGds8LEiABEvALgRcvXvjFm9v6oQASjFXPpEiABEiABKxF4NKlv2Tz5s3WyjRzSwIk4FQC79+/F09PT6fmweyJUwAxew3Z5e/J48eCHV/trILtEqt2YUdgfIhXrVolGzZsEON88OBBuXv3rrx69SrY8sOESMCfBOidBEiABEiABIKFwIcPHyRkyJDBkpZVE6EA4qSaCxs2rL9Trl69upQoUUKuXLni77ABDfBYF3oWLlwoKVOmlF9//VWSJUsmJUuWlCJFiqhzhgwZBD+0pUuXyjfffCMVKlSQnTt3yrNnzwKa5BfDRY8eXTRN+6I7HUiABEiABMxIgHkiARIgAa8EKIB45RFsd82bN/d3WlmyZFFCQI8ePWTOnDlBOuJw7/59GTp0qNSoUUMKFCggly9fliZNmkjq1KklXLhwNpM0aVLJmTOntG7dWi5evCirV68WLD2XOXNmGTZsmNy6dUsJKP4urA8B2rZtq9L1wYlWJEACJEACJEACJEAC3gmY9J4CiEkr5kvZwsgJRiTSpk0rZcuWlaNHjwp0Db/k37/2//77r0ybNk2qVK4sjRs3VgJPokSJ/BVNrly5lDCC8BAaevXuLRBo/BUJPZMACZAACZAACZAACbgkAQogTqrWwKyqommaoJGP+RiLFy+RihUrysmTJwXzNAJaHKhMQbD59ttvpVSpUrJjxw6JGTNmQKNT4WLEiCHLli2TDu3bS5UqVWSpfh2YOSyBYKbywwMJkAAJBIQA1EwDEo5hSIAE3JcA3xu+1z0FEN/5BJnryJGjAh23pmkyYsRwWbFihW5WSunSpWXduvWCeRtYOvJrCeDHAb8LFy6STJkyKQM1Kv+OeHwtHQgiO7Zvl6RJkgjmsFy6dOlrQXx0HzhwoDx//txHN1qSAAmYlYC185U0aRIpVqyYtQvB3JMACQQrgRAhQki3bt2CNU2rJUYBxEk1FjZsGIelHCpUKOnf31MtFYk5GrVq1VIfzKFDhwlWqDp3/rzcv39fHj58qMyNGzdk37590rBhI6lYqZIUKJBfIBRACHFYpnyIKGfOnCqPECTmzZvvgw9akQAJkIC5CIQOHVqg+mquXDE3JOBHAvTmFAKapknkyJGdkrZVEg1hlYwyn34jkDZtGrVE7nZ9xKFz506ClaOOHDmiJq3PmjVLYFauXCkxYsTUr2fKtq1bxdEjHr7lFMISJtC3b99OcO2bX7qRAAmQAAmQAAmQAAm4HgF3EEBcr9b8WCIPDw9JlSqV1KpZUzp16mQzbdq0EQgqGCL0Y1QO91akSBE9D2kdHi8jJAESIAFHEjh//oLq1HFknIyLBEjAtQlgcaCuXbu6diEDWTpMA/fqAAAQAElEQVQKIIEEyOAkQAK+EaAbCZAACZAACbgXAcyx5UaEvtc5BRDf+dCVBEiABEiABKxJgLkmARIgAZMSoADipIqJECGik1K2brINGjTgvBHrVh9zTgIkQAIkQAJuQ4AF9Z0ABRDf+QSZa5MmPwVZ3K4acdKkSSmAuGrlslwkQAIkQAIkQAJuQ4ACiNtUtTMKyjRJgARIgARIgARIgARIwCsBCiBeeQTb3atXr4ItLVdJCMwwsctVysNykECQEmDkDiEQJkwYiREjhkPiYiQkQALuQUDTNIkbN657FDaApaQAEkBwgQ02ZsyYwEbhduEHDx4sL168cLtys8AkQALOI5AkSWLJlSuX8zLAlC1JgJl2bwLY5qB169buDeErpQ/xFXc6BxEB9KoFUdSMlgRIgARIgARIgARIgARMSyAIBRDTlpkZIwESIAESIAE/EXj69Klcu3bNT37piQRIgARAABsRnj17Fpc0XyBAAeQLYILa+vbtO1Qn8ifkf/75RzRN82coN/XOYpMACTiEwD//3JTjx487JC5GQgIk4B4EMF91wYIF7lHYAJaSAkgAwQU2WMmSJaRp06Zy6tQpuXr1qjJ37961Rfv8+XNlZ7gZ5zt37tj84OLKlSuf+YMd3AyDeyO8cYYdfiCGH6RtuNmfkY+v+Xn27JnhReDfPrxxjfgNT1/y41vZlixZIqFDh5Zw4cIZ0fBMAiRAAqYkwEyRAAmQAAn4TiCE7850DSoCBQoUkLFjx8rDh490AeKaMvaN9H///VfZXb360c0437t3z5YlCBDXrl3/zB/s4AaPOOPeCG+cYQd3wyBtw83+jHx8zQ9UFAw/8G8f3rhG/F/z41vZMmfOLJMmTTKi4JkESIAESIAESIAEvBPgvUUIUABxYkVFjx5d8ubNYzPp06e35SZOnDg2+y/50TTNRz/wr2kfVZU07et+kCjSRjjvBvmAO4xf/MC/9zhwj7CIA8YvfjTNa77twyMOGhIgARIgARIgARIgAWsSoABizXrzPdd0JQESIAESIAESIAESIAGTEqAAYtKKYbZIgASsSYC5di0CIUN6SNiwYV2rUCwNCZBAkBOIGDFikKdh5QQogFi59ph3EiABEiCBICWQPHlyKVq0qPDPEgSYSRIwBQEPDw/p3r27KfJi1kxQADFrzTBfJEACJEACJEACJEACJGAJAv7LJAUQ//GibxIgARIgATci8ObNG3n58qW/S7x7927Baoff58glrmYyZc4ijRo3ljNnzvibi6MDHDp0SPLly+dyjPHMgHOtWrXlzz//dDQ2f8eHvXDy5MnjkpzBukyZsnLkyBF/c/EtgP0Kob75c1c3CiDuWvMsd5AQYKQkQAKuReDvvy/L1q1b/VWoq1evyrx582TXrl1yYP8+lzPHjv5Ppk2dKv0HDAiQcOYvmL54fvTokYwcOVIg7Lkq50WLFkq//v3lyZMnvpAIWiek3alTJ9m3b58cPPC7yz3PKNOGDeulX79+gmfKETTfv38vA/TfhyPictU4KIC4as2yXCRAAiTgXgRMU9oTJ05I8+bNVX5ChAghrmhChgwpqVOlcliDTcHy5+Hs2bPSokUL0TTNJRnjuRH9r1jRYnLz5k39yjn/b926LQ0aNFCJa5rmcqw1TRP8tW3bVi5cuIDLQBvswYbfSKAjcuEIQrhw2Vg0EiABEiABEiCBICKABjJU1IIo+q9G6y4NPKzC9vr166/ycK4H66ceKlQoIefgq0cKIMHHmimRAAmQAAmQAAmQAAmQgNsToADiwEeAUZEACZAACZAACZAACZAACfhOgAKI73zoSgIkYA0CzCUJBBkBTCgNssgZMQmQgEsSwDwQlyyYgwpFAcRBIBkNCZAACZCA6xFIliypFC9e3PUK5tASMTISIAF7ApgfxY0I7Yl8fk0B5HMmtCEBEiABEiABRQATUzEJWN3wQAIkQAJ+IKBpmkSKFMkPPh3gxaJRUACxaMUx2yRAAiRAAiRAAiRAAiRgRQIUQKxYa8yzdwK8JwESIIEgIXD+/AXZsGFDkMTNSEmABFyTAOaNdevWzTUL56BSUQBxEEhGQwIkQALuSYClDgiB27dvy40bN+TJkyf+Dv7w4UMvYe7evSv//POPPHjwwIs9b/xO4P79+4oh6uTFixfy6NEj4SRi3/kZzxs43bt3z+YZz6NxAzf7e8Pe1c8ot4eHh6sXM1DlowASKHwMTAIkQAIkQAL+J1C0aFHZs3ev5M2bVw4fPqwiQK8pGi7qRj/gGnb6pe0/7Hr16iWGPe4rV64s+/fvl3r16su6deuUX9i/e/dOXcMvDG5gj7PhZlzj3t6PcQ13VzdgUqtWLcVwr14naEx7evYTCCIoO9jgDDN27FglmMyYMUOePn0KK3WPOHBjnO3DwD7IjBMjHjRokLx8+VIgTMeKFUtdP3z4SPr376+eT7B48+aNnDlzRiDgzZ0715Zbn/hMmDBBxQFPCOv9GYTgPXv2bMUbfuzjgF+EgT2M93t7v9OnT5d///0X3micSIACiBPhM2kSIAESIAH3JJAmTRqpXq2abNmyRVauXCk3b96UypWrSJMmTdXIyLNnz6R6jZpSvXp1OXr0qKAhN3nyZKlTp45cunTJC7TUqVNLuXLlZMKE8YIG9LVr16RFi5YCFZALFy4otzJlyqge/gULFsrIkSMF9xBarl69Jrlz55Zvv/tO0AhHOr1795a6devK7t27vaTjqjeapkmMGDGlQoUKeh1UlgQJEqgJxGjQzp8/X7Jnzy6zZ8+RAwcOyMCBA6V06TLSq1dv6dq1q6B3v1q16tKgQQNVL7t27ZKhw4ZJiRIlBdeuygzlihY9uly5ckV27twpHTp0kD///FM3J6VixYrSpk1bad68uZw9e1a5T9CFC/DaunWret6zZMkiU6ZMVYIK4sJzCMEFfjCyUqZsWZ1hCTlx4gSclRk+fLj07NlTduzYIStWrJAcOXLIiBEjlBue2fr166vfztSpU6Wa/tvCbwc7m+/Zs0cKFS4sEydOlDt37gj8Ir+oXxWYB6cQCOGUVB2bKGMjARIgARIgAUsRwNyShQsXqoYrhAf0Jnfo0F4XMGqrHmSob4waOUIXJFoIGsGLFy9WPb8I8/z5C1tZNU2Tv//+WwoUKKAabLVr11ZusWPHUo2zbt26q8banDlzZPDgwcotZMiQsn79etm4cZMkTpxIDh48qAQST09PWbVqlcSLF186duokM2fOlLdv36owrn64efMfadmypeKNBjDKq2maWoIZ7GbOnCE5c+ZUjetff/1Ffmryk6BBPGTIEGnVqqU0btxY3SNc0qRJZfPmTYJREty7qqlapYrgufzjjz+kXbt2snXbNlm9erVAuHj06KFgtCht2rTyrz5S1KJFC6lZs6YSdiHMzZ03Twkv586dU3jwHOJ3AJ54TqdMniK//fabEhaUB/0A4aRhw4ZSWBcmChYsqNI+dOiQYGTk1evXMmvWLCWsY+QKAgqEeD2YdO7cWcaMGSMYBTl+/IQSMiG4aJoGZxonEaAA4iTwTJYEXIMAS0ECJBAQAkmTJVWjG/HixZPMmTPL06fPBL21oUKFltatW8uxY8elT58+EjVqVBU9em7Tp0+vrmPGjKHOxiFhwoSyb98+1fhavHiJsk6RIqU6oyEYJkwY1aMPNRhYfvvttwIhxMPjYxNgzdq1El/PB0Zl0FgMGy6svHzxUn766Sd4dwsDoWvKlCkybdo0fTTkI18IX/Xr11fzdBImTOSFw/Nnz9Wo1KNHj/R6eyMeHiF1QaSVvHr1SlKmSKEau0l1QcRLIBe7SZw4sUAYTqg/f4kSJRI0+FH+aNGiSbz48cX78tVQi4LKFpa2fv3qtS70lpbYsWN/RuVjHFGVfdhw4dQZB4Q3TNu2beXixYvq94FRu7hx4qhn+q+//pI4+jX8x4gRQwnQESNGFKSXNWtWwWgh3GicT+Dj28f5+WAOSIAESIAESMB0BMKHDydx48YNknxBCECvcN++fZXKyNp16+Ts2TN67/lmvQH7VtCTvHr1GpV2vXr1ZO7cuWpU4o8//qfs5NMRjeBJkybpvbxj9cZz9E+2H0+NGjUSbIjWrVs31Uv/6vWrjw76UQsRQq5fv656/TVNk6VLl0qlihVlu96T/ddfl9RoCPKoe3X5/x4hPWxl1DRNFypeq/kIqPubN2/Kzp07lDuEOaj0xIgRXakSYcQJKnR//nlSfv31V+UHBw8PDyWM4NpVTdiwYeXWrZu6EPJcFfHVy5f/CRQflJX+HL+TEPpzFj58eLly9ZoSUmLFiqkL2EfVs4xRiY8+RTBa8vPPPwtGORo1aqxG7HLmyGE4C+I4ceKkQK0QQg/mmxzQR+/gwYgHYecvWCBQgzt2/Ljg+c2VK5ecOHFcZs+eLR4eIVQ6S5YskaBUwdI0TZIlS4as0XyBAAWQL4ChNQmQAAmQAAmgoZMtWzaHg1i8aJGKs0iRIgIBpGTJEjJ0yBCpUqWKdOzYUfLkyaMaYr1791INMfQUQwDBPA0IKSH0Rp2KQD9ALQsN4c6dOyl1E4yqVK1aRXcRpfbSq1cvGTZsmCCtBvXrq7jh2KF9e4mrC1dnTp8WhMe8EPRez5s3T80bgdoK/LmDmawLcPbl9PTsK7FixVKN5FKlSil1IbiPHz9eMIKEuQgQHqEONHr0KKlRo4aaEwLGEBw1TZMBAwYgiEsbqJl17txZlRFqhD169FDXffv2Uedw+ghGb/35wyjEzyuWK6YIA3ZQ8cNzrTzqh06dOqnnH3znz58nbdq0kfb6M6o7qf9hdYFn7do1kiRJEvU8lyv3oxw+dEjFiREReMLoypDBg6VZ06aSMUMGJfxgbgl+N5h8jt8zRhgxZ0fTNAQJsPEtIH6fUMvzzY+7u1EAcfcngOUnARIgARIIdgJokBmJGtdorBnXcMM1enBhj3tN0wTXMLg3DPxFiRJF0PiCHcKgsYZrGLgbYeAGY9jjGmFh4A/2sDOucW8Gg3ku6B3HaARU1RydJ+/lBS9N01QPOtxwjzQ1TRPc49qww9mwAzsYTdM+U0FCGLObW7duySJdOMakcL9wxnOG8qJceP5gcG3wsL+GP/iHHZ433OPa3oAl7uHPPg7YwSAM3DTtYz0Y1zjDHSNUp3WBGiMgmGgO/5r20S+u4QfG8I9rGucQoADiHO4ukiqLQQIkQAKuTQATXLGqlGuXMuClw9wU7J0R1AaCx5ix46R8hYpSqHARNSpkTGAWF/8L4RFCLWMb1IwRPwSQyVOmSpWqVRXn3r17y8mTJyxFGKMdGE387rvvnJZvzFU5duyY09K3QsIUQKxQS8wjCZAACXgnwPtgIXD37j05fvx4sKRlxUTQYMWytUFtIkWKpOZloOccKnEVKlQQTJq3IjP/5jlUyJBqmeagZoz4Y8SIoThH1nln1RvwVXVBJGPGTP7Nstv7//Dhg1p9zu1B+AKAAogv2Eo8CgAAEABJREFUcOhEAiRAAiRAAmYkgB5WM+QrU6bgaZxC8BgzepRs3rRRxo0dI+nSpXNY8QPC0mGJ+yGiV69eS4YMGfzgM/BeoEI1Yvgw2bjxVxk/fpxkzJgx8JEyBhLwgQAFEB+g0IoESIAESIAEzEgAIw7YSBAjANgDAXmEzjvOrmyw5GvevHlF0xw7cRh7oaCXf/jw4WrJVu8Msakd1PC827vqPRYlKFiwoJr7ElRlPH/hgo8rUGGeD/ahsU8XE8jt73ntOgQCIYC4DgSWhARIgARIgAScSQD7I/z660ZZv2GDagjjHhux7dy500tjDRu1YcWqtWvXyt69++TEiRPSokVLtSfCgQMHZdOmTWq+AOyxrK6x98epU6fUMrvnz59XxYTwgkndWNJ0y9atslU3ysGNDhAssHEeOECtC2ww8Rr3qAdcY+7JmTNn1JLEz59/XG72f//7n9pw79GjR2oJ2v0HDsiCBQsFwuGixYvF0P2/fv2GIK4///zTjaj6XtSnT59Kl86dBc8vls7dvn27Ygn7N2/eytmz5xTr27dvq4gOHz6szsbzjPqABYTuNWvWCOYg4Z7GegQogFivzphjEhAhAxIgAZcisHz5ckmYKKG8fvVKCQO49/DwkP37D8jly5dtZS1Ttqx06tRZ0Phq0aK52msgVaqUgo3gJk2aKPHjx1eN4okTJ0q+fPmkfPnySoCZO3euup89Z44ScAYPHizYmK1GjZqSMEECWb9+gxiNPltiLn6BJYcfPHggkydPllixYgs2euzTt69aaSxSxIiC5Y2B4OTJk4K5EfPnzxc0fFE333zzjVomFg3nwYMGSeHChSR37jxSsEABtR8LNsebO2+uFNDvsTwt/CEudzdYfQqjWTly5BAsIgCBIlWqVNKvXz+FZt++vYLJ4yVKlFRzUWCJ5xIjITlz5hQs8/vixQuZMnWaWg551qzZ8EJjQQIUQCxYacwyCZAACZBA8BAIFSqkRIgQwUtiQXGDBu2vv/wis2fPETResZ/EkKHDBBpH2I/CSBMN3Llz5wgmDDdt2lTlLXTo0ILlR7NkyaLmCqBhd+7ceRk8ZIhEiRJV0PhNkSKFjB49Rvbs3qM22EPDGBul5cjxvZpPUbRoEbfrTUYP/LJlSwWrJg0fPkzOnj0rIXWhr0SJElKwYEG1Dwu4Q90Nm9mBK4SR02fOyrjx43VB74XabBDzYFAfZcuWUfUSMWIEwehKiuTJZdiw4XJAHyF5+/YtonJ7A6EaS+1GjRpVCct//31Z+vcfoHgBTomSpSRp0qRSvHgxMVaf++OPP+T6jX9kxMiRgkUhwDJe3DgybPhwgVoewpnRQMA1Y77MkicKIGapCeaDBEiABEjAdATQSC9UqFCQ52vKlCmqIVypUkWVFlSjZkyfpnZ//v3335UdVtZp166dnDhxUmLGjCkv9dESOKCHGOpBcMd96tSp9cZdPPHs6ynVq1dTu1GfOHFCHznpKImTJIEXGp0A1NPatGmr97S/UaNBr3SemJC+bds2wW7nUGfTvXn5j976HN9nlz69+0jNmjWU4Gd4QFjjGo3kXbt2SZcunQWb3xn27n7WNE2NMEElEGpuEISbNPnJhuXXXzbI1atXlfBhcMMqXN+kT6ee53r16gqEmCT6czxwwAB9RKS7LayZLpDHTp06mSlLpssLBRDTVQkzRAIkQAIk4G4EMLF8/vwFkixZckmu95xD3eTXX39V18WKFVM4NE0TTJbevXuXLF68WKZNnSohQoRQqkD79u2TzJmz6CMmmkDFBZuwTdcFmBw5cgh6m8uVKycrfv5ZKusCDjZkg5oLIs2dJw9OKp3o0aOra3c5YGSpfv16Ak5YVStz5sx6b3x/NZcDE6KLFi0q2bN/r0aZwLmAPioCblBtmzlzpoAtRsdKlSqlkGHUBBcVK1aUyJEjS7169WTO3LnStm1b1eiGG41I586dZe/evVK1ShVdmD4umO+E5zNChPC6QNFDoOI2ffp0JdzVr99AEiVKKBC8UU8YbQofPrx6XmfPni0rV64kUosSoABixYpjnkmABEiABIKFACYiY3QhqBODkNCzZw8pVKigQB0LuvItW7aUMmXKKKHCSD9ChAiqMdazZ08lWMA+f/78AiGlWLGiNr/Zs2cX+EGPPfz88MMP0qZ1a6lUqZIgbvQ8w75WzZo4qTShRqRu3OSgaZouYHzkBD4oNoSzunXrCgyuy5QprQQQXJfXhTj4Ae+eel2BF+ojzychDmzhDvW5iBEjCtS2unbpIpUrV1aNabjRiOIJgQJqg8YzXrZsWaW+BuEOIwfgB1aVdIEZZywH3FN/5o29X8AW/mLHjg1nUxosUmDKjJkkUxRATFIRzAYJkIA1CDCX7kXg8uUrgpV63KvULC0JkEBgCEAdb+jQoYGJwuXDUgBx+SpmAUmABEiABEjAJQiwECRgCQKYj4V5IJbIrJMySQHESeCZLAmQAAmQAAmQAAmQAAlYg4Bjc0kBxLE8GRsJkAAJkAAJkAAJkAAJkIAvBCiA+AKHTiTgnQDvSYAESIAESIAESIAEAkeAAkjg+DE0CZAACZBA8BBwWiqYUOq0xJkwCZCAJQlgHoglMx5MmaYAEkygmQwJkAAJkID1CCRLhl2Zi1sv48wxCTiUACPzDwHsG9OjRw//BHE7vxRA3K7KWWASIAESIAG/EggVKpTaN8Ov/umPBEiABDRNE2MvE9LwmQAFEJ+5+GhLSxIgARIgARL4GgFsWnfv3r2vebO8++3bd9Smcs4qCHqZb9y44azkgy3dU6dPS/To0YMtPe8JeXiEkEePHnm3drn7U6dOSYwYMVyuXGYtEAUQs9YM80UCJGBPgNck4BQC589fkA0bNvgr7UKFCsmoUaNk27ZtcuXKFZczf//9t6xYsUJevHju1IZxpkyZZNy48bJp0yaXY2w8N+vWrZMzZ05L/Pjx/fUMOtJz8uTJVX0jL0a+XO2M3+qIESMkTZo0DkGHeWPdunVzSFyuGgkFEFetWZaLBEiABEjAKQTChAmjhBZNCyH79+93AeO1DAcPHpREiRLJnDlznMLXSBQjTQcPHpDw4cO7HGPjuYkaNaqsW7tWNE0ziu2U8/bt2yV27NguyzlkqFBy6dIlh3HGBHQPDw+n1JVVEqUAYpWaYj5JgARIgAQsQwBCSOHChaR69eouaXLmzGmKutA0TfLnz++SjPHsoGzOBq1pmmqYo86RJ1c0BfRnKEQIkzaJnf0ABFH6pB1EYBktCZAACZAACZAACZAACZDA5wQogHzOhDbmI8AckQAJkAAJkAAJkAAJuAgBCiAuUpEsBgmQAAkEDQHGSgIkQAIkQAKOJUABxLE8GRsJkAAJkIALEcBa/gkTJnShErEoliLAzFqSgKZpkj59ekvmPbgyTQEkuEgzHRIgARIgAcsRiB8/nmTJksVy+WaGSYAEnEcAE9pr1arlvAxYIGUrCCAWwMgskgAJkAAJkAAJkAAJkAAJ+IUABRC/UKIfEnBbAiw4Cbg3gfv378tff/3l3hBYehIgAX8RwEaEBw4c8FcYd/NMAcTdapzlJQESIAES8DOB+/cfyOnTp/3s36EeGRkJkIAlCWAjwvXr11sy78GVaQogwUWa6ZAACZAACZAACZAACViCADMZtAQogAQtX8ZOAiRAAiRAAiRAAiRAAiRgR4ACiB0MXnonwHsSIAESIAESIAESIAEScCwBCiCO5cnYSIAESMAxBBgLCZAACZAACbgoAQogLlqxLBYJkAAJkEDgCYQOHUoiRYoU+IgYg6UIMLMkEFgCsePECWwULh2eAohLVy8LRwIkQAIkEBgCSZMmlQIFCgQmCoYlARJwMwIeHh7Stk0bNyu1/4rriwDiv4jomwRIgARIgARIgARIgARIgAS+RoACyNcI0Z0EnEGAaZIACZiCwMuXL+Xff/81RV6YCRIgAWsQwD4g9+7ds0ZmnZRLCiBOAs9kSYAESIAEzEnAPldXr16TXbt22VvxmgRIgAR8JQABZOTIkb76cXdHCiDu/gSw/CRAAiRAAiRAAiRgDgIukQsIIJgH4hKFCaJCUAAJIrCMlgRIgARIgARIgARIgARI4HMCFEA+Z+J8G+aABEiABEiABEiABEiABFyUAAUQF61YFosESCBgBBiKBEiABEiABAJDQNO0wAR3i7AUQNyimllIEiABEiCBgBJ49+5dQIMynP8I0DcJuAyB9+/fu0xZgqIgFECCgirjJAESIAEScAkCSZMmkZIlS7pEWVgIEiCB4CGgaZr06tUreBJzWCrBGxEFkODlzdRIgARIgAQsRCB06NACY6EsM6skQAJOJqBpmkSIEMHJuTB38hRAzF0/zF0wE2ByJEACJEACJEACJEACQUuAAkjQ8mXsJEACJEACfiNgSl/nz1+QTZs2mTJvzBQJkIA5CWD+R8+ePc2ZOZPkigKISSqC2SABEiABEjAngbdv35ozY8wVCTiMACMigeAlQAEkeHkzNRIgARIgAYsR4I7GFqswZpcESMD0BCiA2FURL0mABEiABEjAO4GHjx55t+I9CZAACXyRwN27d7/oRoePBCiAfOTAIwmQgHMJMHUSMCWBhAkTyKKFi+TAwYPy9OlTm4GOt5Hh169f2+zt/Tx//tzwIvBv72Z/DTfDo1/igl/78PbXQRXXy5cvv1pG3/KFcsEdxpFxQT3OvvzGNdJAWoYx7L2f7fMV2LgQ3kgP8XpPC/f2+frw4YOPTOHPL3HBn5GeI+NCnHh2Eb934998BVdc4I20YHDtPd/GPdwN86UyIrzhB78nI6z3s+EHZ/u4bt68KZ6e/aRatWpwovkCAQogXwBDaxIgARIgARIIHz68LFgwX37bvFnatGljM48ePbLBWb9+vc3e3s/IkSNtftAjau9mf20f19q1a32Ma+LEiba4cGEf3v4ajSS4w6xcudLHuKZMmQJnm2nT5r9yfSmu1atX+xjX9BkzbPHgwj68/fXGjRvhrMyKFSt8jGvOnDnK3TjYh7e/to/r1KlTPsY1b958QaP8a3EdOXLE8CJHjx79YlyGp3fv3vnoB/lDeMMf4oWdd7N48WLDixJKvbsb9yiX4RHlNey9nw0/b968+WK+Ll26ZHgTv8QFz6hX72nh/sqVK3BWxq9x4XlDWO/m+vXrKh4c/BoXfgfe48E9fjeIB8avcY0dO9ZHZvg9Ix4Y/DYRv3fTu3dvONvM4MGDbXFh8nnTpk0kU6ZMNndefE6AAsjnTGhDAiRAAiRAAjYC0aNHlz59+sjs2bNtBnaGh0qVKtns7f0gjOEnTpw4PvqBf/u4qlSp4qO/Ll26GFGpM8L5ZCJHjqzccahRo4aPcXXs2BHONuNTPLDzS1zt2ra1xYMLhPPJlCtXDs7K1KlTx8d8tWzZUrkbB5/igZ19XJkzZ/YxLjQANU0zovLRD+LKnTu3zU/27Nl99Ie4DE+YD4RwPhmEN/whXp/8NGLi3vAAAAXDSURBVGzY0PAivsWFchkeUV6f4oKd4Qd71eDeJ5MmTRrDm/glLnhGvfoUV4oUKeCsjF/jwvPmU1xJkyZV8eDg17jwO/ApLvxuEA+MX+Pq0aOHj/WN3zPigcFv06f0xowZA2ebGThwoJe4smTJYnP76oWbeqAA4qYVz2KTAAmQAAmQAAmQAAmQgDMIUABxBnWm6Z0A70mABEiABEiABEiABNyEAAUQN6loFpMESIAEfCZAWxIgARIgARIIXgIUQIKXN1MjARIgARIgARIggY8EeCQBNyVAAcRNK57FJgESIAESIAESIAESIAFnEDCDAOKMcjNNEiABEiABEiABEiABEiABJxCgAOIE6EySBMxDgDkhARIgARIgARIggeAlQAEkeHkzNRIgARIgARL4SIBHEiABEnBTAhRA3LTiWWwSIAESIAESIAEScFcCLLdzCVAAcS5/pk4CJEACJEACJEACJEACbkWAAohbVbf3wvKeBEiABEiABEiABEiABIKXAAWQ4OXN1EiABEjgIwEeSYAESIAESMBNCVAAcdOKZ7FJgARIgARIwF0JsNwkQALOJUABxLn8mToJkAAJkAAJkAAJkAAJuAsBVU4KIAoDDyRAAiRAAiRAAiRAAiRAAsFBgAJIcFBmGiTgnQDvSYAESIAESIAESMBNCVAAcdOKZ7FJgARIwF0JsNwkQAIkQALOJUABxLn8mToJkAAJkAAJkAAJuAsBlpMEFAEKIAoDDyRAAiRAAiRAAiRAAiRAAsFBgAJIcFD2ngbvSYAESIAESIAESIAESMBNCVAAcdOKZ7FJwF0JsNwkQAIkQAIkQALOJUABxLn8mToJkAAJkAAJuAsBlpMESIAEFAEKIAoDDyRAAiRAAiRAAiRAAiTgqgTMVS4KIOaqD+aGBEiABEiABEiABEiABFyaAAUQl65eFs47Ad6TAAmQAAmQAAmQAAk4lwAFEOfyZ+okQAIk4C4EWE4SIAESIAESUAQogCgMPJAACZAACZAACZCAqxJguUjAXAQogJirPpgbEiABEiABEiABEiABEnBpAqYUQN6/fx8k0BkpCZAACZAACZAACZAACbgTgUePHomHh4epimw6ASR79uwyfcYMef78ualAMTMkQAKBIsDAJEACJEACJEACwUjgw4cPcvLkSXn79i0FkK9xr1mzpsSJHVsaN24slSpVoiEDPgN8BvgM8BngMxCoZ4DfUrYn+Ay44zNQrVp1WbJ0qcyaNetrze9gdzfdCAgIVKtWTRYvXiwrV65U5ueffxYaMuAzwGeAzwCfAT4DfAb4DPAZsNQz4MQ27LJlS2XwoEESMWJENK9NZUwpgHgnpGmaaBqNppGBppGBppGBppGBppGBppGBppGBppGBppGBppGBpn3OwHub2iz3lhBAzAKL+QgwAQYkARIgARIgARIgARIgAUWAAojCwAMJkAAJuCoBlosESIAESIAEzEWAAoi56oO5IQESIAESIAEScBUCLAcJkICPBCiA+IiFliRAAiRAAiRAAiRAAiRAAkFBIDgEkKDIN+MkARIgARIgARIgARIgARKwIAEKIBasNGaZBPxOgD5JgARIgARIgARIwFwEKICYqz6YGxIgARIgAVchwHKQAAmQAAn4SIACiI9YaEkCJEACJEACJEACJGBVAsy3uQlQADF3/TB3JEACJEACJEACJEACJOBSBCiAuFR1ei8M70mABEiABEiABEiABEjAXAQogJirPpgbEiABVyHAcpAACZAACZAACfhIgAKIj1hoSQIkQAIkQAIkYFUCzDcJkIC5CVAAMXf9MHckQAIkQAIkQAIkQAIkYBUCfsonBRA/YaInEiABEiABEiABEiABEiABRxCgAOIIioyDBLwT4D0JkAAJkAAJkAAJkICPBCiA+IiFliRAAiRAAlYlwHyTAAmQAAmYmwAFEHPXD3NHAiRAAiRAAiRAAlYhwHySgJ8I/B8AAP//mmc5rQAAAAZJREFUAwAloJzqZd2ncgAAAABJRU5ErkJggg==>