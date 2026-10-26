# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1836fa4b-2f43-4a48-b8d6-de7bd7d9e935",
# META       "default_lakehouse_name": "LH_Elarwick_Reconciliation",
# META       "default_lakehouse_workspace_id": "98a61ed5-bd83-40ce-98f3-e4ea0cdc8e2c",
# META       "known_lakehouses": [
# META         {
# META           "id": "1836fa4b-2f43-4a48-b8d6-de7bd7d9e935"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze Source Ingestion
# 
# **Purpose:** Load the financial source files into Bronze Delta tables while preserving source values, tracking file versions, preventing duplicate file ingestion, and recording processing outcomes.

# CELL ********************

from datetime import datetime, timezone
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

SOURCE_PATH = "Files"

SOURCE_FILES = {
    "bank_statement.csv": "Bronze_BankStatement",
    "general_ledger.csv": "Bronze_GeneralLedger",
    "chart_of_accounts.csv": "Bronze_ChartOfAccounts",
    "account_mapping.csv": "Bronze_AccountMapping"
}

BATCH_ID = str(uuid.uuid4())
BATCH_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"Batch ID: {BATCH_ID}")
print(f"Source path: {SOURCE_PATH}")
print(f"Files expected: {len(SOURCE_FILES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Validate Source Files
# 
# Confirm that every required CSV file exists in the Lakehouse source folder before ingestion begins.

# CELL ********************

source_listing = notebookutils.fs.ls(SOURCE_PATH)

available_files = {
    item.name for item in source_listing
    if not item.isDir
}

missing_files = [
    filename for filename in SOURCE_FILES
    if filename not in available_files
]

if missing_files:
    raise FileNotFoundError(
        f"Required source files are missing: {missing_files}"
    )

print("Source-file validation passed.")

for filename in SOURCE_FILES:
    print(f"Found: {filename}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Define Processing Controls
# 
# Create consistent control-table schemas for recording ingestion batches and individual file outcomes. These tables provide an audit trail for successful, skipped, and failed processing attempts.

# CELL ********************

# Control-table schemas

BATCH_SCHEMA = StructType([
    StructField("BatchID", StringType(), False),
    StructField("StartedAt", TimestampType(), False),
    StructField("CompletedAt", TimestampType(), True),
    StructField("Status", StringType(), False),
    StructField("FilesExpected", LongType(), False),
    StructField("FilesSucceeded", LongType(), False),
    StructField("FilesSkipped", LongType(), False),
    StructField("FilesFailed", LongType(), False),
    StructField("RowsIngested", LongType(), False),
    StructField("ErrorMessage", StringType(), True)
])

FILE_SCHEMA = StructType([
    StructField("FileName", StringType(), False),
    StructField("SourceFileHash", StringType(), False),
    StructField("BatchID", StringType(), False),
    StructField("TargetTable", StringType(), False),
    StructField("Status", StringType(), False),
    StructField("SourceRowCount", LongType(), False),
    StructField("StartedAt", TimestampType(), False),
    StructField("CompletedAt", TimestampType(), True),
    StructField("ErrorMessage", StringType(), True)
])


# Append one record to a Delta control table.
def append_control_record(table_name, schema, record):
    control_df = spark.createDataFrame([record], schema=schema)
    control_df.write.format("delta").mode("append").saveAsTable(
        table_name
    )


# Calculate SHA-256 from the exact source file content.
def get_source_file_hash(file_path):
    binary_df = (
        spark.read
        .format("binaryFile")
        .load(file_path)
        .select(
            F.sha2(F.col("content"), 256).alias("FileHash")
        )
    )

    result = binary_df.first()

    if result is None:
        raise FileNotFoundError(
            f"Could not read file content: {file_path}"
        )

    return result["FileHash"]


# Check whether this exact file version has already been loaded.
def file_version_already_loaded(
    file_hash, filename, target_table
):
    # First, check the successful file-ingestion log.
    if spark.catalog.tableExists("Control_SourceFiles"):
        logged = (
            spark.table("Control_SourceFiles")
            .filter(
                (F.col("FileName") == filename) &
                (F.col("SourceFileHash") == file_hash) &
                (F.col("Status") == "SUCCEEDED")
            )
            .limit(1)
            .count()
        )

        if logged > 0:
            return True

    # Recover if Bronze was written but its success log was not.
    if spark.catalog.tableExists(target_table):
        existing_rows = (
            spark.table(target_table)
            .filter(
                (F.col("SourceFileName") == filename) &
                (F.col("SourceFileHash") == file_hash)
            )
            .limit(1)
            .count()
        )

        if existing_rows > 0:
            return True

    return False


print("Control schemas and helper functions are ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Ingest Source Files
# 
# Calculate a content hash for each source file. Skip file versions already ingested successfully. For new or changed files, preserve the original values, add ingestion metadata, and append the records to the corresponding Bronze table.

# CELL ********************

# Cell 8 — Run Controlled Ingestion

file_results = []
total_rows_ingested = 0

for filename, target_table in SOURCE_FILES.items():

    file_started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    file_completed_at = None
    source_file_hash = None
    source_row_count = 0
    file_status = "FAILED"
    error_message = None

    file_path = f"{SOURCE_PATH}/{filename}"

    try:
        # Identify the exact version of the source file.
        source_file_hash = get_source_file_hash(file_path)

        # Read the CSV and preserve source values as strings.
        source_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("mode", "FAILFAST")
            .csv(file_path)
        )

        source_columns = source_df.columns

        if not source_columns:
            raise ValueError(f"No columns found in {filename}.")

        if len(source_columns) != len(set(source_columns)):
            raise ValueError(
                f"Duplicate column names detected in {filename}."
            )

        # Count rows even when the file version was loaded previously.
        source_row_count = source_df.count()

        # Skip previously ingested file versions.
        if file_version_already_loaded(
            source_file_hash, filename, target_table
        ):
            file_status = "SKIPPED"

            print(
                f"SKIPPED: {filename} — "
                f"already loaded; source contains "
                f"{source_row_count} rows."
            )

        else:
            # Calculate a hash for each row's source values.
            row_json = F.to_json(
                F.struct(*[F.col(c) for c in source_columns])
            )

            bronze_df = (
                source_df
                .withColumn("BatchID", F.lit(BATCH_ID))
                .withColumn(
                    "SourceFileName", F.lit(filename)
                )
                .withColumn(
                    "SourceFileHash", F.lit(source_file_hash)
                )
                .withColumn(
                    "IngestedAt",
                    F.lit(BATCH_STARTED_AT).cast("timestamp")
                )
                .withColumn(
                    "SourceRowHash", F.sha2(row_json, 256)
                )
            )

            # Append new versions; never overwrite Bronze history.
            (
                bronze_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )

            # Verify rows written for this batch and file version.
            written_rows = (
                spark.table(target_table)
                .filter(
                    (F.col("BatchID") == BATCH_ID) &
                    (F.col("SourceFileHash") == source_file_hash) &
                    (F.col("SourceFileName") == filename)
                )
                .count()
            )

            if written_rows != source_row_count:
                raise ValueError(
                    f"Row-count mismatch for {filename}: "
                    f"source={source_row_count}, "
                    f"written={written_rows}"
                )

            file_status = "SUCCEEDED"
            total_rows_ingested += source_row_count

            print(
                f"SUCCEEDED: {filename} -> {target_table} "
                f"({source_row_count} rows)"
            )

    except Exception as exc:
        error_message = str(exc)[:4000]
        file_status = "FAILED"

        print(f"FAILED: {filename} — {error_message}")

    finally:
        file_completed_at = (
            datetime.now(timezone.utc).replace(tzinfo=None)
        )

        # Save the file outcome and source row count.
        file_record = (
            filename,
            source_file_hash or "UNAVAILABLE",
            BATCH_ID,
            target_table,
            file_status,
            source_row_count,
            file_started_at,
            file_completed_at,
            error_message
        )

        append_control_record(
            "Control_SourceFiles",
            FILE_SCHEMA,
            file_record
        )

        file_results.append({
            "FileName": filename,
            "TargetTable": target_table,
            "Status": file_status,
            "SourceRowCount": source_row_count,
            "ErrorMessage": error_message
        })

print("File processing loop finished.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Record the Ingestion Batch
# 
# Record the batch outcome, including successful files, skipped files, failures, and the number of newly ingested rows.

# CELL ********************

success_count = sum(
    1 for result in file_results
    if result["Status"] == "SUCCEEDED"
)

skipped_count = sum(
    1 for result in file_results
    if result["Status"] == "SKIPPED"
)

failed_results = [
    result for result in file_results
    if result["Status"] == "FAILED"
]

failed_count = len(failed_results)

if failed_count == 0:
    batch_status = "COMPLETED"
elif success_count > 0 or skipped_count > 0:
    batch_status = "COMPLETED_WITH_ERRORS"
else:
    batch_status = "FAILED"

batch_errors = "; ".join(
    f'{result["FileName"]}: {result["ErrorMessage"]}'
    for result in failed_results
) or None

batch_record = (
    BATCH_ID,
    BATCH_STARTED_AT,
    datetime.now(timezone.utc).replace(tzinfo=None),
    batch_status,
    len(SOURCE_FILES),
    success_count,
    skipped_count,
    failed_count,
    total_rows_ingested,
    batch_errors
)

append_control_record(
    "Control_Batches",
    BATCH_SCHEMA,
    batch_record
)

print(f"Batch status: {batch_status}")
print(f"Files succeeded: {success_count}")
print(f"Files skipped: {skipped_count}")
print(f"Files failed: {failed_count}")
print(f"Rows newly ingested: {total_rows_ingested}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Review Ingestion Results
# 
# Review the file-level audit log and the rows written by this batch. Confirm that successful file loads have the expected row counts and that any failed files have a recorded error.

# CELL ********************

# Cell 12 — Final Validation and Summary

print("FILE PROCESSING RESULTS")

RESULT_SCHEMA = StructType([
    StructField("FileName", StringType(), False),
    StructField("TargetTable", StringType(), False),
    StructField("Status", StringType(), False),
    StructField("SourceRowCount", LongType(), False),
    StructField("ErrorMessage", StringType(), True)
])

results_df = spark.createDataFrame(
    [
        (
            r["FileName"],
            r["TargetTable"],
            r["Status"],
            int(r["SourceRowCount"]),
            r["ErrorMessage"]
        )
        for r in file_results
    ],
    schema=RESULT_SCHEMA
)

display(results_df.orderBy("FileName"))

print("LATEST BATCH AUDIT RECORD")

display(
    spark.table("Control_Batches")
    .filter(F.col("BatchID") == BATCH_ID)
)

print("BRONZE TABLE SUMMARY")

for filename, target_table in SOURCE_FILES.items():

    if not spark.catalog.tableExists(target_table):
        print(f"{target_table}: TABLE NOT FOUND")
        continue

    bronze_table = spark.table(target_table)

    current_batch_rows = (
        bronze_table
        .filter(F.col("BatchID") == BATCH_ID)
        .count()
    )

    historical_rows = bronze_table.count()

    print(
        f"{target_table}: "
        f"rows in current batch={current_batch_rows}, "
        f"total historical rows={historical_rows}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Validate Bronze Tables
# 
# Verify the historical row counts and required ingestion metadata in each Bronze table. Stop processing if any table fails validation.

# CELL ********************

# Cell 14 — Bronze Acceptance Validation

EXPECTED_ROWS = {
    "Bronze_BankStatement": 20,
    "Bronze_GeneralLedger": 40,
    "Bronze_ChartOfAccounts": 10,
    "Bronze_AccountMapping": 15
}

REQUIRED_METADATA = [
    "BatchID",
    "SourceFileName",
    "SourceFileHash",
    "IngestedAt",
    "SourceRowHash"
]

validation_results = []

for table_name, expected_rows in EXPECTED_ROWS.items():

    if not spark.catalog.tableExists(table_name):
        validation_results.append({
            "TableName": table_name,
            "ExpectedRows": expected_rows,
            "ActualRows": 0,
            "MissingMetadata": ", ".join(REQUIRED_METADATA),
            "Status": "FAILED"
        })
        continue

    df = spark.table(table_name)
    actual_rows = df.count()

    missing_metadata = [
        column for column in REQUIRED_METADATA
        if column not in df.columns
    ]

    passed = (
        actual_rows == expected_rows
        and len(missing_metadata) == 0
    )

    validation_results.append({
        "TableName": table_name,
        "ExpectedRows": expected_rows,
        "ActualRows": actual_rows,
        "MissingMetadata": ", ".join(missing_metadata) or "None",
        "Status": "PASSED" if passed else "FAILED"
    })

VALIDATION_SCHEMA = StructType([
    StructField("TableName", StringType(), False),
    StructField("ExpectedRows", LongType(), False),
    StructField("ActualRows", LongType(), False),
    StructField("MissingMetadata", StringType(), False),
    StructField("Status", StringType(), False)
])

validation_df = spark.createDataFrame(
    validation_results,
    schema=VALIDATION_SCHEMA
)

display(validation_df)

failed_count = sum(
    1 for result in validation_results
    if result["Status"] == "FAILED"
)

if failed_count:
    raise ValueError(
        f"Bronze validation failed for {failed_count} table(s)."
    )

print("BRONZE VALIDATION PASSED: all four tables meet acceptance checks.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_Elarwick_Reconciliation.dbo.dim_chartofaccounts LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
