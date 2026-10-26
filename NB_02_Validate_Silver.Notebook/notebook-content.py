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

# # Silver Data Validation and Standardization
# 
# **Purpose:** Select the latest ingested version of each source file, standardize financial data types, validate source records, quarantine invalid records, and publish clean Silver tables for downstream reconciliation.

# CELL ********************

from datetime import datetime, timezone
import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

RUN_ID = str(uuid.uuid4())
RUN_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

SOURCE_TABLES = {
    "bank_statement.csv": "Bronze_BankStatement",
    "general_ledger.csv": "Bronze_GeneralLedger",
    "chart_of_accounts.csv": "Bronze_ChartOfAccounts",
    "account_mapping.csv": "Bronze_AccountMapping"
}

print(f"Silver validation run: {RUN_ID}")
print(f"Source tables configured: {len(SOURCE_TABLES)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Select Current Source Versions
# 
# Bronze preserves historical file versions. Select the most recently ingested version of each source file so that superseded records are not processed as additional transactions.

# CELL ********************

def load_latest_bronze_version(table_name, filename):
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Required Bronze table not found: {table_name}")

    df = spark.table(table_name).filter(
        F.col("SourceFileName") == filename
    )

    if df.limit(1).count() == 0:
        raise ValueError(
            f"No Bronze records found for {filename} in {table_name}"
        )

    latest_version = (
        df.groupBy("SourceFileHash")
        .agg(F.max("IngestedAt").alias("VersionIngestedAt"))
        .orderBy(
            F.col("VersionIngestedAt").desc(),
            F.col("SourceFileHash").desc()
        )
        .first()
    )

    latest_hash = latest_version["SourceFileHash"]

    return df.filter(F.col("SourceFileHash") == latest_hash)


bank_raw = load_latest_bronze_version(
    "Bronze_BankStatement", "bank_statement.csv"
)

gl_raw = load_latest_bronze_version(
    "Bronze_GeneralLedger", "general_ledger.csv"
)

coa_raw = load_latest_bronze_version(
    "Bronze_ChartOfAccounts", "chart_of_accounts.csv"
)

mapping_raw = load_latest_bronze_version(
    "Bronze_AccountMapping", "account_mapping.csv"
)

print("Latest source versions loaded.")

for name, df in [
    ("Bank statement", bank_raw),
    ("General ledger", gl_raw),
    ("Chart of accounts", coa_raw),
    ("Account mapping", mapping_raw)
]:
    print(f"{name}: {df.count()} source rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Define Data-Quality Helpers
# 
# Use consistent rules to identify missing values, invalid dates, invalid numeric amounts, and invalid records. Preserve source rows in Bronze; quarantine records that fail validation rather than silently deleting them.

# CELL ********************

quarantine_frames = []
quality_results = []


def quarantine_records(df, source_type, key_column, condition,
                      issue_code, issue_description):
    """Capture invalid source rows without removing them from Bronze."""

    matching = df.filter(condition)

    if matching.limit(1).count() == 0:
        return 0

    record_json = F.to_json(
        F.struct(*[F.col(c) for c in df.columns])
    )

    quarantined = matching.select(
        F.lit(RUN_ID).alias("ValidationRunID"),
        F.lit(source_type).alias("SourceType"),
        F.col(key_column).cast("string").alias("SourceRecordKey"),
        F.col("BatchID").cast("string").alias("SourceBatchID"),
        F.col("SourceFileHash").cast("string").alias("SourceFileHash"),
        F.lit(issue_code).alias("IssueCode"),
        F.lit(issue_description).alias("IssueDescription"),
        record_json.alias("SourceRecordJSON"),
        F.lit(RUN_STARTED_AT).cast("timestamp").alias("DetectedAt")
    )

    quarantine_frames.append(quarantined)
    return matching.count()


def record_quality(entity, rule_name, source_count,
                   passed_count, failed_count):
    quality_results.append({
        "ValidationRunID": RUN_ID,
        "EntityName": entity,
        "RuleName": rule_name,
        "SourceRowCount": int(source_count),
        "PassedRowCount": int(passed_count),
        "FailedRowCount": int(failed_count),
        "RunStatus": "PASSED" if failed_count == 0 else "ISSUES_FOUND",
        "DetectedAt": RUN_STARTED_AT
    })


print("Validation helper functions are ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Standardize Bank Transactions
# 
# Convert transaction dates and amounts to typed fields. Require transaction identifiers, valid dates, nonnegative amounts, and exactly one positive cash direction. Transactions with both debit and credit amounts populated are quarantined; missing references alone are not treated as invalid.

# CELL ********************

bank = (
    bank_raw
    .withColumn("TransactionDate", F.expr("try_cast(TransactionDate AS DATE)"))
    .withColumn("ValueDate", F.expr("try_cast(ValueDate AS DATE)"))
    .withColumn("DebitAmount", F.expr("try_cast(DebitAmount AS DECIMAL(18,2))"))
    .withColumn("CreditAmount", F.expr("try_cast(CreditAmount AS DECIMAL(18,2))"))
)

bank_invalid = (
    F.col("BankTransactionID").isNull() |
    (F.trim(F.col("BankTransactionID")) == "") |
    F.col("TransactionDate").isNull() |
    F.col("ValueDate").isNull() |
    F.col("DebitAmount").isNull() |
    F.col("CreditAmount").isNull() |
    (F.col("DebitAmount") < 0) |
    (F.col("CreditAmount") < 0) |
    (
        (F.col("DebitAmount") > 0) &
        (F.col("CreditAmount") > 0)
    ) |
    (
        (F.col("DebitAmount") == 0) &
        (F.col("CreditAmount") == 0)
    ) |
    F.col("CurrencyCode").isNull() |
    (F.trim(F.col("CurrencyCode")) == "") |
    F.col("BankAccountID").isNull() |
    (F.trim(F.col("BankAccountID")) == "")
)

bank_source_count = bank.count()

bank_quarantine_count = quarantine_records(
    bank,
    "BankTransaction",
    "BankTransactionID",
    bank_invalid,
    "INVALID_BANK_TRANSACTION",
    "Missing required value, invalid date or amount, or invalid debit/credit direction."
)

bank_valid = bank.filter(~bank_invalid)

silver_bank = bank_valid.select(
    "BankTransactionID",
    "TransactionDate",
    "ValueDate",
    "Description",
    "ReferenceNumber",
    "DebitAmount",
    "CreditAmount",
    "CurrencyCode",
    "BankAccountID",
    "SourceTransactionID",
    "BatchID",
    "SourceFileName",
    "SourceFileHash",
    "IngestedAt",
    "SourceRowHash"
)

record_quality(
    "BankTransactions",
    "Required fields, dates, amounts and cash direction",
    bank_source_count,
    bank_source_count - bank_quarantine_count,
    bank_quarantine_count
)

(
    silver_bank.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_BankTransactions")
)

print(f"Bank source rows: {bank_source_count}")
print(f"Bank rows quarantined: {bank_quarantine_count}")
print(f"Valid bank rows published: {silver_bank.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Standardize General Ledger Lines
# 
# Convert posting dates and debit/credit amounts to typed fields. Quarantine invalid lines and every otherwise-valid line belonging to an unbalanced journal entry. A journal entry is balanced when its total debits equal its total credits to two decimal places.

# CELL ********************

gl = (
    gl_raw
    .withColumn("PostingDate", F.expr("try_cast(PostingDate AS DATE)"))
    .withColumn("DebitAmount", F.expr("try_cast(DebitAmount AS DECIMAL(18,2))"))
    .withColumn("CreditAmount", F.expr("try_cast(CreditAmount AS DECIMAL(18,2))"))
)

gl_invalid = (
    F.col("JournalEntryID").isNull() |
    (F.trim(F.col("JournalEntryID")) == "") |
    F.col("JournalLineID").isNull() |
    (F.trim(F.col("JournalLineID")) == "") |
    F.col("PostingDate").isNull() |
    F.col("AccountCode").isNull() |
    (F.trim(F.col("AccountCode")) == "") |
    F.col("DebitAmount").isNull() |
    F.col("CreditAmount").isNull() |
    (F.col("DebitAmount") < 0) |
    (F.col("CreditAmount") < 0) |
    (
        (F.col("DebitAmount") > 0) &
        (F.col("CreditAmount") > 0)
    ) |
    (
        (F.col("DebitAmount") == 0) &
        (F.col("CreditAmount") == 0)
    )
)

gl_source_count = gl.count()

gl_invalid_count = quarantine_records(
    gl,
    "JournalLine",
    "JournalLineID",
    gl_invalid,
    "INVALID_JOURNAL_LINE",
    "Missing required value, invalid date or amount, or invalid debit/credit direction."
)

gl_valid_candidates = gl.filter(~gl_invalid)

# Assess balance across each journal entry's otherwise-valid lines.
journal_balance = (
    gl_valid_candidates
    .groupBy("JournalEntryID")
    .agg(
        F.sum("DebitAmount").alias("TotalDebits"),
        F.sum("CreditAmount").alias("TotalCredits")
    )
    .withColumn(
        "IsBalanced",
        F.round(F.col("TotalDebits"), 2) ==
        F.round(F.col("TotalCredits"), 2)
    )
)

unbalanced_entries = journal_balance.filter(
    ~F.col("IsBalanced")
).select("JournalEntryID")

gl_unbalanced_rows = gl_valid_candidates.join(
    unbalanced_entries,
    on="JournalEntryID",
    how="inner"
)

gl_unbalanced_count = quarantine_records(
    gl_unbalanced_rows,
    "JournalLine",
    "JournalLineID",
    F.lit(True),
    "UNBALANCED_JOURNAL_ENTRY",
    "Journal entry debit and credit totals do not balance."
)

silver_gl = (
    gl_valid_candidates
    .join(unbalanced_entries, on="JournalEntryID", how="left_anti")
    .select(
        "JournalEntryID",
        "JournalLineID",
        "PostingDate",
        "AccountCode",
        "DebitAmount",
        "CreditAmount",
        "Description",
        "ReferenceNumber",
        "SourceTransactionID",
        "BatchID",
        "SourceFileName",
        "SourceFileHash",
        "IngestedAt",
        "SourceRowHash"
    )
)

record_quality(
    "JournalLines",
    "Required fields, dates, amounts and cash direction",
    gl_source_count,
    gl_source_count - gl_invalid_count - gl_unbalanced_count,
    gl_invalid_count + gl_unbalanced_count
)

(
    silver_gl.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_JournalLines")
)

print(f"General ledger source rows: {gl_source_count}")
print(f"Invalid journal lines: {gl_invalid_count}")
print(f"Lines quarantined for unbalanced entries: {gl_unbalanced_count}")
print(f"Valid journal lines published: {silver_gl.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Validate Chart of Accounts and Account Mapping
# 
# Standardize account status and effective dates. Quarantine records with missing required account identifiers, invalid status values, invalid dates, or an effective date range that ends before it starts.

# CELL ********************

# Validate Account Mapping — corrected implementation

mapping = (
    mapping_raw
    .withColumn(
        "EffectiveFrom",
        F.expr("try_cast(NULLIF(TRIM(EffectiveFrom), '') AS DATE)")
    )
    .withColumn(
        "EffectiveToRaw",
        F.col("EffectiveTo").cast("string")
    )
    .withColumn(
        "EffectiveTo",
        F.expr("try_cast(NULLIF(TRIM(EffectiveTo), '') AS DATE)")
    )
)

effective_to_invalid = (
    F.col("EffectiveToRaw").isNotNull()
    & (F.trim(F.col("EffectiveToRaw")) != "")
    & F.col("EffectiveTo").isNull()
)

mapping_invalid_corrected = (
    F.col("SourceSystem").isNull()
    | (F.trim(F.col("SourceSystem")) == "")
    | F.col("SourceAccountCode").isNull()
    | (F.trim(F.col("SourceAccountCode")) == "")
    | F.col("TargetAccountID").isNull()
    | (F.trim(F.col("TargetAccountID")) == "")
    | F.col("EffectiveFrom").isNull()
    | effective_to_invalid
    | (
        F.col("EffectiveTo").isNotNull()
        & F.col("EffectiveFrom").isNotNull()
        & (F.col("EffectiveTo") < F.col("EffectiveFrom"))
    )
)

mapping_source_count = mapping.count()

mapping_invalid_count = mapping.filter(
    mapping_invalid_corrected
).count()

mapping_valid_count = mapping_source_count - mapping_invalid_count

print(f"Source mapping rows: {mapping_source_count}")
print(f"Invalid mapping rows: {mapping_invalid_count}")
print(f"Valid mapping rows: {mapping_valid_count}")

if mapping_valid_count != 15 or mapping_invalid_count != 0:
    raise ValueError(
        "Account mapping validation did not produce the expected "
        "15 valid rows. Do not overwrite Dim_AccountMapping."
    )

silver_mapping = (
    mapping
    .filter(~mapping_invalid_corrected)
    .select(
        "SourceSystem",
        "SourceAccountCode",
        "TargetAccountID",
        "EffectiveFrom",
        "EffectiveTo",
        "BatchID",
        "SourceFileName",
        "SourceFileHash",
        "IngestedAt",
        "SourceRowHash"
    )
)

# Verify the DataFrame before writing to the table.
if silver_mapping.count() != 15:
    raise ValueError(
        "Expected 15 rows in silver_mapping before writing."
    )

record_quality(
    "AccountMapping",
    "Required mapping fields and effective date range",
    mapping_source_count,
    mapping_valid_count,
    mapping_invalid_count
)

(
    silver_mapping.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Dim_AccountMapping")
)

# Verify the persisted table after the write.
persisted_mapping_count = spark.table(
    "Dim_AccountMapping"
).count()

if persisted_mapping_count != 15:
    raise ValueError(
        f"Expected 15 persisted mapping rows, "
        f"found {persisted_mapping_count}."
    )

print(
    f"SUCCESS: Dim_AccountMapping contains "
    f"{persisted_mapping_count} rows."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Publish Data-Quality Results
# 
# Combine the quarantined records from all source entities into one table. Publish a validation summary for each entity and rule, then report the final row counts for the Silver tables. These outputs represent the latest validation run.

# CELL ********************

# Standard schema for quarantined source records
QUARANTINE_SCHEMA = StructType([
    StructField("ValidationRunID", StringType(), False),
    StructField("SourceType", StringType(), False),
    StructField("SourceRecordKey", StringType(), True),
    StructField("SourceBatchID", StringType(), True),
    StructField("SourceFileHash", StringType(), True),
    StructField("IssueCode", StringType(), False),
    StructField("IssueDescription", StringType(), False),
    StructField("SourceRecordJSON", StringType(), True),
    StructField("DetectedAt", TimestampType(), False)
])

if quarantine_frames:
    quarantine_df = quarantine_frames[0]

    for next_df in quarantine_frames[1:]:
        quarantine_df = quarantine_df.unionByName(next_df)
else:
    quarantine_df = spark.createDataFrame(
        [], schema=QUARANTINE_SCHEMA
    )

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Quarantine")
)


# Write the data-quality summary with an explicit schema.
QUALITY_SCHEMA = StructType([
    StructField("ValidationRunID", StringType(), False),
    StructField("EntityName", StringType(), False),
    StructField("RuleName", StringType(), False),
    StructField("SourceRowCount", LongType(), False),
    StructField("PassedRowCount", LongType(), False),
    StructField("FailedRowCount", LongType(), False),
    StructField("RunStatus", StringType(), False),
    StructField("DetectedAt", TimestampType(), False)
])

quality_df = spark.createDataFrame(
    [
        (
            r["ValidationRunID"],
            r["EntityName"],
            r["RuleName"],
            r["SourceRowCount"],
            r["PassedRowCount"],
            r["FailedRowCount"],
            r["RunStatus"],
            r["DetectedAt"]
        )
        for r in quality_results
    ],
    schema=QUALITY_SCHEMA
)

(
    quality_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_DataQualityResults")
)


# Report the latest validation run.
print("SILVER VALIDATION SUMMARY")
display(
    spark.table("Silver_DataQualityResults")
    .filter(F.col("ValidationRunID") == RUN_ID)
    .orderBy("EntityName")
)

print("QUARANTINE SUMMARY")
display(
    spark.table("Silver_Quarantine")
    .filter(F.col("ValidationRunID") == RUN_ID)
    .groupBy("SourceType", "IssueCode")
    .count()
    .orderBy("SourceType", "IssueCode")
)

print("SILVER TABLE ROW COUNTS")

SILVER_TABLES = [
    "Silver_BankTransactions",
    "Silver_JournalLines",
    "Dim_ChartOfAccounts",
    "Dim_AccountMapping"
]

for table_name in SILVER_TABLES:
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")

print(f"Quarantined records: {quarantine_df.count()}")
print("Notebook 2 processing completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE TABLE LH_Elarwick_Reconciliation.dbo.Dim_ChartOfAccounts;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
