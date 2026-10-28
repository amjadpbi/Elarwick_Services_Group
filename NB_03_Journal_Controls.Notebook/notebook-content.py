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

# # Notebook 3: Reconciliation Engine
# 
# ## Purpose
# Reconcile validated bank transactions against balanced general-ledger journal entries using deterministic matching rules.
# 
# ## Inputs
# - Silver_BankTransactions
# - Silver_JournalLines
# - Dim_ChartOfAccounts
# - Dim_AccountMapping
# 
# ## Outputs
# - Reconciliation results
# - Unmatched bank transactions
# - Unmatched GL journal entries
# - Reconciliation exceptions
# - Run-level reconciliation summary
# 
# ## Principles
# - Preserve source records; never delete suspected duplicates.
# - Use reference evidence and amount/date checks to classify matches.
# - Distinguish confirmed matches from potential matches.
# - Keep amount discrepancies and date mismatches visible.
# - Validate results against known test scenarios before acceptance.

# CELL ********************

from pyspark.sql import functions as F

required_tables = [
    "Silver_BankTransactions",
    "Silver_JournalLines",
    "Dim_ChartOfAccounts",
    "Dim_AccountMapping"
]

for table_name in required_tables:
    print(f"\n{'=' * 70}")
    print(f"TABLE: {table_name}")
    print(f"{'=' * 70}")

    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Required table not found: {table_name}. "
            "Confirm the Lakehouse is attached and Notebook 2 completed."
        )

    df = spark.table(table_name)

    print("Row count:", df.count())
    print("Columns and data types:")
    df.printSchema()

    print("Sample records:")
    display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reconciliation Methodology
# 
# 1. Aggregate balanced GL journal lines into journal-entry-level records.
# 2. Match bank transactions to GL entries using exact reference, amount, direction, and date evidence.
# 3. Support documented one-to-many and many-to-one matches using explicit reference evidence.
# 4. Classify amount differences, date mismatches, missing references, and duplicate-like transactions as exceptions.
# 5. Never use amount and date alone as sufficient evidence for a confirmed match.
# 6. Preserve all source records and report unmatched items separately.
# 
# DebitAmount represents a bank inflow. CreditAmount represents a bank outflow.
# For GL entries, bank-facing direction must be determined from the posting to the mapped bank account, not from the journal entry's total debits or credits.

# CELL ********************

from pyspark.sql import functions as F

bank = spark.table("Silver_BankTransactions")
gl = spark.table("Silver_JournalLines")
coa = spark.table("Dim_ChartOfAccounts")
mapping = spark.table("Dim_AccountMapping")

print("BANK TRANSACTION DIRECTIONS")
display(
    bank.select(
        "BankTransactionID",
        "TransactionDate",
        "Description",
        "ReferenceNumber",
        "DebitAmount",
        "CreditAmount",
        "BankAccountID"
    ).orderBy("BankTransactionID")
)

print("GL JOURNAL-ENTRY SUMMARY")
display(
    gl.groupBy("JournalEntryID")
      .agg(
          F.min("PostingDate").alias("PostingDate"),
          F.sum("DebitAmount").alias("TotalDebit"),
          F.sum("CreditAmount").alias("TotalCredit"),
          F.count("*").alias("LineCount"),
          F.concat_ws(", ", F.collect_set("ReferenceNumber")).alias("References"),
          F.concat_ws(" | ", F.collect_set("Description")).alias("Descriptions")
      )
      .orderBy("JournalEntryID")
)

print("CHART OF ACCOUNTS")
display(
    coa.select("AccountID", "AccountCode", "AccountName", "AccountType", "IsActive")
       .orderBy("AccountCode")
)

print("ACCOUNT MAPPING")
display(
    mapping.select(
        "SourceSystem",
        "SourceAccountCode",
        "TargetAccountID",
        "EffectiveFrom",
        "EffectiveTo"
    ).orderBy("SourceSystem", "SourceAccountCode")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deterministic Reconciliation Rules
# 
# - Identify bank-account GL postings using the mapped bank account.
# - Convert bank and GL postings to signed amounts: debit minus credit.
# - Flag duplicate-like bank records before matching.
# - Match equal reference groups when signed amounts and dates agree.
# - Record amount discrepancies and date mismatches as exceptions.
# - Keep missing-reference candidates as potential matches only.
# - Preserve unmatched bank transactions and unmatched GL entries.
# - Do not modify or delete the Silver source tables.

# CELL ********************

from pyspark.sql import functions as F

gl = spark.table("Silver_JournalLines")
coa = spark.table("Dim_ChartOfAccounts")
mapping = spark.table("Dim_AccountMapping")

# Step 1: Resolve the ERP bank-account mapping.
bank_account_ids = (
    mapping
    .filter(
        (F.col("SourceSystem") == "ERP") &
        (F.col("SourceAccountCode") == "1000")
    )
    .select(
        F.col("TargetAccountID").alias("AccountID")
    )
    .distinct()
)

# Step 2: Join using the matching AccountID column.
bank_account_codes = (
    coa.join(bank_account_ids, on="AccountID", how="inner")
       .filter(F.col("IsActive") == True)
       .select("AccountCode", "AccountID", "AccountName")
       .distinct()
)

if bank_account_codes.count() == 0:
    raise ValueError(
        "No active GL bank account was found for ERP source account 1000."
    )

print("Mapped bank account:")
display(bank_account_codes)

# Step 3: Inspect the GL postings to that account.
bank_gl_lines = (
    gl.join(
        bank_account_codes.select("AccountCode"),
        on="AccountCode",
        how="inner"
    )
    .select(
        "JournalEntryID",
        "JournalLineID",
        "PostingDate",
        "AccountCode",
        "DebitAmount",
        "CreditAmount",
        "ReferenceNumber",
        "Description"
    )
    .orderBy("JournalEntryID", "JournalLineID")
)

print("GL lines posted to the mapped bank account:")
display(bank_gl_lines)

print("Bank-account GL posting count:", bank_gl_lines.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Signed Amount Convention
# 
# For bank statement transactions:
# - DebitAmount is a cash inflow (positive).
# - CreditAmount is a cash outflow (negative).
# 
# For GL postings to the Operating Bank account:
# - DebitAmount increases the bank balance (positive).
# - CreditAmount decreases the bank balance (negative).
# 
# SignedAmount = DebitAmount - CreditAmount
# 
# Only GL lines posted to the mapped Operating Bank account are eligible for bank transaction matching.

# CELL ********************

from pyspark.sql import functions as F

gl = spark.table("Silver_JournalLines")
coa = spark.table("Dim_ChartOfAccounts")
mapping = spark.table("Dim_AccountMapping")

# Resolve the active ERP mapping for the Operating Bank account.
bank_account_ids = (
    mapping
    .filter(
        (F.col("SourceSystem") == "ERP") &
        (F.col("SourceAccountCode") == "1000") &
        (F.col("EffectiveFrom") <= F.current_date()) &
        (
            F.col("EffectiveTo").isNull() |
            (F.col("EffectiveTo") >= F.current_date())
        )
    )
    .select(F.col("TargetAccountID").alias("AccountID"))
    .distinct()
)

bank_account_codes = (
    coa.join(bank_account_ids, on="AccountID", how="inner")
       .filter(F.col("IsActive") == True)
       .select("AccountCode")
       .distinct()
)

if bank_account_codes.count() != 1:
    raise ValueError(
        f"Expected exactly one active mapped bank account; "
        f"found {bank_account_codes.count()}."
    )

bank_gl_lines = (
    gl.join(bank_account_codes, on="AccountCode", how="inner")
      .withColumn(
          "GLSignedAmount",
          F.coalesce(F.col("DebitAmount"), F.lit(0).cast("decimal(18,2)")) -
          F.coalesce(F.col("CreditAmount"), F.lit(0).cast("decimal(18,2)"))
      )
      .select(
          "JournalEntryID",
          "JournalLineID",
          "PostingDate",
          "ReferenceNumber",
          "Description",
          "GLSignedAmount"
      )
)

print("Bank-side GL lines:", bank_gl_lines.count())
display(bank_gl_lines.orderBy("PostingDate", "JournalEntryID"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 8 — Build Reconciliation Results, Exceptions, and Summary

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DecimalType, LongType
)

# ============================================================
# 1. LOAD AND VALIDATE INPUTS
# ============================================================

bank = spark.table("Silver_BankTransactions")
gl = spark.table("Silver_JournalLines")
coa = spark.table("Dim_ChartOfAccounts")
mapping = spark.table("Dim_AccountMapping")

required_bank_columns = {
    "BankTransactionID", "TransactionDate", "ReferenceNumber",
    "DebitAmount", "CreditAmount", "BankAccountID"
}
required_gl_columns = {
    "JournalEntryID", "JournalLineID", "PostingDate",
    "AccountCode", "DebitAmount", "CreditAmount", "ReferenceNumber"
}

if not required_bank_columns.issubset(set(bank.columns)):
    raise ValueError("Silver_BankTransactions is missing required columns.")

if not required_gl_columns.issubset(set(gl.columns)):
    raise ValueError("Silver_JournalLines is missing required columns.")

# Keep the reconciliation date logic tied to transaction dates.
# The mapping must be effective on the date of each GL posting.

# ============================================================
# 2. PREPARE BANK TRANSACTIONS
# ============================================================

bank_prepared = (
    bank
    .withColumn(
        "SignedAmount",
        F.coalesce(F.col("DebitAmount"), F.lit(0).cast("decimal(18,2)")) -
        F.coalesce(F.col("CreditAmount"), F.lit(0).cast("decimal(18,2)"))
    )
    .withColumn(
        "NormalizedReference",
        F.when(
            F.col("ReferenceNumber").isNull() |
            (F.trim(F.col("ReferenceNumber")) == ""),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("ReferenceNumber")))
    )
)

# Flag all members of a duplicate-like group. Do not delete them.
bank_duplicate_groups = (
    bank_prepared
    .filter(F.col("NormalizedReference").isNotNull())
    .groupBy("NormalizedReference", "TransactionDate", "SignedAmount")
    .agg(
        F.count("*").alias("DuplicateGroupCount"),
        F.collect_set("BankTransactionID").alias("DuplicateTransactionIDs")
    )
    .filter(F.col("DuplicateGroupCount") > 1)
)

bank_prepared = (
    bank_prepared
    .join(
        bank_duplicate_groups.select(
            "NormalizedReference", "TransactionDate", "SignedAmount",
            "DuplicateGroupCount"
        ),
        on=["NormalizedReference", "TransactionDate", "SignedAmount"],
        how="left"
    )
    .withColumn(
        "IsDuplicateLike",
        F.coalesce(F.col("DuplicateGroupCount") > 1, F.lit(False))
    )
)

# ============================================================
# 3. RESOLVE BANK ACCOUNT MAPPING FOR EACH GL POSTING DATE
# ============================================================

# Source account 1000 maps through ERP to the Operating Bank account.
# Resolve mappings as-of the GL posting date, not today's date.

gl_bank_mapping = (
    gl.alias("g")
    .join(
        mapping.filter(
            (F.col("SourceSystem") == "ERP") &
            (F.col("SourceAccountCode") == "1000")
        ).alias("m"),
        (
            (F.col("g.PostingDate") >= F.col("m.EffectiveFrom")) &
            (
                F.col("m.EffectiveTo").isNull() |
                (F.col("g.PostingDate") <= F.col("m.EffectiveTo"))
            )
        ),
        "inner"
    )
    .join(
        coa.filter(F.col("IsActive") == True).alias("c"),
        F.col("m.TargetAccountID") == F.col("c.AccountID"),
        "inner"
    )
    .filter(F.col("g.AccountCode") == F.col("c.AccountCode"))
    .select(
        F.col("g.JournalEntryID").alias("JournalEntryID"),
        F.col("g.JournalLineID").alias("JournalLineID"),
        F.col("g.PostingDate").alias("PostingDate"),
        F.col("g.ReferenceNumber").alias("ReferenceNumber"),
        F.col("g.DebitAmount").alias("DebitAmount"),
        F.col("g.CreditAmount").alias("CreditAmount"),
        F.col("g.AccountCode").alias("AccountCode")
    )
    .withColumn(
        "GLSignedAmount",
        F.coalesce(F.col("DebitAmount"), F.lit(0).cast("decimal(18,2)")) -
        F.coalesce(F.col("CreditAmount"), F.lit(0).cast("decimal(18,2)"))
    )
    .withColumn(
        "NormalizedReference",
        F.when(
            F.col("ReferenceNumber").isNull() |
            (F.trim(F.col("ReferenceNumber")) == ""),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("ReferenceNumber")))
    )
)

# A single journal entry should have exactly one bank-side posting
# for this dataset. Fail rather than silently double-counting entries.
gl_bank_entry_counts = (
    gl_bank_mapping
    .groupBy("JournalEntryID")
    .agg(F.count("*").alias("BankPostingLineCount"))
)

invalid_bank_entry_counts = gl_bank_entry_counts.filter(
    F.col("BankPostingLineCount") != 1
)

if invalid_bank_entry_counts.limit(1).count() > 0:
    display(invalid_bank_entry_counts)
    raise ValueError(
        "At least one journal entry has zero or multiple mapped bank-side "
        "lines. Review account mapping before reconciling."
    )

gl_bank_entries = (
    gl_bank_mapping
    .groupBy("JournalEntryID")
    .agg(
        F.min("PostingDate").alias("PostingDate"),
        F.first("NormalizedReference", ignorenulls=True).alias("NormalizedReference"),
        F.sum("GLSignedAmount").alias("GLSignedAmount"),
        F.count("*").alias("BankPostingLineCount")
    )
)

# ============================================================
# 4. AGGREGATE REFERENCE GROUPS
# ============================================================

bank_reference_groups = (
    bank_prepared
    .filter(F.col("NormalizedReference").isNotNull())
    .groupBy("NormalizedReference")
    .agg(
        F.sum("SignedAmount").alias("BankSignedAmount"),
        F.count("*").alias("BankTransactionCount"),
        F.min("TransactionDate").alias("BankFirstDate"),
        F.max("TransactionDate").alias("BankLastDate"),
        F.sort_array(F.collect_set("BankTransactionID")).alias("BankTransactionIDs"),
        F.max(F.col("IsDuplicateLike").cast("int")).alias("HasDuplicateLike")
    )
)

gl_reference_groups = (
    gl_bank_entries
    .filter(F.col("NormalizedReference").isNotNull())
    .groupBy("NormalizedReference")
    .agg(
        F.sum("GLSignedAmount").alias("GLSignedAmount"),
        F.count("*").alias("GLEntryCount"),
        F.min("PostingDate").alias("GLFirstDate"),
        F.max("PostingDate").alias("GLLastDate"),
        F.sort_array(F.collect_set("JournalEntryID")).alias("JournalEntryIDs")
    )
)

# ============================================================
# 5. MATCH REFERENCE GROUPS AND CLASSIFY EXCEPTIONS
# ============================================================

reference_results = (
    bank_reference_groups
    .join(gl_reference_groups, on="NormalizedReference", how="full_outer")
    .withColumn(
        "AmountDifference",
        F.when(
            F.col("BankSignedAmount").isNotNull() &
            F.col("GLSignedAmount").isNotNull(),
            F.col("BankSignedAmount") - F.col("GLSignedAmount")
        ).otherwise(F.lit(None).cast("decimal(18,2)"))
    )
    .withColumn(
        "DateDifferenceDays",
        F.when(
            F.col("BankFirstDate").isNotNull() &
            F.col("GLFirstDate").isNotNull(),
            F.datediff(F.col("BankFirstDate"), F.col("GLFirstDate"))
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "ReconciliationStatus",
        F.when(
            F.col("BankSignedAmount").isNull(),
            F.lit("LEDGER_ONLY")
        ).when(
            F.col("GLSignedAmount").isNull(),
            F.lit("BANK_ONLY")
        ).when(
            (F.coalesce(F.col("HasDuplicateLike"), F.lit(0)) == 1) &
            (F.abs(F.col("AmountDifference")) > F.lit(0.01)),
            F.lit("POSSIBLE_DUPLICATE")
        ).when(
            F.abs(F.col("AmountDifference")) > F.lit(0.01),
            F.lit("AMOUNT_DISCREPANCY")
        ).when(
            (F.col("BankFirstDate") != F.col("GLFirstDate")) |
            (F.col("BankLastDate") != F.col("GLLastDate")),
            F.lit("DATE_MISMATCH")
        ).when(
            (F.col("BankTransactionCount") == 1) &
            (F.col("GLEntryCount") == 1),
            F.lit("CONFIRMED_MATCH")
        ).otherwise(
            F.lit("CONFIRMED_GROUP_MATCH")
        )
    )
    .withColumn(
        "MatchMethod",
        F.when(
            F.col("ReconciliationStatus").isin(
                "CONFIRMED_MATCH", "CONFIRMED_GROUP_MATCH"
            ),
            F.lit("REFERENCE_AMOUNT_DATE")
        ).otherwise(F.lit("REFERENCE_REVIEW"))
    )
    .withColumn(
        "ExceptionReason",
        F.when(
            F.col("ReconciliationStatus") == "POSSIBLE_DUPLICATE",
            F.lit("Multiple bank transactions share the same reference, date and amount.")
        ).when(
            F.col("ReconciliationStatus") == "AMOUNT_DISCREPANCY",
            F.lit("Reference exists on both sides but signed amounts differ.")
        ).when(
            F.col("ReconciliationStatus") == "DATE_MISMATCH",
            F.lit("Reference and amount align, but posting dates differ.")
        ).when(
            F.col("ReconciliationStatus") == "BANK_ONLY",
            F.lit("No GL bank-side posting has this reference.")
        ).when(
            F.col("ReconciliationStatus") == "LEDGER_ONLY",
            F.lit("No bank transaction has this reference.")
        ).otherwise(F.lit(None).cast("string"))
    )
    .select(
        "NormalizedReference",
        "BankTransactionIDs",
        "JournalEntryIDs",
        "BankSignedAmount",
        "GLSignedAmount",
        "AmountDifference",
        "BankFirstDate",
        "BankLastDate",
        "GLFirstDate",
        "GLLastDate",
        "DateDifferenceDays",
        "BankTransactionCount",
        "GLEntryCount",
        "ReconciliationStatus",
        "MatchMethod",
        "ExceptionReason"
    )
)

# ============================================================
# 6. HANDLE MISSING-REFERENCE POTENTIAL MATCHES
# ============================================================

bank_no_reference = bank_prepared.filter(
    F.col("NormalizedReference").isNull()
)

gl_no_reference = gl_bank_entries.filter(
    F.col("NormalizedReference").isNull()
)

potential_matches = (
    bank_no_reference.alias("b")
    .join(
        gl_no_reference.alias("g"),
        (
            (F.col("b.TransactionDate") == F.col("g.PostingDate")) &
            (F.abs(F.col("b.SignedAmount") - F.col("g.GLSignedAmount")) <= F.lit(0.01))
        ),
        "inner"
    )
    .select(
        F.lit(None).cast("string").alias("NormalizedReference"),
        F.array(F.col("b.BankTransactionID")).alias("BankTransactionIDs"),
        F.array(F.col("g.JournalEntryID")).alias("JournalEntryIDs"),
        F.col("b.SignedAmount").alias("BankSignedAmount"),
        F.col("g.GLSignedAmount").alias("GLSignedAmount"),
        (F.col("b.SignedAmount") - F.col("g.GLSignedAmount")).alias("AmountDifference"),
        F.col("b.TransactionDate").alias("BankFirstDate"),
        F.col("b.TransactionDate").alias("BankLastDate"),
        F.col("g.PostingDate").alias("GLFirstDate"),
        F.col("g.PostingDate").alias("GLLastDate"),
        F.lit(0).cast("int").alias("DateDifferenceDays"),
        F.lit(1).cast("bigint").alias("BankTransactionCount"),
        F.lit(1).cast("bigint").alias("GLEntryCount"),
        F.lit("POTENTIAL_MATCH").alias("ReconciliationStatus"),
        F.lit("DATE_AMOUNT_ONLY").alias("MatchMethod"),
        F.lit(
            "Reference evidence is missing; manual review is required."
        ).alias("ExceptionReason")
    )
)

# If a missing-reference bank record has no candidate, keep it bank-only.
bank_potential_ids = potential_matches.select(
    F.explode("BankTransactionIDs").alias("BankTransactionID")
).distinct()

bank_no_reference_unmatched = (
    bank_no_reference
    .join(
        bank_potential_ids,
        on="BankTransactionID",
        how="left_anti"
    )
    .select(
        F.lit(None).cast("string").alias("NormalizedReference"),
        F.array("BankTransactionID").alias("BankTransactionIDs"),
        F.array().cast("array<string>").alias("JournalEntryIDs"),
        F.col("SignedAmount").alias("BankSignedAmount"),
        F.lit(None).cast("decimal(18,2)").alias("GLSignedAmount"),
        F.lit(None).cast("decimal(18,2)").alias("AmountDifference"),
        F.col("TransactionDate").alias("BankFirstDate"),
        F.col("TransactionDate").alias("BankLastDate"),
        F.lit(None).cast("date").alias("GLFirstDate"),
        F.lit(None).cast("date").alias("GLLastDate"),
        F.lit(None).cast("int").alias("DateDifferenceDays"),
        F.lit(1).cast("bigint").alias("BankTransactionCount"),
        F.lit(0).cast("bigint").alias("GLEntryCount"),
        F.lit("BANK_ONLY").alias("ReconciliationStatus"),
        F.lit("NO_REFERENCE").alias("MatchMethod"),
        F.lit("No reference or date-and-amount candidate found.").alias("ExceptionReason")
    )
)

# Normalize the data types and union the result sets.
result_columns = reference_results.columns

potential_matches = potential_matches.select(*result_columns)
bank_no_reference_unmatched = bank_no_reference_unmatched.select(*result_columns)

reconciliation_results = (
    reference_results
    .unionByName(potential_matches)
    .unionByName(bank_no_reference_unmatched)
    .withColumn("ReconciliationRunID", F.lit(str(__import__("uuid").uuid4())))
    .withColumn("ProcessedAt", F.current_timestamp())
)

# ============================================================
# 7. PERSIST RESULTS AND EXCEPTIONS
# ============================================================

# Convert array columns to strings for Warehouse compatibility.
reconciliation_results = (
    reconciliation_results
    .withColumn(
        "BankTransactionIDs",
        F.array_join(F.col("BankTransactionIDs"), ",")
    )
    .withColumn(
        "JournalEntryIDs",
        F.array_join(F.col("JournalEntryIDs"), ",")
    )
)

# Persist the original reconciliation results table.
(
    reconciliation_results
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_reconciliationresults")
)

# Build exceptions from reconciliation results.
reconciliation_exceptions = (
    reconciliation_results
    .filter(
        F.col("ReconciliationStatus").isin(
            "LEDGER_ONLY",
            "BANK_ONLY",
            "POSSIBLE_DUPLICATE",
            "AMOUNT_DISCREPANCY",
            "DATE_MISMATCH",
            "POTENTIAL_MATCH"
        )
    )
)

# Persist the original reconciliation exceptions table.
(
    reconciliation_exceptions
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_reconciliationexceptions")
)

# Build reconciliation summary.
reconciliation_summary = (
    reconciliation_results
    .groupBy("ReconciliationStatus")
    .agg(
        F.count("*").alias("ReferenceGroupCount"),
        F.sum(
            F.coalesce(F.col("BankTransactionCount"), F.lit(0))
        ).alias("BankTransactionCount"),
        F.sum(
            F.coalesce(F.col("GLEntryCount"), F.lit(0))
        ).alias("GLEntryCount")
    )
    .withColumn("ProcessedAt", F.current_timestamp())
)

# Persist reconciliation summary.
(
    reconciliation_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_reconciliationsummary")
)

# ============================================================
# 8. DISPLAY RESULTS
# ============================================================

print("Reconciliation engine execution completed.")
print("Gold_ReconciliationResults rows:", reconciliation_results.count())
print("Gold_ReconciliationExceptions rows:", reconciliation_exceptions.count())

print("Reconciliation status summary:")
display(reconciliation_summary.orderBy("ReconciliationStatus"))

print("Reconciliation results:")
display(
    reconciliation_results.orderBy(
        "ReconciliationStatus", "NormalizedReference"
    )
)

print("Exceptions requiring review:")
display(
    reconciliation_exceptions.orderBy(
        "ReconciliationStatus", "NormalizedReference"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 9 — Validate Reconciliation Results Against Expected Scenarios

# CELL ********************

from pyspark.sql import functions as F

results = spark.table("Gold_ReconciliationResults")

def has_status(reference, status):
    return (
        results
        .filter(
            (F.col("NormalizedReference") == reference) &
            (F.col("ReconciliationStatus") == status)
        )
        .limit(1)
        .count() > 0
    )

checks = {
    "REF-001 duplicate-like group flagged":
        has_status("REF-001", "POSSIBLE_DUPLICATE"),

    "REF-013 amount discrepancy flagged":
        has_status("REF-013", "AMOUNT_DISCREPANCY"),

    "REF-014 date mismatch flagged":
        has_status("REF-014", "DATE_MISMATCH"),

    "GROUP-010 confirmed grouped match":
        has_status("GROUP-010", "CONFIRMED_GROUP_MATCH"),

    "GROUP-012 confirmed grouped match":
        has_status("GROUP-012", "CONFIRMED_GROUP_MATCH"),

    "REF-018 confirmed match":
        has_status("REF-018", "CONFIRMED_MATCH"),

    "REF-019 confirmed match":
        has_status("REF-019", "CONFIRMED_MATCH"),

    "REF-020 confirmed match":
        has_status("REF-020", "CONFIRMED_MATCH"),

    "Missing-reference candidate kept potential":
        results.filter(
            F.col("ReconciliationStatus") == "POTENTIAL_MATCH"
        ).limit(1).count() > 0,

    "Bank-only item identified":
        results.filter(
            F.col("ReconciliationStatus") == "BANK_ONLY"
        ).limit(1).count() > 0,

    "Ledger-only items identified":
        results.filter(
            F.col("ReconciliationStatus") == "LEDGER_ONLY"
        ).limit(1).count() > 0
}

for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")

failed = [name for name, passed in checks.items() if not passed]

if failed:
    raise AssertionError(
        "Reconciliation acceptance checks failed: " + "; ".join(failed)
    )

print("\nAll reconciliation acceptance checks passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 10 — Audit Reconciliation Coverage and Transaction Integrity

# CELL ********************

from pyspark.sql import functions as F

results = spark.table("Gold_ReconciliationResults")
bank = spark.table("Silver_BankTransactions")
gl = spark.table("Silver_JournalLines")

# Build the source-level GL entry list from validated journal lines.
gl_entries = gl.select("JournalEntryID").distinct()

# Flatten bank transaction IDs from reconciliation results.
bank_allocations = (
    results
    .select(F.explode_outer("BankTransactionIDs").alias("BankTransactionID"))
    .filter(F.col("BankTransactionID").isNotNull())
)

bank_reuse = (
    bank_allocations
    .groupBy("BankTransactionID")
    .count()
    .filter(F.col("count") > 1)
)

# Flatten GL journal entry IDs from reconciliation results.
gl_allocations = (
    results
    .select(F.explode_outer("JournalEntryIDs").alias("JournalEntryID"))
    .filter(F.col("JournalEntryID").isNotNull())
)

gl_reuse = (
    gl_allocations
    .groupBy("JournalEntryID")
    .count()
    .filter(F.col("count") > 1)
)

# Identify source records not represented in the reconciliation output.
bank_unrepresented = (
    bank.select("BankTransactionID").distinct()
    .join(
        bank_allocations.select("BankTransactionID").distinct(),
        on="BankTransactionID",
        how="left_anti"
    )
)

gl_unrepresented = (
    gl_entries
    .join(
        gl_allocations.select("JournalEntryID").distinct(),
        on="JournalEntryID",
        how="left_anti"
    )
)

print("SOURCE COUNTS")
print("Bank transactions:", bank.select("BankTransactionID").distinct().count())
print("Distinct GL journal entries:", gl_entries.count())

print("\nALLOCATION COUNTS")
print("Distinct bank IDs represented:",
      bank_allocations.select("BankTransactionID").distinct().count())
print("Distinct GL entries represented:",
      gl_allocations.select("JournalEntryID").distinct().count())

print("\nBANK TRANSACTIONS REUSED ACROSS RESULT ROWS")
display(bank_reuse)

print("\nGL JOURNAL ENTRIES REUSED ACROSS RESULT ROWS")
display(gl_reuse)

print("\nBANK TRANSACTIONS NOT REPRESENTED")
display(bank_unrepresented)

print("\nGL JOURNAL ENTRIES NOT REPRESENTED")
display(gl_unrepresented)

print("\nSTATUS SUMMARY")
display(
    results.groupBy("ReconciliationStatus")
           .count()
           .orderBy("ReconciliationStatus")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transaction-Level Reconciliation Allocations
# 
# Create an auditable allocation ledger from the Silver source tables.
# 
# Controls:
# - Allocate amounts only when reference, signed amount, and date evidence support the match.
# - Allow grouped settlements only when the group totals reconcile.
# - Record each bank transaction and GL journal entry separately.
# - Never confirm duplicate-like or missing-reference candidates automatically.
# - Preserve discrepancies and unmatched items for review.
# - Validate that allocation amounts reconcile and source records are not reused improperly.

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DateType, DecimalType, TimestampType
)
from decimal import Decimal
from datetime import datetime, timezone
import uuid

# ============================================================
# 1. LOAD SOURCE TABLES
# ============================================================

bank = spark.table("Silver_BankTransactions")
gl = spark.table("Silver_JournalLines")
coa = spark.table("Dim_ChartOfAccounts")
mapping = spark.table("Dim_AccountMapping")

run_id = str(uuid.uuid4())
processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
ZERO = Decimal("0.00")
TOLERANCE = Decimal("0.01")

def as_decimal(value):
    return Decimal(str(value)).quantize(Decimal("0.01"))

# ============================================================
# 2. PREPARE BANK TRANSACTIONS
# ============================================================

bank_prepared = bank.withColumn(
    "SignedAmount",
    F.coalesce(
        F.col("DebitAmount"),
        F.lit(0).cast("decimal(18,2)")
    ) - F.coalesce(
        F.col("CreditAmount"),
        F.lit(0).cast("decimal(18,2)")
    )
)

bank_prepared = bank_prepared.withColumn(
    "NormalizedReference",
    F.when(
        F.col("ReferenceNumber").isNull()
        | (F.trim(F.col("ReferenceNumber")) == ""),
        F.lit(None).cast("string")
    ).otherwise(F.trim(F.col("ReferenceNumber")))
)

bank_rows = bank_prepared.select(
    "BankTransactionID",
    "TransactionDate",
    "NormalizedReference",
    "SignedAmount",
    "Description"
).collect()

bank_by_ref = {}

for row in bank_rows:
    ref = row["NormalizedReference"]

    if ref is not None:
        bank_by_ref.setdefault(ref, []).append({
            "id": row["BankTransactionID"],
            "date": row["TransactionDate"],
            "ref": ref,
            "amount": as_decimal(row["SignedAmount"]),
            "description": row["Description"]
        })

# Flag duplicate-like bank records for manual review.
duplicate_keys = set()

for ref, items in bank_by_ref.items():
    counts = {}

    for item in items:
        key = (item["date"], item["amount"])
        counts[key] = counts.get(key, 0) + 1

    for item in items:
        key = (item["date"], item["amount"])
        if counts[key] > 1:
            duplicate_keys.add(item["id"])

# ============================================================
# 3. RESOLVE GL BANK-SIDE ENTRIES
# ============================================================

# Use only effective ERP mappings to the active Operating Bank
# account (account code 1000).

erp_mapping = mapping.filter(
    F.col("SourceSystem") == "ERP"
).alias("m")

active_coa = coa.filter(
    F.col("IsActive") == True
).alias("c")

gl_bank_lines = (
    gl.alias("g")
    .join(
        erp_mapping,
        (
            (F.col("g.AccountCode") == F.col("m.SourceAccountCode"))
            & (F.col("g.PostingDate") >= F.col("m.EffectiveFrom"))
            & (
                F.col("m.EffectiveTo").isNull()
                | (F.col("g.PostingDate") <= F.col("m.EffectiveTo"))
            )
        ),
        "inner"
    )
    .join(
        active_coa,
        (
            (F.col("m.TargetAccountID") == F.col("c.AccountID"))
            & (F.col("g.AccountCode") == F.col("c.AccountCode"))
        ),
        "inner"
    )
    .filter(F.col("c.AccountCode") == "1000")
    .select(
        F.col("g.JournalEntryID").alias("JournalEntryID"),
        F.col("g.PostingDate").alias("PostingDate"),
        F.col("g.ReferenceNumber").alias("ReferenceNumber"),
        F.col("g.DebitAmount").alias("DebitAmount"),
        F.col("g.CreditAmount").alias("CreditAmount")
    )
)

gl_bank_lines = gl_bank_lines.withColumn(
    "SignedAmount",
    F.coalesce(
        F.col("DebitAmount"),
        F.lit(0).cast("decimal(18,2)")
    ) - F.coalesce(
        F.col("CreditAmount"),
        F.lit(0).cast("decimal(18,2)")
    )
)

gl_bank_lines = gl_bank_lines.withColumn(
    "NormalizedReference",
    F.when(
        F.col("ReferenceNumber").isNull()
        | (F.trim(F.col("ReferenceNumber")) == ""),
        F.lit(None).cast("string")
    ).otherwise(F.trim(F.col("ReferenceNumber")))
)

gl_bank_lines = (
    gl_bank_lines
    .groupBy(
        "JournalEntryID",
        "PostingDate",
        "NormalizedReference"
    )
    .agg(F.sum("SignedAmount").alias("SignedAmount"))
)

gl_rows = gl_bank_lines.collect()
gl_by_ref = {}

for row in gl_rows:
    ref = row["NormalizedReference"]

    if ref is not None:
        gl_by_ref.setdefault(ref, []).append({
            "id": row["JournalEntryID"],
            "date": row["PostingDate"],
            "ref": ref,
            "amount": as_decimal(row["SignedAmount"])
        })

# ============================================================
# 4. ALLOCATE CONFIRMED REFERENCE GROUPS
# ============================================================

allocation_records = []
review_records = []
used_bank_ids = set()
used_gl_ids = set()

def add_review(
    ref, status, bank_ids, gl_ids,
    bank_amount, gl_amount, reason
):
    review_records.append({
        "AllocationRunID": run_id,
        "ReferenceNumber": ref,
        "ReconciliationStatus": status,
        "BankTransactionIDs": (
            ",".join(sorted(bank_ids)) if bank_ids else None
        ),
        "JournalEntryIDs": (
            ",".join(sorted(gl_ids)) if gl_ids else None
        ),
        "BankGroupAmount": (
            str(bank_amount) if bank_amount is not None else None
        ),
        "GLGroupAmount": (
            str(gl_amount) if gl_amount is not None else None
        ),
        "ReviewReason": reason,
        "ProcessedAt": processed_at
    })

all_refs = sorted(set(bank_by_ref) | set(gl_by_ref))

for ref in all_refs:
    bank_items = sorted(
        bank_by_ref.get(ref, []),
        key=lambda x: (x["date"], x["id"])
    )

    gl_items = sorted(
        gl_by_ref.get(ref, []),
        key=lambda x: (x["date"], x["id"])
    )

    bank_total = sum(
        (x["amount"] for x in bank_items), ZERO
    )
    gl_total = sum(
        (x["amount"] for x in gl_items), ZERO
    )

    bank_ids = [x["id"] for x in bank_items]
    gl_ids = [x["id"] for x in gl_items]

    # Duplicate-like records must not be auto-confirmed.
    if any(x["id"] in duplicate_keys for x in bank_items):
        add_review(
            ref, "POSSIBLE_DUPLICATE",
            bank_ids, gl_ids, bank_total, gl_total,
            "Duplicate-like bank records require manual review."
        )
        continue

    if not bank_items:
        add_review(
            ref, "LEDGER_ONLY", [], gl_ids, None, gl_total,
            "No bank transaction has this reference."
        )
        continue

    if not gl_items:
        add_review(
            ref, "BANK_ONLY", bank_ids, [], bank_total, None,
            "No mapped GL bank-side entry has this reference."
        )
        continue

    bank_dates = {x["date"] for x in bank_items}
    gl_dates = {x["date"] for x in gl_items}

    if bank_dates != gl_dates:
        add_review(
            ref, "DATE_MISMATCH",
            bank_ids, gl_ids, bank_total, gl_total,
            "Bank transaction dates and GL posting dates differ."
        )
        continue

    # For automatic allocation, require one common date.
    if len(bank_dates) != 1 or len(gl_dates) != 1:
        add_review(
            ref, "ALLOCATION_REVIEW",
            bank_ids, gl_ids, bank_total, gl_total,
            "Multiple dates exist in the reference group; manual review required."
        )
        continue

    if abs(bank_total - gl_total) > TOLERANCE:
        add_review(
            ref, "AMOUNT_DISCREPANCY",
            bank_ids, gl_ids, bank_total, gl_total,
            "Signed bank and GL group totals differ."
        )
        continue

    # Work on copies so failed allocations do not leave partial rows.
    bank_remaining = [
        {"item": x, "remaining": x["amount"]}
        for x in bank_items
    ]

    gl_remaining = [
        {"item": x, "remaining": x["amount"]}
        for x in gl_items
    ]

    group_allocations = []
    valid_allocation = True

    for b in bank_remaining:
        while abs(b["remaining"]) > TOLERANCE:
            candidate = next(
                (
                    g for g in gl_remaining
                    if abs(g["remaining"]) > TOLERANCE
                    and g["remaining"] * b["remaining"] > 0
                ),
                None
            )

            if candidate is None:
                valid_allocation = False
                break

            allocated = min(
                abs(b["remaining"]),
                abs(candidate["remaining"])
            )

            signed_allocated = (
                allocated if b["remaining"] > 0 else -allocated
            )

            group_allocations.append({
                "AllocationRunID": run_id,
                "ReferenceNumber": ref,
                "BankTransactionID": b["item"]["id"],
                "JournalEntryID": candidate["item"]["id"],
                "BankTransactionDate": b["item"]["date"],
                "GLPostingDate": candidate["item"]["date"],
                "AllocatedSignedAmount": signed_allocated,
                "BankSignedAmount": b["item"]["amount"],
                "GLSignedAmount": candidate["item"]["amount"],
                "MatchMethod": (
                    "REFERENCE_AMOUNT_DATE"
                    if len(bank_items) == 1 and len(gl_items) == 1
                    else "GROUP_REFERENCE_AMOUNT_DATE"
                ),
                "ReconciliationStatus": (
                    "CONFIRMED_MATCH"
                    if len(bank_items) == 1 and len(gl_items) == 1
                    else "CONFIRMED_GROUP_MATCH"
                ),
                "ProcessedAt": processed_at
            })

            b["remaining"] -= signed_allocated
            candidate["remaining"] -= signed_allocated

        if not valid_allocation:
            break

    if (
        not valid_allocation
        or any(abs(x["remaining"]) > TOLERANCE
               for x in bank_remaining)
        or any(abs(x["remaining"]) > TOLERANCE
               for x in gl_remaining)
    ):
        add_review(
            ref, "ALLOCATION_REVIEW",
            bank_ids, gl_ids, bank_total, gl_total,
            "Group totals agree, but complete direction-consistent allocations could not be built."
        )
        continue

    # A source record cannot be reused across different reference groups.
    if (
        any(x in used_bank_ids for x in bank_ids)
        or any(x in used_gl_ids for x in gl_ids)
    ):
        raise ValueError(
            f"Source record reused across reference groups: {ref}"
        )

    allocation_records.extend(group_allocations)
    used_bank_ids.update(bank_ids)
    used_gl_ids.update(gl_ids)

# ============================================================
# 5. MISSING-REFERENCE CANDIDATES: REVIEW ONLY
# ============================================================

bank_missing_rows = (
    bank_prepared
    .filter(F.col("NormalizedReference").isNull())
    .select(
        "BankTransactionID",
        "TransactionDate",
        "SignedAmount"
    )
    .collect()
)

gl_missing_rows = (
    gl_bank_lines
    .filter(F.col("NormalizedReference").isNull())
    .select(
        "JournalEntryID",
        "PostingDate",
        "SignedAmount"
    )
    .collect()
)

bank_missing_ref = [
    {
        "id": r["BankTransactionID"],
        "date": r["TransactionDate"],
        "amount": as_decimal(r["SignedAmount"])
    }
    for r in bank_missing_rows
]

gl_missing_ref = [
    {
        "id": r["JournalEntryID"],
        "date": r["PostingDate"],
        "amount": as_decimal(r["SignedAmount"])
    }
    for r in gl_missing_rows
]

potential_gl_ids = set()

for b in bank_missing_ref:
    candidates = [
        g for g in gl_missing_ref
        if g["date"] == b["date"]
        and abs(g["amount"] - b["amount"]) <= TOLERANCE
    ]

    if candidates:
        candidate_ids = [g["id"] for g in candidates]
        potential_gl_ids.update(candidate_ids)

        add_review(
            None, "POTENTIAL_MATCH",
            [b["id"]], candidate_ids,
            b["amount"],
            sum((g["amount"] for g in candidates), ZERO),
            "Date and amount agree, but the reference is missing; manual confirmation required."
        )

    elif b["id"] not in used_bank_ids:
        add_review(
            None, "BANK_ONLY",
            [b["id"]], [], b["amount"], None,
            "Missing reference and no date-and-amount candidate found."
        )

for g in gl_missing_ref:
    if (
        g["id"] not in used_gl_ids
        and g["id"] not in potential_gl_ids
    ):
        add_review(
            None, "LEDGER_ONLY",
            [], [g["id"]], None, g["amount"],
            "Missing reference and no confirmed allocation."
        )

# ============================================================
# 6. CREATE OUTPUT DATAFRAMES
# ============================================================

allocation_schema = StructType([
    StructField("AllocationRunID", StringType(), False),
    StructField("ReferenceNumber", StringType(), True),
    StructField("BankTransactionID", StringType(), False),
    StructField("JournalEntryID", StringType(), False),
    StructField("BankTransactionDate", DateType(), True),
    StructField("GLPostingDate", DateType(), True),
    StructField("AllocatedSignedAmount", DecimalType(18, 2), False),
    StructField("BankSignedAmount", DecimalType(18, 2), False),
    StructField("GLSignedAmount", DecimalType(18, 2), False),
    StructField("MatchMethod", StringType(), False),
    StructField("ReconciliationStatus", StringType(), False),
    StructField("ProcessedAt", TimestampType(), False)
])

allocation_rows = [
    (
        r["AllocationRunID"],
        r["ReferenceNumber"],
        r["BankTransactionID"],
        r["JournalEntryID"],
        r["BankTransactionDate"],
        r["GLPostingDate"],
        r["AllocatedSignedAmount"],
        r["BankSignedAmount"],
        r["GLSignedAmount"],
        r["MatchMethod"],
        r["ReconciliationStatus"],
        r["ProcessedAt"]
    )
    for r in allocation_records
]

allocation_df = spark.createDataFrame(
    allocation_rows,
    schema=allocation_schema
)

review_schema = StructType([
    StructField("AllocationRunID", StringType(), False),
    StructField("ReferenceNumber", StringType(), True),
    StructField("ReconciliationStatus", StringType(), False),
    StructField("BankTransactionIDs", StringType(), True),
    StructField("JournalEntryIDs", StringType(), True),
    StructField("BankGroupAmount", StringType(), True),
    StructField("GLGroupAmount", StringType(), True),
    StructField("ReviewReason", StringType(), True),
    StructField("ProcessedAt", TimestampType(), False)
])

review_rows = [
    (
        r["AllocationRunID"],
        r["ReferenceNumber"],
        r["ReconciliationStatus"],
        r["BankTransactionIDs"],
        r["JournalEntryIDs"],
        r["BankGroupAmount"],
        r["GLGroupAmount"],
        r["ReviewReason"],
        r["ProcessedAt"]
    )
    for r in review_records
]

review_df = spark.createDataFrame(
    review_rows,
    schema=review_schema
)

# ============================================================
# 7. VALIDATE ALLOCATIONS BEFORE WRITING
# ============================================================

# Multiple rows per bank transaction or GL entry are allowed for
# grouped matches. Validate the summed allocations instead.

if allocation_df.filter(
    F.col("AllocatedSignedAmount").isNull()
).limit(1).count() > 0:
    raise AssertionError("Null allocation amounts were produced.")

bank_allocated = (
    allocation_df
    .groupBy("BankTransactionID")
    .agg(F.sum("AllocatedSignedAmount").alias("AllocatedTotal"))
)

bank_source_amounts = bank_prepared.select(
    F.col("BankTransactionID"),
    F.col("SignedAmount").alias("SourceAmount")
)

bank_check = (
    bank_allocated
    .join(bank_source_amounts, "BankTransactionID", "inner")
    .filter(
        F.abs(
            F.col("AllocatedTotal") - F.col("SourceAmount")
        ) > F.lit(0.01)
    )
)

gl_allocated = (
    allocation_df
    .groupBy("JournalEntryID")
    .agg(F.sum("AllocatedSignedAmount").alias("AllocatedTotal"))
)

gl_source_amounts = gl_bank_lines.select(
    F.col("JournalEntryID"),
    F.col("SignedAmount").alias("SourceAmount")
)

gl_check = (
    gl_allocated
    .join(gl_source_amounts, "JournalEntryID", "inner")
    .filter(
        F.abs(
            F.col("AllocatedTotal") - F.col("SourceAmount")
        ) > F.lit(0.01)
    )
)

if bank_check.limit(1).count() > 0:
    display(bank_check)
    raise AssertionError(
        "Bank allocation totals do not equal source transaction amounts."
    )

if gl_check.limit(1).count() > 0:
    display(gl_check)
    raise AssertionError(
        "GL allocation totals do not equal source bank-side amounts."
    )

print("Allocation rows:", allocation_df.count())
print("Review rows:", review_df.count())
print("Bank allocation-total validation: PASSED")
print("GL allocation-total validation: PASSED")

# ============================================================
# 8. WRITE GOLD TABLES
# ============================================================

(
    allocation_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Gold_ReconciliationAllocations")
)

(
    review_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Gold_ReconciliationAllocationReview")
)

print("Created Gold_ReconciliationAllocations")
print("Created Gold_ReconciliationAllocationReview")

display(
    spark.table("Gold_ReconciliationAllocations")
    .orderBy("ReferenceNumber", "BankTransactionID", "JournalEntryID")
)

display(
    spark.table("Gold_ReconciliationAllocationReview")
    .orderBy("ReconciliationStatus", "ReferenceNumber")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from decimal import Decimal

# ============================================================
# CELL 13 — ALLOCATION ACCEPTANCE TESTS
# ============================================================

allocations = spark.table("Gold_ReconciliationAllocations")
reviews = spark.table("Gold_ReconciliationAllocationReview")

bank_source = (
    spark.table("Silver_BankTransactions")
    .withColumn(
        "SignedAmount",
        F.coalesce(
            F.col("DebitAmount"),
            F.lit(0).cast("decimal(18,2)")
        ) - F.coalesce(
            F.col("CreditAmount"),
            F.lit(0).cast("decimal(18,2)")
        )
    )
)

# Rebuild the mapped GL bank-side amounts.
gl = spark.table("Silver_JournalLines")
coa = spark.table("Dim_ChartOfAccounts")
mapping = spark.table("Dim_AccountMapping")

gl_source = (
    gl.alias("g")
    .join(
        mapping.filter(F.col("SourceSystem") == "ERP").alias("m"),
        (
            (F.col("g.AccountCode") == F.col("m.SourceAccountCode"))
            & (F.col("g.PostingDate") >= F.col("m.EffectiveFrom"))
            & (
                F.col("m.EffectiveTo").isNull()
                | (F.col("g.PostingDate") <= F.col("m.EffectiveTo"))
            )
        ),
        "inner"
    )
    .join(
        coa.filter(F.col("IsActive") == True).alias("c"),
        (
            (F.col("m.TargetAccountID") == F.col("c.AccountID"))
            & (F.col("g.AccountCode") == F.col("c.AccountCode"))
        ),
        "inner"
    )
    .filter(F.col("c.AccountCode") == "1000")
    .select(
        F.col("g.JournalEntryID").alias("JournalEntryID"),
        F.col("g.DebitAmount").alias("DebitAmount"),
        F.col("g.CreditAmount").alias("CreditAmount")
    )
    .withColumn(
        "SignedAmount",
        F.coalesce(
            F.col("DebitAmount"),
            F.lit(0).cast("decimal(18,2)")
        ) - F.coalesce(
            F.col("CreditAmount"),
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .groupBy("JournalEntryID")
    .agg(F.sum("SignedAmount").alias("SourceAmount"))
)

failures = []

def check(condition, pass_message, fail_message):
    if condition:
        print(f"PASS: {pass_message}")
    else:
        print(f"FAIL: {fail_message}")
        failures.append(fail_message)

# ------------------------------------------------------------
# 1. BANK ALLOCATION TOTALS
# ------------------------------------------------------------

bank_allocated = (
    allocations
    .groupBy("BankTransactionID")
    .agg(F.sum("AllocatedSignedAmount").alias("AllocatedTotal"))
)

bank_expected = bank_source.select(
    "BankTransactionID",
    F.col("SignedAmount").alias("SourceAmount")
)

bank_errors = (
    bank_allocated
    .join(bank_expected, "BankTransactionID", "inner")
    .filter(
        F.abs(
            F.col("AllocatedTotal") - F.col("SourceAmount")
        ) > F.lit(0.01)
    )
)

bank_allocated_ids = {
    r["BankTransactionID"]
    for r in allocations.select("BankTransactionID").distinct().collect()
}

bank_expected_totals = {
    r["BankTransactionID"]: r["SourceAmount"]
    for r in bank_expected
    .filter(F.col("BankTransactionID").isin(list(bank_allocated_ids)))
    .collect()
}

bank_actual_totals = {
    r["BankTransactionID"]: r["AllocatedTotal"]
    for r in bank_allocated.collect()
}

bank_missing_or_mismatched = [
    bank_id for bank_id in bank_allocated_ids
    if bank_id not in bank_expected_totals
    or bank_id not in bank_actual_totals
    or abs(
        Decimal(str(bank_actual_totals[bank_id]))
        - Decimal(str(bank_expected_totals[bank_id]))
    ) > Decimal("0.01")
]

check(
    bank_errors.limit(1).count() == 0
    and len(bank_missing_or_mismatched) == 0,
    "Allocated bank totals equal source transaction amounts",
    "Bank allocation totals do not reconcile"
)

# ------------------------------------------------------------
# 2. GL ALLOCATION TOTALS
# ------------------------------------------------------------

gl_allocated = (
    allocations
    .groupBy("JournalEntryID")
    .agg(F.sum("AllocatedSignedAmount").alias("AllocatedTotal"))
)

gl_errors = (
    gl_allocated
    .join(gl_source, "JournalEntryID", "inner")
    .filter(
        F.abs(
            F.col("AllocatedTotal") - F.col("SourceAmount")
        ) > F.lit(0.01)
    )
)

gl_allocated_ids = {
    r["JournalEntryID"]
    for r in allocations.select("JournalEntryID").distinct().collect()
}

gl_expected_totals = {
    r["JournalEntryID"]: r["SourceAmount"]
    for r in gl_source
    .filter(F.col("JournalEntryID").isin(list(gl_allocated_ids)))
    .collect()
}

gl_actual_totals = {
    r["JournalEntryID"]: r["AllocatedTotal"]
    for r in gl_allocated.collect()
}

gl_missing_or_mismatched = [
    gl_id for gl_id in gl_allocated_ids
    if gl_id not in gl_expected_totals
    or gl_id not in gl_actual_totals
    or abs(
        Decimal(str(gl_actual_totals[gl_id]))
        - Decimal(str(gl_expected_totals[gl_id]))
    ) > Decimal("0.01")
]

check(
    gl_errors.limit(1).count() == 0
    and len(gl_missing_or_mismatched) == 0,
    "Allocated GL totals equal source bank-side amounts",
    "GL allocation totals do not reconcile"
)

# ------------------------------------------------------------
# 3. GROUPED MATCH TOTALS
# ------------------------------------------------------------

group010 = (
    allocations
    .filter(F.col("ReferenceNumber") == "GROUP-010")
    .agg(F.sum("AllocatedSignedAmount").alias("Total"))
    .first()["Total"]
)

check(
    group010 is not None and abs(Decimal(str(group010)) - Decimal("300.00")) <= Decimal("0.01"),
    "GROUP-010 allocation totals 300",
    "GROUP-010 allocation total is incorrect"
)

group012 = (
    allocations
    .filter(F.col("ReferenceNumber") == "GROUP-012")
    .agg(F.sum("AllocatedSignedAmount").alias("Total"))
    .first()["Total"]
)

check(
    group012 is not None and abs(Decimal(str(group012)) - Decimal("450.00")) <= Decimal("0.01"),
    "GROUP-012 allocation totals 450",
    "GROUP-012 allocation total is incorrect"
)

# ------------------------------------------------------------
# 4. EXCEPTION SAFEGUARDS
# ------------------------------------------------------------

ref001_allocations = allocations.filter(
    F.col("ReferenceNumber") == "REF-001"
).count()

check(
    ref001_allocations == 0,
    "REF-001 not automatically allocated",
    "REF-001 was incorrectly allocated"
)

ref013_review = reviews.filter(
    (F.col("ReferenceNumber") == "REF-013")
    & (F.col("ReconciliationStatus") == "AMOUNT_DISCREPANCY")
).count()

check(
    ref013_review > 0,
    "B013 discrepancy remains for review",
    "B013 discrepancy review is missing"
)

ref014_review = reviews.filter(
    (F.col("ReferenceNumber") == "REF-014")
    & (F.col("ReconciliationStatus") == "DATE_MISMATCH")
).count()

check(
    ref014_review > 0,
    "B014 date mismatch remains for review",
    "B014 date-mismatch review is missing"
)

# ------------------------------------------------------------
# 5. FINAL RESULT
# ------------------------------------------------------------

print("\nAllocation acceptance summary")
print("Allocation rows:", allocations.count())
print("Review rows:", reviews.count())
print("Failed tests:", len(failures))

if failures:
    raise AssertionError(
        "Allocation acceptance failed: " + "; ".join(failures)
    )

print("ALL ALLOCATION ACCEPTANCE TESTS PASSED")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
