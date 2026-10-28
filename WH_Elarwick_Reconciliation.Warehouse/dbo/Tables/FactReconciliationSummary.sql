CREATE TABLE [dbo].[FactReconciliationSummary] (
    [ReconciliationStatus] VARCHAR (8000) NULL,
    [ReferenceGroupCount]  BIGINT         NULL,
    [BankTransactionCount] BIGINT         NULL,
    [GLEntryCount]         BIGINT         NULL,
    [ProcessedAt]          DATETIME2 (6)  NULL
);


GO