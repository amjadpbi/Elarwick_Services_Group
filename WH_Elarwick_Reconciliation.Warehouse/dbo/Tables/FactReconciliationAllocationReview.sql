CREATE TABLE [dbo].[FactReconciliationAllocationReview] (
    [AllocationRunID]      VARCHAR (8000) NULL,
    [ReferenceNumber]      VARCHAR (8000) NULL,
    [ReconciliationStatus] VARCHAR (8000) NULL,
    [BankTransactionIDs]   VARCHAR (8000) NULL,
    [JournalEntryIDs]      VARCHAR (8000) NULL,
    [BankGroupAmount]      VARCHAR (8000) NULL,
    [GLGroupAmount]        VARCHAR (8000) NULL,
    [ReviewReason]         VARCHAR (8000) NULL,
    [ProcessedAt]          DATETIME2 (6)  NULL
);


GO