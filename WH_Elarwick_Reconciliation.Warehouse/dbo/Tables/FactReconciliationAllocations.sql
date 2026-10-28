CREATE TABLE [dbo].[FactReconciliationAllocations] (
    [AllocationRunID]       VARCHAR (8000)  NULL,
    [ReferenceNumber]       VARCHAR (8000)  NULL,
    [BankTransactionID]     VARCHAR (8000)  NULL,
    [JournalEntryID]        VARCHAR (8000)  NULL,
    [BankTransactionDate]   DATE            NULL,
    [GLPostingDate]         DATE            NULL,
    [AllocatedSignedAmount] DECIMAL (18, 2) NULL,
    [BankSignedAmount]      DECIMAL (18, 2) NULL,
    [GLSignedAmount]        DECIMAL (18, 2) NULL,
    [MatchMethod]           VARCHAR (8000)  NULL,
    [ReconciliationStatus]  VARCHAR (8000)  NULL,
    [ProcessedAt]           DATETIME2 (6)   NULL
);


GO