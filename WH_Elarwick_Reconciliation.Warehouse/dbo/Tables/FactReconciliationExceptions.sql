CREATE TABLE [dbo].[FactReconciliationExceptions] (
    [NormalizedReference]  VARCHAR (8000)  NULL,
    [BankTransactionIDs]   VARCHAR (8000)  NULL,
    [JournalEntryIDs]      VARCHAR (8000)  NULL,
    [BankSignedAmount]     DECIMAL (29, 2) NULL,
    [GLSignedAmount]       DECIMAL (38, 2) NULL,
    [AmountDifference]     DECIMAL (38, 2) NULL,
    [BankFirstDate]        DATE            NULL,
    [BankLastDate]         DATE            NULL,
    [GLFirstDate]          DATE            NULL,
    [GLLastDate]           DATE            NULL,
    [DateDifferenceDays]   INT             NULL,
    [BankTransactionCount] BIGINT          NULL,
    [GLEntryCount]         BIGINT          NULL,
    [ReconciliationStatus] VARCHAR (8000)  NULL,
    [MatchMethod]          VARCHAR (8000)  NULL,
    [ExceptionReason]      VARCHAR (8000)  NULL,
    [ReconciliationRunID]  VARCHAR (8000)  NULL,
    [ProcessedAt]          DATETIME2 (6)   NULL
);


GO