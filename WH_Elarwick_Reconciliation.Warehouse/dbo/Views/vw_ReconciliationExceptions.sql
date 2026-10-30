CREATE VIEW dbo.vw_ReconciliationExceptions
AS
SELECT
    NormalizedReference,
    BankTransactionIDs,
    JournalEntryIDs,
    BankSignedAmount,
    GLSignedAmount,
    AmountDifference,
    BankFirstDate,
    BankLastDate,
    GLFirstDate,
    GLLastDate,
    DateDifferenceDays,
    BankTransactionCount,
    GLEntryCount,
    ReconciliationStatus,
    MatchMethod,
    ExceptionReason,
    ReconciliationRunID,
    ProcessedAt
FROM dbo.FactReconciliationExceptions;

GO