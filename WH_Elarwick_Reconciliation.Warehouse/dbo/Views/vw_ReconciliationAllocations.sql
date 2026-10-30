CREATE VIEW dbo.vw_ReconciliationAllocations
AS
SELECT
    AllocationRunID,
    ReferenceNumber,
    BankTransactionID,
    JournalEntryID,
    BankTransactionDate,
    GLPostingDate,
    AllocatedSignedAmount,
    BankSignedAmount,
    GLSignedAmount,
    MatchMethod,
    ReconciliationStatus,
    ProcessedAt
FROM dbo.FactReconciliationAllocations;

GO