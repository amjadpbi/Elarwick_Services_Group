CREATE VIEW dbo.vw_ReconciliationAllocationReview
AS
SELECT
    AllocationRunID,
    ReferenceNumber,
    ReconciliationStatus,
    BankTransactionIDs,
    JournalEntryIDs,
    BankGroupAmount,
    GLGroupAmount,
    ReviewReason,
    ProcessedAt
FROM dbo.FactReconciliationAllocationReview;

GO