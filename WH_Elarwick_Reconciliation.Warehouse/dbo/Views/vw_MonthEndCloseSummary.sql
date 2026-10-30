CREATE VIEW dbo.vw_MonthEndCloseSummary
AS
SELECT
    ReconciliationStatus,
    COUNT(*) AS ReferenceGroupCount,
    SUM(BankTransactionCount) AS BankTransactionCount,
    SUM(GLEntryCount) AS GLEntryCount,
    SUM(BankSignedAmount) AS TotalBankSignedAmount,
    SUM(GLSignedAmount) AS TotalGLSignedAmount,
    SUM(AmountDifference) AS TotalAmountDifference,
    SUM(
        CASE
            WHEN ReconciliationStatus IN (
                'CONFIRMED_MATCH',
                'CONFIRMED_GROUP_MATCH'
            )
            THEN 0
            ELSE 1
        END
    ) AS ReviewGroupCount,
    MAX(ProcessedAt) AS LatestProcessedAt
FROM dbo.FactReconciliationResults
GROUP BY ReconciliationStatus;

GO