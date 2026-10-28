CREATE TABLE [dbo].[DimChartOfAccounts] (
    [AccountID]      VARCHAR (8000) NULL,
    [AccountCode]    VARCHAR (8000) NULL,
    [AccountName]    VARCHAR (8000) NULL,
    [AccountType]    VARCHAR (8000) NULL,
    [IsActive]       BIT            NULL,
    [BatchID]        VARCHAR (8000) NULL,
    [SourceFileName] VARCHAR (8000) NULL,
    [SourceFileHash] VARCHAR (8000) NULL,
    [IngestedAt]     DATETIME2 (6)  NULL,
    [SourceRowHash]  VARCHAR (8000) NULL
);


GO