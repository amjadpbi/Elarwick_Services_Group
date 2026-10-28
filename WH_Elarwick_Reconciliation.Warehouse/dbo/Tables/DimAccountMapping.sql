CREATE TABLE [dbo].[DimAccountMapping] (
    [SourceSystem]      VARCHAR (8000) NULL,
    [SourceAccountCode] VARCHAR (8000) NULL,
    [TargetAccountID]   VARCHAR (8000) NULL,
    [EffectiveFrom]     DATE           NULL,
    [EffectiveTo]       DATE           NULL,
    [BatchID]           VARCHAR (8000) NULL,
    [SourceFileName]    VARCHAR (8000) NULL,
    [SourceFileHash]    VARCHAR (8000) NULL,
    [IngestedAt]        DATETIME2 (6)  NULL,
    [SourceRowHash]     VARCHAR (8000) NULL
);


GO