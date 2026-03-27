-- ============================================================================
-- COLUMN AUDIT: Run these DESC commands to identify which columns exist where
-- ============================================================================
-- Run Section A on LEGACY (ALS), Section B on MODERN (ENTITYDEV)
-- Report back any "invalid identifier" errors
-- ============================================================================


-- ============================================================================
-- SECTION A: LEGACY DB (ALS schema) — run all 4
-- ============================================================================
-- A1:
DESC ALS.ENT;
-- A2:
DESC ALS.ENTMOD;
-- A3:
DESC ALS.TRANTRAIL;
-- A4: Check if mft_ind_vals exists in legacy
SELECT object_name, object_type
FROM all_objects
WHERE owner = 'ALS'
  AND object_name = 'MFT_IND_VALS';


-- ============================================================================
-- SECTION B: MODERN DB (ENTITYDEV schema) — run all 4
-- ============================================================================
-- B1:
DESC ENT;
-- B2:
DESC ENTMOD;
-- B3:
DESC TRANTRAIL;
-- B4: Check mft_ind_vals
SELECT object_name, object_type
FROM all_objects
WHERE object_name = 'MFT_IND_VALS';


-- ============================================================================
-- SECTION C: TARGETED COLUMN CHECKS
-- ============================================================================
-- Run these on each DB to quickly find which columns are missing.
-- Any that return 0 = column does NOT exist on that table.
-- ============================================================================

-- C1: Run on LEGACY (ALS) — columns likely to be missing in legacy:
SELECT 'AGI_TPI_IND' as COL,
       COUNT(*) as EXISTS_ON_ENT
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'AGI_TPI_IND'
UNION ALL
SELECT 'AGI_TPI_TX_YR',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'AGI_TPI_TX_YR'
UNION ALL
SELECT 'AGI_AMT',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'AGI_AMT'
UNION ALL
SELECT 'TPI_AMT',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'TPI_AMT'
UNION ALL
SELECT 'CC',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'CC'
UNION ALL
SELECT 'DT_DOD',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'DT_DOD'
UNION ALL
SELECT 'FEDCONIND',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'FEDCONIND'
UNION ALL
SELECT 'FEDEMPIND',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'FEDEMPIND'
UNION ALL
SELECT 'IRSEMPIND',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'IRSEMPIND'
UNION ALL
SELECT 'L903',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'L903'
UNION ALL
SELECT 'LLCIND',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'LLCIND'
UNION ALL
SELECT 'THEFTIND',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'THEFTIND'
UNION ALL
SELECT 'OICACCYR',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'OICACCYR'
UNION ALL
SELECT 'MODFATCAIND',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'MODFATCAIND'
UNION ALL
SELECT 'L725DT',
       COUNT(*)
FROM all_tab_columns
WHERE owner = 'ALS' AND table_name = 'ENT' AND column_name = 'L725DT';

-- C2: Run on MODERN (ENTITYDEV) — columns likely to be missing in modern:
SELECT 'BAL_941_14' as COL,
       COUNT(*) as ON_ENT
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'BAL_941_14'
UNION ALL
SELECT 'BAL_941_14',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENTMOD' AND column_name = 'BAL_941_14'
UNION ALL
SELECT 'CNT_941_14',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'CNT_941_14'
UNION ALL
SELECT 'CNT_941_14',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENTMOD' AND column_name = 'CNT_941_14'
UNION ALL
SELECT 'CNT_941',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'CNT_941'
UNION ALL
SELECT 'BAL_941',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'BAL_941'
UNION ALL
SELECT 'TDI_CNT_941',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'TDI_CNT_941'
UNION ALL
SELECT 'F1058DT1',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'F1058DT1'
UNION ALL
SELECT 'L3174DT1',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'L3174DT1'
UNION ALL
SELECT 'F1058DT2',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'F1058DT2'
UNION ALL
SELECT 'L3174DT2',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'L3174DT2'
UNION ALL
SELECT 'DT_OAMOD',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'DT_OAMOD'
UNION ALL
SELECT 'DT_POAMOD',
       COUNT(*)
FROM all_tab_columns
WHERE table_name = 'ENT' AND column_name = 'DT_POAMOD';
