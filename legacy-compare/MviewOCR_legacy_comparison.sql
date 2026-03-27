-- ============================================================================
-- MODULE VIEW DATA COMPARISON: LEGACY vs MODERN
-- ============================================================================
-- Run Query 1 on LEGACY DB (ALS schema)
-- Run Query 2 on MODERN DB (ENTITYDEV/DIAL schema)
-- Run Query 3 on EITHER DB after exporting both result sets
--
-- Both queries use the same column list and sort order for direct comparison.
-- ============================================================================


-- ============================================================================
-- QUERY 1: LEGACY DATABASE (run on ALS schema)
-- ============================================================================
-- Uses ALS schema prefixes, myorg = 'CF' (works in legacy, not in Java pool),
-- and the legacy's native SWITCHROID + DECODE trail matching.
-- ============================================================================
SELECT
    TIN,
    TINTT,
    TINFS,
    c.TINSID,
    BODCD,
    DECODE (c.STATUS, 'O', ALS.STATIND (a.TINSID), 0) as STATIND,
    TOTASSD,
    a.GRADE as CASEGRADE,
    c.STATUS,
    SEGIND as CASEIND,
    DECODE (a.ASSNCFF, to_date('01/01/1900','mm/dd/yyyy'), b.ASSNRO, a.ASSNCFF) as ASSNCFF,
    DVICTCD,
    EMPHRS,
    TOTHRS,
    HRS,
    TO_NUMBER (PDTIND) as CAUIND,
    LDIND,
    a.PYRIND,
    RPTIND,
    TPCTRL,
    RWMS,
    NVL (a.RISK, 399) as RISK,
    LSTTOUCH,
    NVL (a.ZIPCDE, 0) as ZIPCDE,
    SUBSTR (TP, 1, 35) as TP,
    SUBSTR (TP2, 1, 35) as TP2,
    SUBSTR (STREET, 1, 35) as STREET,
    STREET2,
    CITY,
    STATE,
    PREDCD,
    PRED_UPDT_CYC,
    EMPTOUCH,
    TO_NUMBER (b.PYRIND) as M_PYRIND,
    TOTTOUCH,
    c.CLOSEDT,
    MFT,
    PERIOD,
    TYPE,
    CYCLE,
    BALANCE,
    NVL (RTNDT, to_date('01/01/1900','mm/dd/yyyy')) as RTNDT,
    TO_NUMBER (NVL (b.LFIIND, 0)) as M_LFIIND,
    TO_NUMBER (NVL (a.LFIIND, 0)) as LFIIND,
    DECODE (NVL (b.AGEIND, ' '), ' ', 'C', '0', 'C', b.AGEIND) as AGEIND,
    DECODE (tinfs, 2, TO_NUMBER (CCNIPSELECTCD), TO_NUMBER (SELCODE)) as SELCODE,
    (CASE WHEN TYPE IN ('O', 'N') THEN NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy'))
          ELSE CSED END) as CSED,
    CSEDIND,
    LRA,
    (CASE WHEN TYPE IN ('O', 'N') THEN NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy'))
          ELSE ASED END) as ASED,
    ASEDIND,
    b.ROID as M_ROID,
    PROID,
    c.ROID,
    b.STATUS as M_STATUS,
    CASECODE,
    SUBCODE,
    CIVPCD,
    b.ASSNRO as M_ASSNRO,
    c.ASSNRO,
    DUEDATE,
    CREATEDT,
    NVL (FTLCD, '0') as FTLCD,
    NVL (FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
    DECODE (b.status, 'C', CLSDT, NULL) as CLSDT,
    DECODE (b.status, 'C', b.DISPCODE, NULL) as DISPCD,
    b.NAICSCD as M_NAICSCD,
    c.NAICSCD,
    NVL (POAIND, ' ') as POAIND,
    NVL (CYCAGE2A, 0) as CYCAGE2A,
    NVL (CYCAGE2I, 0) as CYCAGE2I,
    NVL (CYCMOD2I, 0) as CYCMOD2I,
    b.INSPCIND,
    b.ERRFDIND,
    b.TSACTCD,
    SPECPRJCD,
    NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy')) as OICSED,
    NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy')) as OIASED,
    FMSLVCD,
    b.NAICSVLD,
    b.NAICSYR,
    IAFTPIND,
    NVL (TDAcnt, 0) as TDACNT,
    NVL (TDIcnt, 0) as TDICNT,
    NVL ((TDAcnt + TDIcnt), 0) as MODCNT,
    NVL (OIcnt, 0) as OICNT,
    NVL (FTDcnt, 0) as FTDCNT,
    NVL (OICcnt, 0) as OICCNT,
    NVL (nIDRScnt, 0) as NIDRSCNT,
    INITDT,
    XXDT,
    ASSNGRP,
    b.EXTRDT,
    BODCLCD,
    ICSCC,
    TC,
    NVL (DT_OAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_OA,
    NVL (DT_POAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_POA,
    PRGNAME1,
    PRGNAME2,
    ASSNFLD,
    FLDHRS,
    HINFIND,
    CNT_941,
    BAL_941,
    CNT_941_14,
    BAL_941_14,
    NVL (L725DT, to_date('01/01/1900','mm/dd/yyyy')) as L725DT,
    MODFATCAIND as FATCAIND,
    AGI_AMT,
    TPI_AMT,
    AGI_TPI_TX_YR,
    AGI_TPI_IND,
    CC,
    NVL (DT_DOD, to_date('01/01/1900', 'mm/dd/yyyy')) as DT_DOD,
    FEDCONIND,
    FEDEMPIND,
    IRSEMPIND,
    L903,
    LLCIND,
    THEFTIND,
    OICACCYR,
    DECODE(TYPE,
      'A', 'TDA_A ', 'B', 'TDA_B ', 'C', 'TDA_C ', 'D', 'TDA_D ', 'E', 'TDA_E ',
      'F', 'TDI_F ', 'G', 'TDI_G ', 'I', 'TDI_I ',
      'N', 'OI-in ', 'O', 'OI-out', 'R', 'CIP   ', 'T', 'FTD   ',
      'X', 'TCMP  ', 'Y', 'OIC   ', '', '') as MODULETYPE
FROM ALS.ENT a,
     ALS.TRANTRAIL c,
     ALS.ENTMOD b
WHERE a.TINSID = c.TINSID
  AND a.TINSID = b.EMODSID
  AND to_number(ALS.SWITCHROID(b.ROID)) = to_number(ALS.SWITCHROID(c.ROID))
  AND DECODE(c.STATUS,
        'O', 1,
        'C', (c.extrdt - ALS.max_extrdt(c.tinsid, c.roid)),
        'c', (c.extrdt - ALS.max_extrdt(c.tinsid, c.roid)),
        'R', (c.extrdt - ALS.max_extrdt(c.tinsid, c.roid)),
        -1) >= 0
  AND ORG = 'CF'
  AND myorg = 'CF'
  AND ALS.case_org(b.roid) = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND b.STATUS NOT IN ('E', 'Q')
  AND trunc(b.roid/power(10, 8-6)) = 251435
ORDER BY TIN, MFT, PERIOD, TYPE, b.ROID;


-- ============================================================================
-- QUERY 2: MODERN DATABASE (run on ENTITYDEV schema)
-- ============================================================================
-- Same column list and sort order as Query 1 for direct comparison.
-- Uses the corrected filters (b.STATUS = 'O', trail ranking).
-- NOTE: Removed PL/SQL functions (STATIND, etc.) that differ between
-- legacy and modern — those are Phase 2 comparisons.
-- ============================================================================
SELECT
    TIN,
    TINTT,
    TINFS,
    c.TINSID,
    BODCD,
    DECODE (c.STATUS, 'O', STATIND (a.TINSID), 0) as STATIND,
    TOTASSD,
    a.GRADE as CASEGRADE,
    c.STATUS,
    SEGIND as CASEIND,
    DECODE (a.ASSNCFF, to_date('01/01/1900','mm/dd/yyyy'), b.ASSNRO, a.ASSNCFF) as ASSNCFF,
    DVICTCD,
    EMPHRS,
    TOTHRS,
    HRS,
    TO_NUMBER (PDTIND) as CAUIND,
    LDIND,
    a.PYRIND,
    RPTIND,
    TPCTRL,
    RWMS,
    NVL (a.RISK, 399) as RISK,
    LSTTOUCH,
    NVL (a.ZIPCDE, 0) as ZIPCDE,
    SUBSTR (TP, 1, 35) as TP,
    SUBSTR (TP2, 1, 35) as TP2,
    SUBSTR (STREET, 1, 35) as STREET,
    STREET2,
    CITY,
    STATE,
    PREDCD,
    PRED_UPDT_CYC,
    EMPTOUCH,
    TO_NUMBER (b.PYRIND) as M_PYRIND,
    TOTTOUCH,
    c.CLOSEDT,
    MFT,
    PERIOD,
    TYPE,
    CYCLE,
    BALANCE,
    NVL (RTNDT, to_date('01/01/1900','mm/dd/yyyy')) as RTNDT,
    TO_NUMBER (NVL (b.LFIIND, 0)) as M_LFIIND,
    TO_NUMBER (NVL (a.LFIIND, 0)) as LFIIND,
    DECODE (NVL (b.AGEIND, ' '), ' ', 'C', '0', 'C', b.AGEIND) as AGEIND,
    DECODE (tinfs, 2, TO_NUMBER (CCNIPSELECTCD), TO_NUMBER (SELCODE)) as SELCODE,
    (CASE WHEN TYPE IN ('O', 'N') THEN NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy'))
          ELSE CSED END) as CSED,
    CSEDIND,
    LRA,
    (CASE WHEN TYPE IN ('O', 'N') THEN NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy'))
          ELSE ASED END) as ASED,
    ASEDIND,
    b.ROID as M_ROID,
    PROID,
    c.ROID,
    b.STATUS as M_STATUS,
    CASECODE,
    SUBCODE,
    CIVPCD,
    b.ASSNRO as M_ASSNRO,
    c.ASSNRO,
    DUEDATE,
    CREATEDT,
    NVL (FTLCD, '0') as FTLCD,
    NVL (FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
    DECODE (b.status, 'C', CLSDT, NULL) as CLSDT,
    DECODE (b.status, 'C', b.DISPCODE, NULL) as DISPCD,
    b.NAICSCD as M_NAICSCD,
    c.NAICSCD,
    NVL (POAIND, ' ') as POAIND,
    NVL (CYCAGE2A, 0) as CYCAGE2A,
    NVL (CYCAGE2I, 0) as CYCAGE2I,
    NVL (CYCMOD2I, 0) as CYCMOD2I,
    b.INSPCIND,
    b.ERRFDIND,
    b.TSACTCD,
    SPECPRJCD,
    NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy')) as OICSED,
    NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy')) as OIASED,
    FMSLVCD,
    b.NAICSVLD,
    b.NAICSYR,
    IAFTPIND,
    NVL (TDAcnt, 0) as TDACNT,
    NVL (TDIcnt, 0) as TDICNT,
    NVL ((TDAcnt + TDIcnt), 0) as MODCNT,
    NVL (OIcnt, 0) as OICNT,
    NVL (FTDcnt, 0) as FTDCNT,
    NVL (OICcnt, 0) as OICCNT,
    NVL (nIDRScnt, 0) as NIDRSCNT,
    INITDT,
    XXDT,
    ASSNGRP,
    b.EXTRDT,
    BODCLCD,
    ICSCC,
    TC,
    NVL (DT_OAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_OA,
    NVL (DT_POAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_POA,
    PRGNAME1,
    PRGNAME2,
    ASSNFLD,
    FLDHRS,
    HINFIND,
    CNT_941,
    BAL_941,
    CNT_941_14,
    BAL_941_14,
    NVL (L725DT, to_date('01/01/1900','mm/dd/yyyy')) as L725DT,
    MODFATCAIND as FATCAIND,
    AGI_AMT,
    TPI_AMT,
    AGI_TPI_TX_YR,
    AGI_TPI_IND,
    CC,
    NVL (DT_DOD, to_date('01/01/1900', 'mm/dd/yyyy')) as DT_DOD,
    FEDCONIND,
    FEDEMPIND,
    IRSEMPIND,
    L903,
    LLCIND,
    THEFTIND,
    OICACCYR,
    DECODE(TYPE,
      'A', 'TDA_A ', 'B', 'TDA_B ', 'C', 'TDA_C ', 'D', 'TDA_D ', 'E', 'TDA_E ',
      'F', 'TDI_F ', 'G', 'TDI_G ', 'I', 'TDI_I ',
      'N', 'OI-in ', 'O', 'OI-out', 'R', 'CIP   ', 'T', 'FTD   ',
      'X', 'TCMP  ', 'Y', 'OIC   ', '', '') as MODULETYPE
FROM ENT a
  INNER JOIN (
    SELECT c2.*,
           ROW_NUMBER() OVER (PARTITION BY c2.TINSID ORDER BY c2.EXTRDT DESC, c2.ROWID DESC) as trail_rn
    FROM TRANTRAIL c2
    WHERE c2.STATUS = 'O'
      AND c2.ORG = 'CF'
  ) c ON a.TINSID = c.TINSID AND c.trail_rn = 1
  INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE b.STATUS = 'O'
  AND case_org(b.roid) = 'CF'
  AND trunc(b.roid/power(10, 8-6)) = 251435
ORDER BY TIN, MFT, PERIOD, TYPE, b.ROID;


-- ============================================================================
-- QUERY 3: QUICK COUNT COMPARISON (run on each DB)
-- ============================================================================
-- Run on LEGACY first, then MODERN. Both should return 1773.
-- ============================================================================

-- LEGACY (run on ALS):
SELECT 'LEGACY' as SOURCE, COUNT(*) as CNT
FROM ALS.ENT a, ALS.TRANTRAIL c, ALS.ENTMOD b
WHERE a.TINSID = c.TINSID
  AND a.TINSID = b.EMODSID
  AND to_number(ALS.SWITCHROID(b.ROID)) = to_number(ALS.SWITCHROID(c.ROID))
  AND DECODE(c.STATUS, 'O', 1,
        'C', (c.extrdt - ALS.max_extrdt(c.tinsid, c.roid)),
        'c', (c.extrdt - ALS.max_extrdt(c.tinsid, c.roid)),
        'R', (c.extrdt - ALS.max_extrdt(c.tinsid, c.roid)),
        -1) >= 0
  AND ORG = 'CF'
  AND myorg = 'CF'
  AND ALS.case_org(b.roid) = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND b.STATUS NOT IN ('E', 'Q')
  AND trunc(b.roid/power(10, 8-6)) = 251435;

-- MODERN (run on ENTITYDEV):
SELECT 'MODERN' as SOURCE, COUNT(*) as CNT
FROM ENT a
  INNER JOIN (
    SELECT c2.*,
           ROW_NUMBER() OVER (PARTITION BY c2.TINSID ORDER BY c2.EXTRDT DESC, c2.ROWID DESC) as trail_rn
    FROM TRANTRAIL c2
    WHERE c2.STATUS = 'O' AND c2.ORG = 'CF'
  ) c ON a.TINSID = c.TINSID AND c.trail_rn = 1
  INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE b.STATUS = 'O'
  AND case_org(b.roid) = 'CF'
  AND trunc(b.roid/power(10, 8-6)) = 251435;


-- ============================================================================
-- QUERY 4: FIND MISMATCHED RECORDS (run after exporting both to temp tables)
-- ============================================================================
-- Option A: If you can create temp tables on one DB, load both result sets
-- and use MINUS to find differences.
--
-- Option B: Export both to CSV from Toad, use this KEY-based comparison.
-- The key is TIN + MFT + PERIOD + TYPE + M_ROID (unique per module).
-- ============================================================================

-- Records in LEGACY but NOT in MODERN (missing from modern):
-- Run on a DB where you've loaded both as temp tables
/*
SELECT 'IN_LEGACY_ONLY' as DIFF_TYPE, l.*
FROM legacy_results l
WHERE NOT EXISTS (
  SELECT 1 FROM modern_results m
  WHERE m.TIN = l.TIN
    AND m.MFT = l.MFT
    AND m.PERIOD = l.PERIOD
    AND m.TYPE = l.TYPE
    AND m.M_ROID = l.M_ROID
);

-- Records in MODERN but NOT in LEGACY (extra in modern):
SELECT 'IN_MODERN_ONLY' as DIFF_TYPE, m.*
FROM modern_results m
WHERE NOT EXISTS (
  SELECT 1 FROM legacy_results l
  WHERE l.TIN = m.TIN
    AND l.MFT = m.MFT
    AND l.PERIOD = m.PERIOD
    AND l.TYPE = m.TYPE
    AND l.M_ROID = m.M_ROID
);
*/

-- ============================================================================
-- QUERY 5: TOAD CSV EXPORT COMPARISON (simplest approach)
-- ============================================================================
-- 1. Run Query 1 on legacy DB in Toad → right-click grid → Export → CSV
--    Save as: legacy_mview_251435.csv
-- 2. Run Query 2 on modern DB in Toad → right-click grid → Export → CSV
--    Save as: modern_mview_251435.csv
-- 3. Sort both files by TIN, MFT, PERIOD, TYPE (already sorted by ORDER BY)
-- 4. Use a diff tool (Beyond Compare, WinMerge, or Excel VLOOKUP) on both CSVs
--
-- Quick Excel comparison:
--   - Open both CSVs in Excel
--   - In legacy sheet, create a key column: =A2&"-"&AF2&"-"&AG2&"-"&AH2
--     (TIN & MFT & PERIOD & TYPE)
--   - In modern sheet, create same key column
--   - Use VLOOKUP or COUNTIF to find keys present in one but not the other
-- ============================================================================
