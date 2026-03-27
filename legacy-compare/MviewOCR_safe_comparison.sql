-- ============================================================================
-- MODULE VIEW SAFE DATA COMPARISON: LEGACY vs MODERN
-- ============================================================================
-- These queries use ONLY core columns that should exist on both schemas.
-- Suspect columns (potentially schema-specific) are excluded.
--
-- EXCLUDED COLUMNS (may not exist on both DBs):
--   Legacy may be missing:  AGI_AMT, TPI_AMT, AGI_TPI_TX_YR, AGI_TPI_IND,
--                           CC, DT_DOD, FEDCONIND, FEDEMPIND, IRSEMPIND,
--                           L903, LLCIND, THEFTIND, OICACCYR, MODFATCAIND,
--                           L725DT, F1058DT2, L3174DT2, DT_OAMOD, DT_POAMOD
--   Modern may be missing:  BAL_941_14, CNT_941_14, CNT_941, BAL_941,
--                           TDI_CNT_941, F1058DT1, L3174DT1
--   Either may be missing:  IND_941 (from mft_ind_vals pipelined function)
--
-- Once you run the column audit (MviewOCR_column_audit.sql), we can add
-- confirmed columns back in.
-- ============================================================================


-- ============================================================================
-- QUERY 1: LEGACY DATABASE (ALS schema) — SAFE VERSION
-- ============================================================================
SELECT
    TIN,
    TINTT,
    TINFS,
    c.TINSID,
    BODCD,
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
    PRGNAME1,
    PRGNAME2,
    ASSNFLD,
    FLDHRS,
    HINFIND,
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
-- QUERY 2: MODERN DATABASE (ENTITYDEV) — SAFE VERSION
-- ============================================================================
SELECT
    TIN,
    TINTT,
    TINFS,
    c.TINSID,
    BODCD,
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
    PRGNAME1,
    PRGNAME2,
    ASSNFLD,
    FLDHRS,
    HINFIND,
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
-- QUERY 3: QUICK COUNT (run on each DB)
-- ============================================================================

-- LEGACY:
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

-- MODERN:
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
-- COLUMNS REMOVED (add back after confirming with audit):
-- ============================================================================
/*
  REMOVED FROM LEGACY QUERY (likely don't exist in ALS schema):
    AGI_AMT              -- newer IRS field
    TPI_AMT              -- newer IRS field
    AGI_TPI_TX_YR        -- newer IRS field
    AGI_TPI_IND          -- newer IRS field  ← YOUR ERROR
    CC                   -- newer field
    DT_DOD               -- newer field
    FEDCONIND            -- newer field
    FEDEMPIND            -- newer field
    IRSEMPIND            -- newer field
    L903                 -- newer field
    LLCIND               -- newer field
    THEFTIND             -- newer field
    OICACCYR             -- newer field
    MODFATCAIND          -- newer field (FATCAIND alias)
    L725DT               -- newer field
    F1058DT2             -- newer field
    L3174DT2             -- newer field
    DT_OAMOD             -- newer field
    DT_POAMOD            -- newer field

  REMOVED FROM MODERN QUERY (likely don't exist or are on different table):
    BAL_941_14           -- may be legacy-only or mft_ind_vals  ← YOUR ERROR
    CNT_941_14           -- may be legacy-only or mft_ind_vals
    CNT_941              -- may be legacy-only or mft_ind_vals
    BAL_941              -- may be legacy-only or mft_ind_vals
    TDI_CNT_941          -- may be legacy-only or mft_ind_vals
    F1058DT1             -- may be legacy-only
    L3174DT1             -- may be legacy-only
    IND_941              -- from mft_ind_vals pipelined function (d alias)

  REMOVED FROM BOTH (PL/SQL function output, may differ between schemas):
    STATIND              -- STATIND(a.TINSID), PL/SQL — compare separately
    CAFCD                -- NVL(TO_CHAR(CAFCD)) — unqualified, table unknown

  Once you run the column audit, tell me which columns exist on which tables
  and I'll add them back into the correct queries.
*/
