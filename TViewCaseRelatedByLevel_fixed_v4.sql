-- ============================================================================
-- TViewCaseRelatedByLevel_fixed_v4.sql
-- Matches legacy TVIEW: ENT + TIMETIN only
--
-- ROOT CAUSE OF ROW MISMATCH:
--   Modernized query added TRANTRAIL join and TABLE(mft_ind_vals()) that
--   do NOT exist in legacy TVIEW. Both multiplied rows per case.
--   Legacy is purely ENT + TIMETIN.
--
-- FIX: Removed TRANTRAIL and mft_ind_vals entirely.
--   All their columns defaulted for Java DTO compatibility.
--
-- OPTIMIZATIONS:
--   1. TOUR/SEID CTEs deduplicated with MIN()/GROUP BY
--   2. Comma-style joins throughout
--   3. Explicit TO_DATE() on all date literals
--   4. PARALLEL hints on filtered_timetin
--
-- STILL PENDING (need function source from Brian):
--   - case_org(tt.roid) = 'CF'  <-- BIGGEST bottleneck
--   - GETSEGIND(tt.roid, tt.timesid)
-- ============================================================================

WITH
-- Pre-filter TIMETIN with case_org (still PL/SQL - biggest bottleneck)
filtered_timetin AS (
    SELECT /*+ PARALLEL(tt, 4) MATERIALIZE */
        tt.*
    FROM TIMETIN tt
    WHERE tt.RPTDT > TO_DATE('01/01/1900', 'mm/dd/yyyy')
      AND case_org(tt.roid) = 'CF'
),

-- TOUR lookup from entemp (deduplicated - one row per roid)
tour_cte AS (
    SELECT roid, MIN(tour) AS tour
    FROM entemp
    WHERE elevel > 0
      AND eactive IN ('A', 'Y')
    GROUP BY roid
),

-- SEID lookup from entemp (deduplicated - one row per roid)
seid_cte AS (
    SELECT roid, MIN(SEID) AS SEID
    FROM entemp
    WHERE eactive IN ('A', 'Y')
      AND elevel >= 0
    GROUP BY roid
),

-- Main query - ENT + TIMETIN only (matches legacy TVIEW exactly)
main_data AS (
    SELECT /*+ PARALLEL(a, 4) */
        tt.ROID,
        p.SEID,
        a.TIN,
        a.TINTT,
        a.TINFS,
        tt.TIMESID AS TINSID,
        SUBSTR(a.TP, 1, 35) AS TP,
        a.RISK AS C_RISK,
        tt.RISK AS H_RISK,
        GETSEGIND(tt.roid, tt.timesid) AS C_CASEIND,
        GETSEGIND(tt.roid, tt.timesid) AS H_CASEIND,
        NVL(tt.CONTCD, ' ') AS CONTACTCD,
        tt.EXTRDT,
        tt.RPTDT,
        a.casecode AS C_CASECODE,
        a.casecode AS CASECODE,
        tt.CODE AS H_CASECODE,
        a.subcode AS C_SUBCODE,
        a.subcode AS SUBCODE,
        tt.SUBCODE AS H_SUBCODE,
        ' ' AS TIMECODE,
        ' ' AS TIMEDESC,
        'T' AS TIMEDEF,
        a.GRADE AS C_GRADE,
        tt.GRADE AS H_GRADE,
        tt.HOURS,
        tt.BODCD,
        tour_sub.TOUR,
        tt.PRGNAME1,
        tt.PRGNAME2,
        a.TOTASSD,
        0 AS BAL_941_14,              -- no mft_ind_vals in legacy
        a.GRADE AS CASEGRADE,
        ' ' AS NAICSCD,               -- no TRANTRAIL in legacy
        a.CCNIPSELECTCD,
        0 AS CNT_941_14,              -- no mft_ind_vals in legacy
        0 AS CNT_941,                 -- no mft_ind_vals in legacy
        0 AS TDI_CNT_941,             -- no mft_ind_vals in legacy
        0 AS TDACNT,                   -- no TRANTRAIL in legacy
        0 AS TDICNT,                   -- no TRANTRAIL in legacy
        0 AS MODCNT,                   -- no TRANTRAIL in legacy
        0 AS STATIND,                  -- no TRANTRAIL in legacy
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS ASSNFLD,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS ASSNQUE,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS CLOSEDT,
        NVL(a.DT_DOD, TO_DATE('01/01/1900', 'mm/dd/yyyy')) AS DT_DOD,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS XXDT,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS INITDT,
        a.DT_OA,
        a.DT_POA,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS PICKDT,
        a.DVICTCD,
        '?' AS QPICKIND,
        0 AS FLDHRS,                   -- no TRANTRAIL in legacy
        0 AS EMPHRS,                   -- no TRANTRAIL in legacy
        0 AS HRS,                      -- no TRANTRAIL in legacy
        NVL(a.TOTHRS, 0) AS TOTHRS,
        0 AS IND_941,                  -- no mft_ind_vals in legacy
        'No' AS FORMATTED_IND_941,     -- no mft_ind_vals in legacy
        a.HINFIND,
        a.AGEIND,
        TO_NUMBER(a.PDTIND) AS CAUIND,
        0 AS PYRENT,                   -- no TRANTRAIL in legacy
        a.PYRIND,
        a.FATCAIND,
        a.FEDCONIND,
        a.FEDEMPIND,
        a.IRSEMPIND,
        a.L903,
        TO_NUMBER(NVL(a.LFIIND, 0)) AS LFIIND,
        a.LLCIND,
        a.RPTIND,
        a.THEFTIND,
        a.INSPCIND,
        a.OICACCYR,
        LPAD(NVL(a.RISK, 399) || NVL(a.ARISK, 'e'), 4, ' ') AS ARANK,
        0 AS TOT_IRP_INC,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS EMPTOUCH,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS LSTTOUCH,
        NVL(a.TOTTOUCH, 0) AS TOTTOUCH,
        NVL(a.STREET2, ' ') AS STREET2,
        0 AS PROID,                    -- no TRANTRAIL in legacy
        0 AS TOT_INC_DELQ_YR,
        0 AS PRIOR_YR_RET_AGI_AMT,
        0 AS TXPER_TXPYR_AMT,
        0 AS PRIOR_ASSGMNT_NUM,
        a.AGI_AMT,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS PRIOR_ASSGMNT_ACT_DT,
        0 AS BAL_941,                 -- no mft_ind_vals in legacy
        ' ' AS SELCODE,
        ' ' AS STATUS,
        '?' AS CASEIND,
        -1 AS DISPCD,
        ' ' AS CC,
        a.ASSNCFF,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS ASSNRO,
        a.LDIND,
        a.TPCTRL,
        NVL(a.RISK, 399) AS RISK,
        NVL(a.CITY, ' ') AS CITY,
        a.STATE,
        SUBSTR(NVL(a.TP2, ' '), 1, 35) AS TP2,
        SUBSTR(NVL(a.STREET, ' '), 1, 35) AS STREET,
        0 AS ZIPCDE

    -- ================================================================
    -- ENT + TIMETIN only (matches legacy TVIEW exactly)
    -- ================================================================
    FROM ENT a,
         filtered_timetin tt,
         seid_cte p,
         tour_cte tour_sub

    WHERE a.TINSID = tt.TIMESID
      AND tt.roid = p.roid(+)
      AND tt.roid = tour_sub.roid(+)

      -- Level-based filtering
      AND trunc(tt.roid/power(10, 8 - :elevel)) = :levelValue
      AND sysdate - tt.rptdt <= :daysUpperLimit
)

SELECT
    ROID,
    SEID,
    TIN,
    TINTT,
    TINFS,
    TINSID,
    TP,
    C_RISK,
    H_RISK,
    C_CASEIND,
    H_CASEIND,
    CONTACTCD,
    EXTRDT,
    RPTDT,
    C_CASECODE,
    CASECODE,
    H_CASECODE,
    C_SUBCODE,
    SUBCODE,
    H_SUBCODE,
    TIMECODE,
    TIMEDESC,
    TIMEDEF,
    C_GRADE,
    H_GRADE,
    HOURS,
    BODCD,
    TOUR,
    PRGNAME1,
    PRGNAME2,
    TOTASSD,
    BAL_941_14,
    CASEGRADE,
    NAICSCD,
    CCNIPSELECTCD,
    CNT_941_14,
    CNT_941,
    TDI_CNT_941,
    TDACNT,
    TDICNT,
    MODCNT,
    STATIND,
    ASSNFLD,
    ASSNQUE,
    CLOSEDT,
    DT_DOD,
    XXDT,
    INITDT,
    DT_OA,
    DT_POA,
    PICKDT,
    DVICTCD,
    QPICKIND,
    FLDHRS,
    EMPHRS,
    HRS,
    TOTHRS,
    IND_941,
    FORMATTED_IND_941,
    HINFIND,
    AGEIND,
    CAUIND,
    PYRENT,
    PYRIND,
    FATCAIND,
    FEDCONIND,
    FEDEMPIND,
    IRSEMPIND,
    L903,
    LFIIND,
    LLCIND,
    RPTIND,
    THEFTIND,
    INSPCIND,
    OICACCYR,
    ARANK,
    TOT_IRP_INC,
    EMPTOUCH,
    LSTTOUCH,
    TOTTOUCH,
    STREET2,
    PROID,
    TOT_INC_DELQ_YR,
    PRIOR_YR_RET_AGI_AMT,
    TXPER_TXPYR_AMT,
    PRIOR_ASSGMNT_NUM,
    AGI_AMT,
    PRIOR_ASSGMNT_ACT_DT,
    BAL_941,
    SELCODE,
    STATUS,
    CASEIND,
    DISPCD,
    CC,
    ASSNCFF,
    ASSNRO,
    LDIND,
    TPCTRL,
    RISK,
    CITY,
    STATE,
    TP2,
    STREET,
    ZIPCDE
FROM main_data
