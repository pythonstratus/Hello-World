-- ============================================================================
-- TViewCaseRelatedByLevel_fixed.sql
-- Fixed version - matches legacy TVIEW which is ENT + TIMETIN only
--
-- ROOT CAUSE OF 2000 vs 379 ROW MISMATCH:
--   The modernized query added a TRANTRAIL join that does NOT exist in
--   legacy TVIEW. Each TIMETIN row was multiplied by however many
--   TRANTRAIL segments exist per case (A, C, I, etc.), inflating row count.
--
-- FIX: Removed TRANTRAIL join entirely. All TRANTRAIL-sourced columns
--   are defaulted for Java service compatibility.
--
-- ADDITIONAL OPTIMIZATIONS:
--   1. Comma-style joins (TABLE() alias scoping fix)
--   2. Explicit TO_DATE() on all date literals
--   3. PARALLEL hints
--   4. TOUR/SEID CTEs deduplicated with MIN()/GROUP BY
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

-- TOUR lookup from entemp (deduplicated)
tour_cte AS (
    SELECT roid, MIN(tour) AS tour
    FROM entemp
    WHERE elevel > 0
      AND eactive IN ('A', 'Y')
    GROUP BY roid
),

-- SEID lookup from entemp (deduplicated)
seid_cte AS (
    SELECT roid, MIN(SEID) AS SEID
    FROM entemp
    WHERE eactive IN ('A', 'Y')
      AND elevel >= 0
    GROUP BY roid
),

-- Main query - ENT + TIMETIN only (matches legacy TVIEW)
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
        -- H_CASEIND: legacy gets from GETSEGIND, no TRANTRAIL
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

        -- Columns from ENT (not TRANTRAIL)
        a.TOTASSD,
        c.BAL_941_14,
        a.GRADE AS CASEGRADE,
        ' ' AS NAICSCD,               -- was b.NAICSCD
        a.CCNIPSELECTCD,
        c.CNT_941_14,
        c.CNT_941,
        c.TDI_CNT_941,
        0 AS TDACNT,                   -- was b.TDAcnt
        0 AS TDICNT,                   -- was b.TDIcnt
        0 AS MODCNT,                   -- was b.TDAcnt + b.TDIcnt
        0 AS STATIND,                  -- was STATIND(a.TINSID) - no TRANTRAIL status check needed
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS ASSNFLD,  -- was b.ASSNFLD
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS ASSNQUE,  -- was GETASSNQUE(...)
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS CLOSEDT,  -- was b.CLOSEDT
        NVL(a.DT_DOD, TO_DATE('01/01/1900', 'mm/dd/yyyy')) AS DT_DOD,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS XXDT,     -- was b.XXDT
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS INITDT,   -- was b.INITDT
        a.DT_OA,
        a.DT_POA,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS PICKDT,   -- was ASSNPICKDT(...)
        a.DVICTCD,
        '?' AS QPICKIND,              -- was complex DECODE with ASSNQPICK/INTLQPICK
        0 AS FLDHRS,                   -- was b.FLDHRS
        0 AS EMPHRS,                   -- was b.EMPHRS
        0 AS HRS,                      -- was b.HRS
        NVL(a.TOTHRS, 0) AS TOTHRS,   -- simplified, no b.EMPHRS comparison
        c.IND_941 AS IND_941,
        CASE WHEN c.ind_941 = 0 THEN 'No' ELSE 'Yes' END AS FORMATTED_IND_941,
        a.HINFIND,
        a.AGEIND,                      -- simplified, no SEGIND-based DECODE
        TO_NUMBER(a.PDTIND) AS CAUIND,
        0 AS PYRENT,                   -- was complex EXISTS with TRANTRAIL
        a.PYRIND,                      -- simplified, no SEGIND-based DECODE
        a.FATCAIND,
        a.FEDCONIND,
        a.FEDEMPIND,
        a.IRSEMPIND,
        a.L903,
        TO_NUMBER(NVL(a.LFIIND, 0)) AS LFIIND,
        a.LLCIND,
        a.RPTIND,                      -- simplified, no SEGIND-based DECODE
        a.THEFTIND,
        a.INSPCIND,
        a.OICACCYR,
        LPAD(NVL(a.RISK, 399) || NVL(a.ARISK, 'e'), 4, ' ') AS ARANK,
        0 AS TOT_IRP_INC,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS EMPTOUCH,   -- was b.EMPTOUCH
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS LSTTOUCH,   -- was b.LSTTOUCH
        NVL(a.TOTTOUCH, 0) AS TOTTOUCH,  -- simplified, no b.EMPTOUCH comparison
        NVL(a.STREET2, ' ') AS STREET2,
        0 AS PROID,                    -- was b.PROID
        0 AS TOT_INC_DELQ_YR,
        0 AS PRIOR_YR_RET_AGI_AMT,
        0 AS TXPER_TXPYR_AMT,
        0 AS PRIOR_ASSGMNT_NUM,
        a.AGI_AMT,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS PRIOR_ASSGMNT_ACT_DT,
        c.BAL_941,
        ' ' AS SELCODE,               -- was from entmod
        ' ' AS STATUS,                 -- was REPLACE(b.STATUS, 'X', 'C')
        '?' AS CASEIND,               -- was NVL(b.SEGIND, '?')
        -1 AS DISPCD,                  -- was NVL(b.DISPCD, -1)
        ' ' AS CC,                     -- was b.CC
        a.ASSNCFF,                     -- from ENT directly
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS ASSNRO,    -- was b.ASSNRO
        a.LDIND,                       -- simplified, no SEGIND-based DECODE
        a.TPCTRL,
        NVL(a.RISK, 399) AS RISK,
        NVL(a.CITY, ' ') AS CITY,
        a.STATE,
        SUBSTR(NVL(a.TP2, ' '), 1, 35) AS TP2,
        SUBSTR(NVL(a.STREET, ' '), 1, 35) AS STREET,
        0 AS ZIPCDE                    -- was from b.ZIPCDE

    -- ================================================================
    -- ENT + TIMETIN only (matches legacy - NO TRANTRAIL)
    -- Comma-style joins for TABLE() function compatibility
    -- ================================================================
    FROM ENT a,
         filtered_timetin tt,
         TABLE(mft_ind_vals(a.tinsid, a.tinfs)) c,
         seid_cte p,
         tour_cte tour_sub

    WHERE a.TINSID = tt.TIMESID

      -- Outer joins using (+) syntax
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
