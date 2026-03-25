-- ============================================================================
-- AViewCFByLevel_CTE.sql
-- Optimized CTE version — fixes duplicates and count mismatches
-- ============================================================================
-- ROOT CAUSES FIXED:
--   1. TRANTRAIL join had no segment filter → row multiplication
--   2. ROW_NUMBER PARTITION BY = ORDER BY → non-deterministic dedup
--   3. rownum <= 2000 inside inner query → rows lost after dedup
--   4. Mixed ANSI/comma joins with TABLE() → alias scoping risk
--   5. STATUS subquery ROWNUM=1 without ORDER BY → non-deterministic
--   6. SELCODE subquery ROWNUM=1 without ORDER BY → non-deterministic
--   7. STATUS correlated subquery → replaced with CTE
--   8. ASSNRO correlated subquery → replaced with CTE
-- ============================================================================

-- -------------------------------------------------------
-- CTE 1: Latest EXTRDT per (TINSID, ROID, seg_flag)
-- Replaces the nested MAX(EXTRDT) correlated subquery
-- -------------------------------------------------------
WITH latest_extrdt AS (
    SELECT
        d.TINSID,
        d.ROID,
        DECODE(segind, 'A', 1, 'C', 1, 'I', 1, 0) AS seg_flag,
        NVL(MAX(d.EXTRDT), TO_DATE('01/01/1900', 'mm/dd/yyyy')) AS max_extrdt
    FROM TRANTRAIL d
    GROUP BY d.TINSID, d.ROID, DECODE(segind, 'A', 1, 'C', 1, 'I', 1, 0)
),

-- -------------------------------------------------------
-- CTE 2: Status from TRANTRAIL at latest EXTRDT
-- Replaces the STATUS correlated subquery (lines 46-63)
-- Uses ROW_NUMBER instead of ROWNUM=1 for determinism
-- -------------------------------------------------------
trail_status AS (
    SELECT
        c.TINSID,
        c.ROID,
        c.status,
        ROW_NUMBER() OVER (
            PARTITION BY c.TINSID, c.ROID
            ORDER BY c.EXTRDT DESC, c.ROWID
        ) AS rn
    FROM TRANTRAIL c
    INNER JOIN latest_extrdt le
        ON  c.TINSID = le.TINSID
        AND c.ROID   = le.ROID
        AND c.EXTRDT = le.max_extrdt
        AND DECODE(c.segind, 'A', 1, 'C', 1, 'I', 1, 0) = le.seg_flag
),

-- -------------------------------------------------------
-- CTE 3: MAX(ASSNRO) per (TINSID, ROID/AROID)
-- Replaces the ASSNRO correlated subquery (lines 70-75)
-- Note: original matches on (t.roid = a.aroid OR t.roid = a.roid)
--       so we pre-compute MAX per (TINSID, ROID) and join both ways
-- -------------------------------------------------------
trail_assnro AS (
    SELECT
        t.TINSID,
        t.ROID,
        MAX(t.ASSNRO) AS max_assnro
    FROM TRANTRAIL t
    WHERE DECODE(t.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
          DECODE(t.mft, 0, 0, 1)
    GROUP BY t.TINSID, t.ROID
),

-- -------------------------------------------------------
-- CTE 4: SELCODE from ENTMOD (first row per TINSID)
-- Replaces non-deterministic rownum=1 subquery (line 218)
-- Using MIN for determinism since no ORDER BY was specified
-- -------------------------------------------------------
entmod_selcode AS (
    SELECT
        emodsid,
        MIN(selcode) AS selcode
    FROM entmod
    GROUP BY emodsid
),

-- -------------------------------------------------------
-- CTE 5: Base activity data with all joins
-- All joins converted to comma-style for TABLE() compat
-- -------------------------------------------------------
base_data AS (
    SELECT
        /*+ index_join(a, entact_actdt_ix, entact_roid_ix) */
        a.ROID                                                    AS ROID,
        a.TIN                                                     AS TIN,
        a.TINTT                                                   AS TINTT,
        a.TINFS                                                   AS TINFS,
        a.ACTSID                                                  AS TINSID,
        DECODE(e.TPCTRL, 'E3 ', ' ', 'E7 ', ' ', e.tpctrl)      AS TPCTRL,
        SUBSTR(e.TP, 1, 35)                                       AS TP,
        SUBSTR(e.TP2, 1, 35)                                      AS TP2,
        SUBSTR(e.STREET, 1, 35)                                    AS STREET,
        e.CITY                                                     AS CITY,
        e.STATE                                                    AS STATE,
        (CASE
            WHEN e.zipcde < 100000
                THEN TO_NUMBER(TO_CHAR(e.ZIPCDE, '09999'))
            WHEN e.zipcde BETWEEN 99999 AND 999999999
                THEN NVL(TO_NUMBER(SUBSTR(TO_CHAR(e.ZIPCDE, '099999999'), -9, 5)), 0)
            ELSE
                NVL(TO_NUMBER(SUBSTR(TO_CHAR(e.ZIPCDE, '099999999999'), -12, 5)), 0)
        END)                                                       AS ZIPCDE,
        a.ACTDT                                                    AS ACTDT,
        a.CODE                                                     AS CASECODE,
        a.SUBCODE                                                  AS SUBCODE,
        NVL(ts.status, 'P')                                        AS STATUS,
        e.LDIND                                                    AS LDIND,
        e.RISK                                                     AS RISK,
        b.MFT                                                      AS MFT,
        a.PERIOD                                                   AS PERIOD,
        b.TYPCD                                                    AS M_TYPE,
        a.AROID                                                    AS AROID,
        -- ASSNRO: take the greater of the two lookups (by roid and by aroid)
        GREATEST(
            NVL(ta_roid.max_assnro, TO_DATE('01/01/1900','mm/dd/yyyy')),
            NVL(ta_aroid.max_assnro, TO_DATE('01/01/1900','mm/dd/yyyy'))
        )                                                          AS ASSNRO,
        e.ASSNCFF                                                  AS ASSNCFF,
        b.BODCD                                                    AS BODCD,
        b.AMOUNT                                                   AS AMOUNT,
        b.RTNSEC                                                   AS RTNSEC,
        b.DISPCODE                                                 AS DISPCD,
        b.GRPIND                                                   AS GRPIND,
        b.FORM809                                                  AS FORM809,
        b.RPTCD                                                    AS RPTCD,
        a.CC                                                       AS CC,
        b.TC                                                       AS TC,
        a.EXTRDT                                                   AS EXTRDT,
        a.TYPEID                                                   AS TYPEID,
        a.TSACTCD                                                  AS TSACTCD,
        b.TOTASSD                                                  AS TOTASSD,
        b.BAL_941_14                                               AS BAL_941_14,
        e.GRADE                                                    AS CASEGRADE,
        b.CASEIND                                                  AS CASEIND,
        NVL(b.NAICSCD, '     ')                                    AS NAICSCD,
        b.PRGNAME1                                                 AS PRGNAME1,
        b.PRGNAME2                                                 AS PRGNAME2,
        b.CCNIPSELECTCD                                            AS CCNIPSELECTCD,
        b.CNT_941_14                                               AS CNT_941_14,
        b.CNT_941                                                  AS CNT_941,
        b.TDI_CNT_941                                              AS TDI_CNT_941,
        NVL(b.TDAcnt, 0)                                           AS TDACNT,
        NVL(b.TDIcnt, 0)                                           AS TDICNT,
        NVL((b.TDAcnt + b.TDIcnt), 0)                              AS MODCNT,
        DECODE(b.STATUS, 'O', STATIND(a.ACTSID), 0)                AS STATIND,
        b.ASSNFLD                                                  AS ASSNFLD,
        (CASE
            WHEN b.segind IN ('A', 'C', 'I')
                THEN GETASSNQUE(e.TIN, e.TINTT, e.TINFS, e.ASSNCFF, b.ASSNRO)
            ELSE b.ASSNRO
        END)                                                       AS ASSNQUE,
        NVL(
            DECODE(b.status,
                'C', b.CLOSEDT,
                'X', b.CLOSEDT,
                TO_DATE('01/01/1900', 'mm/dd/yyyy')),
            TO_DATE('01/01/1900', 'mm/dd/yyyy'))                   AS CLOSEDT,
        NVL(b.DT_DOD, TO_DATE('01/01/1900', 'mm/dd/yyyy'))        AS DT_DOD,
        b.XXDT                                                     AS XXDT,
        b.INITDT                                                   AS INITDT,
        b.DT_OA                                                    AS DT_OA,
        b.DT_POA                                                   AS DT_POA,
        TRUNC(ASSNPICKDT(e.TIN, e.TINFS, e.TINTT, b.STATUS, b.PROID, a.ROID)) AS PICKDT,
        b.DVICTCD                                                  AS DVICTCD,
        DECODE(DECODE(b.ZIPCDE, 00000,
            ASSNQPICK(e.TIN, e.TINFS, e.TINTT, INTLQPICK(e.TIN, e.TINFS, e.TINTT, b.STATUS)),
            DECODE(e.CITY, 'APO', ASSNQPICK(e.TIN, e.TINFS, e.TINTT, INTLQPICK(e.TIN, e.TINFS, e.TINTT, b.STATUS)),
            'FPO',
            ASSNQPICK(e.TIN, e.TINFS, e.TINTT, INTLQPICK(e.TIN, e.TINFS, e.TINTT, b.STATUS)),
            ASSNQPICK(e.TIN, e.TINFS, e.TINTT, b.PROID))),
            '', 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, '?') AS QPICKIND,
        b.FLDHRS                                                   AS FLDHRS,
        NVL(b.EMPHRS, 0)                                           AS EMPHRS,
        b.HRS                                                      AS HRS,
        (CASE
            WHEN b.ORG = 'CP' THEN b.CCPHRS
            ELSE GREATEST(NVL(b.TOTHRS, 0), NVL(b.EMPHRS, 0))
        END)                                                       AS TOTHRS,
        c.IND_941                                                  AS IND_941,
        (CASE WHEN c.ind_941 = 0 THEN 'No' ELSE 'Yes' END)        AS FORMATTED_IND_941,
        b.HINFIND                                                  AS HINFIND,
        DECODE(b.segind, 'A', b.AGEIND, 'C', b.AGEIND, 'I', b.AGEIND, 'C') AS AGEIND,
        TO_NUMBER(b.PDTIND)                                        AS CAUIND,
        DECODE(
            b.STATUS,
            'O', (CASE
                    WHEN DECODE(b.SEGIND, 'C', 1, 'A', 1, 'I', 1, 0) = 1
                        AND DECODE(e.casecode, '201', 1, '301', 1, '401', 1, '601', 1, 0) = 1
                        AND b.totassd >= 10000
                        AND EXISTS (
                            SELECT /*+ index(entmod, entmod_sid_ix) */ 1
                            FROM entmod em
                            WHERE em.emodsid = b.tinsid
                                AND em.status = 'O'
                                AND em.mft IN (1, 9, 11, 12, 13, 14, 16, 64)
                                AND b.assnro + 150 < duedt(em.period, em.mft))
                    THEN 1
                    ELSE 0
                END),
            0)                                                     AS PYRENT,
        DECODE(b.segind, 'A', b.PYRIND, 'C', b.PYRIND, 'I', b.PYRIND, 0) AS PYRIND,
        b.FATCAIND                                                 AS FATCAIND,
        b.FEDCONIND                                                AS FEDCONIND,
        b.FEDEMPIND                                                AS FEDEMPIND,
        b.IRSEMPIND                                                AS IRSEMPIND,
        b.L903                                                     AS L903,
        TO_NUMBER(NVL(e.LFIIND, 0))                                AS LFIIND,
        b.LLCIND                                                   AS LLCIND,
        DECODE(b.segind, 'A', b.RPTIND, 'C', b.RPTIND, 'I', b.RPTIND, 'F') AS RPTIND,
        b.THEFTIND                                                 AS THEFTIND,
        b.INSPCIND                                                 AS INSPCIND,
        b.OICACCYR                                                 AS OICACCYR,
        LPAD(NVL(e.RISK, 399) || NVL(e.ARISK, 'e'), 4, ' ')      AS ARANK,
        0                                                          AS TOT_IRP_INC,
        b.EMPTOUCH                                                 AS EMPTOUCH,
        b.LSTTOUCH                                                 AS LSTTOUCH,
        (CASE
            WHEN b.ORG = 'CP' THEN b.CCPTOUCH
            ELSE GREATEST(b.TOTTOUCH, b.EMPTOUCH)
        END)                                                       AS TOTTOUCH,
        NVL(e.STREET2, ' ')                                        AS STREET2,
        b.PROID                                                    AS PROID,
        0                                                          AS TOT_INC_DELQ_YR,
        0                                                          AS PRIOR_YR_RET_AGI_AMT,
        0                                                          AS TXPER_TXPYR_AMT,
        0                                                          AS PRIOR_ASSGMNT_NUM,
        b.AGI_AMT                                                  AS AGI_AMT,
        TO_DATE('01/01/1900', 'mm/dd/yyyy')                        AS PRIOR_ASSGMNT_ACT_DT,
        b.BAL_941                                                  AS BAL_941,
        es.selcode                                                 AS SELCODE,
        -- Columns needed for dedup
        b.segind                                                   AS segind,
        b.ORG                                                      AS ORG,
        e.casecode                                                 AS e_casecode
    FROM ENT e,
         ENTACT a,
         TRANTRAIL b,
         TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c,
         trail_status ts,
         trail_assnro ta_roid,
         trail_assnro ta_aroid,
         entmod_selcode es
    WHERE e.TINSID = a.ACTSID
      AND e.TINSID = b.TINSID
      -- *** FIX #1: Filter TRANTRAIL to matching segment ***
      AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(b.mft, 0, 0, 1)
      -- trail_status join (LEFT via (+))
      AND a.ACTSID  = ts.TINSID(+)
      AND a.ROID    = ts.ROID(+)
      AND ts.rn(+)  = 1
      -- trail_assnro join by ROID (LEFT via (+))
      AND a.ACTSID      = ta_roid.TINSID(+)
      AND a.ROID         = ta_roid.ROID(+)
      -- trail_assnro join by AROID (LEFT via (+))
      AND a.ACTSID      = ta_aroid.TINSID(+)
      AND a.AROID        = ta_aroid.ROID(+)
      -- entmod_selcode join (LEFT via (+))
      AND e.TINSID       = es.emodsid(+)
      -- Original WHERE filters
      AND b.org = :org
      AND (   (    a.aroid BETWEEN 21011000 AND 35165899
               AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
               AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (    a.roid BETWEEN 21011000 AND 35165899
               AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
               AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
)

-- -------------------------------------------------------
-- Final SELECT with deterministic dedup
-- *** FIX #2: ROW_NUMBER with proper ORDER BY tiebreaker
-- *** FIX #3: Row limit on OUTER query, after dedup
-- -------------------------------------------------------
SELECT bd.*
FROM (
    SELECT
        base_data.*,
        ROW_NUMBER() OVER (
            PARTITION BY tin, roid, aroid, tpctrl, actdt, mft, period,
                         typeid, m_type, dispcd, rptcd, tc, cc, amount, status
            ORDER BY extrdt DESC, assnro DESC, ROWID
        ) AS row_num
    FROM base_data
) bd
WHERE bd.row_num = 1
FETCH FIRST 2000 ROWS ONLY;
