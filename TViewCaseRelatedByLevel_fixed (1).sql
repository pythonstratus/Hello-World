-- ============================================================================
-- TViewCaseRelatedByLevel_fixed.sql
-- Fixed version of TViewCaseRelatedByLevel.sql
--
-- FIXES APPLIED:
--   1. Converted ALL joins to comma-style with (+) outer joins
--      (fixes TABLE() + ANSI JOIN alias scoping bug)
--   2. Inlined duedt() as SQL CASE with add_months() in PYRENT EXISTS
--   3. Inlined STATIND() as pre-computed CTE (statind_cte)
--   4. Added PARALLEL hints for performance
--   5. Explicit TO_DATE() on all date literals
--
-- STILL PENDING (need function source from Brian):
--   - case_org(tt.roid) = 'CF'  <-- BIGGEST bottleneck, blocks index usage
--   - GETSEGIND(tt.roid, tt.timesid)
--   - GETASSNQUE()
--   - ASSNPICKDT()
--   - ASSNQPICK()
--   - INTLQPICK()
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

-- Pre-compute STATIND as CTE (replaces per-row PL/SQL function)
-- Returns 2-bit flag: bit1(2)=open ased approaching, bit0(1)=open csed approaching
statind_cte AS (
    SELECT /*+ MATERIALIZE */
        emodsid,
        (CASE WHEN SUM(CASE
            WHEN ased > TO_DATE('01/01/1900','MM/DD/YYYY')
             AND ased < SYSDATE + 180
            THEN 1 ELSE 0 END) > 0 THEN 2 ELSE 0 END)
        +
        (CASE WHEN SUM(CASE
            WHEN csed > TO_DATE('01/01/1900','MM/DD/YYYY')
             AND csed < SYSDATE + 180
            THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END)
        AS statind_val
    FROM entmod
    WHERE status = 'O'
    GROUP BY emodsid
),

-- SELCODE lookup (replaces rownum=1 non-deterministic subquery)
selcode_cte AS (
    SELECT /*+ MATERIALIZE */
        emodsid,
        MIN(selcode) AS selcode
    FROM entmod
    GROUP BY emodsid
),

-- TOUR lookup from entemp (replaces scalar subquery)
tour_cte AS (
    SELECT roid, tour
    FROM entemp
    WHERE elevel > 0
      AND eactive IN ('A', 'Y')
),

-- SEID lookup from entemp (replaces scalar subquery)
seid_cte AS (
    SELECT roid, SEID
    FROM entemp
    WHERE eactive IN ('A', 'Y')
      AND elevel >= 0
),

-- Main query with comma-style joins (fixes TABLE() alias scoping)
main_data AS (
    SELECT /*+ PARALLEL(a, 4) PARALLEL(b, 4) */
        tt.ROID,

        -- SEID: from pre-computed CTE
        p.SEID,

        a.TIN,
        a.TINTT,
        a.TINFS,
        tt.TIMESID AS TINSID,
        SUBSTR(a.TP, 1, 35) AS TP,
        a.RISK AS C_RISK,
        tt.RISK AS H_RISK,
        GETSEGIND(tt.roid, tt.timesid) AS C_CASEIND,
        b.SEGIND AS H_CASEIND,
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

        -- TOUR: from pre-computed CTE
        tour_sub.TOUR,

        tt.PRGNAME1,
        tt.PRGNAME2,
        a.TOTASSD,
        c.BAL_941_14,
        a.GRADE AS CASEGRADE,
        NVL(b.NAICSCD, ' ') AS NAICSCD,
        a.CCNIPSELECTCD,
        c.CNT_941_14,
        c.CNT_941,
        c.TDI_CNT_941,
        NVL(b.TDAcnt, 0) AS TDACNT,
        NVL(b.TDIcnt, 0) AS TDICNT,
        NVL((b.TDAcnt + b.TDIcnt), 0) AS MODCNT,

        -- STATIND: inlined from CTE (replaces PL/SQL function)
        DECODE(b.STATUS, 'O', NVL(si.statind_val, 0), 0) AS STATIND,

        NVL(b.ASSNFLD, TO_DATE('01/01/1900', 'mm/dd/yyyy')) AS ASSNFLD,

        -- ASSNQUE: conditional on segind
        CASE
            WHEN b.segind IN ('A', 'C', 'I') THEN
                GETASSNQUE(a.TIN, a.TINTT, a.TINFS, a.ASSNCFF, b.ASSNRO)
            ELSE
                b.ASSNRO
        END AS ASSNQUE,

        -- CLOSEDT
        NVL(
            DECODE(b.status,
                'C', b.CLOSEDT,
                'X', b.CLOSEDT,
                TO_DATE('01/01/1900', 'mm/dd/yyyy')),
            TO_DATE('01/01/1900', 'mm/dd/yyyy')
        ) AS CLOSEDT,

        NVL(a.DT_DOD, TO_DATE('01/01/1900', 'mm/dd/yyyy')) AS DT_DOD,
        b.XXDT,
        b.INITDT,
        a.DT_OA,
        a.DT_POA,
        TRUNC(ASSNPICKDT(a.TIN, a.TINFS, a.TINTT, b.STATUS, b.PROID, b.ROID)) AS PICKDT,
        a.DVICTCD,

        -- QPICKIND
        DECODE(DECODE(b.ZIPCDE, 00000,
            ASSNQPICK(a.TIN, a.TINFS, a.TINTT, INTLQPICK(a.TIN, a.TINFS, a.TINTT, b.STATUS)),
            DECODE(a.CITY, 'APO', ASSNQPICK(a.TIN, a.TINFS, a.TINTT, INTLQPICK(a.TIN, a.TINFS, a.TINTT, b.STATUS)),
                'FPO',
                ASSNQPICK(a.TIN, a.TINFS, a.TINTT, INTLQPICK(a.TIN, a.TINFS, a.TINTT, b.STATUS)),
                ASSNQPICK(a.TIN, a.TINFS, a.TINTT, b.PROID))),
            '',0, 0,0, 1,1, 2,2, 3,3, 4,4, 5,5, 6,6, 7,7, '?') AS QPICKIND,

        b.FLDHRS,
        NVL(b.EMPHRS, 0) AS EMPHRS,
        b.HRS,
        CASE
            WHEN b.ORG = 'CP' THEN a.CCPHRS
            ELSE GREATEST(NVL(a.TOTHRS, 0), NVL(b.EMPHRS, 0))
        END AS TOTHRS,
        c.IND_941 AS IND_941,
        CASE WHEN c.ind_941 = 0 THEN 'No' ELSE 'Yes' END AS FORMATTED_IND_941,
        a.HINFIND,
        DECODE(b.segind, 'A', a.AGEIND, 'C', a.AGEIND, 'I', a.AGEIND, 'C') AS AGEIND,
        TO_NUMBER(a.PDTIND) AS CAUIND,

        -- PYRENT: EXISTS with inlined duedt() (no PL/SQL context switch)
        DECODE(
            b.STATUS,
            'O',
            CASE
                WHEN DECODE(b.SEGIND, 'C', 1, 'A', 1, 'I', 1, 0) = 1
                 AND DECODE(a.casecode, '201', 1, '301', 1, '401', 1, '601', 1, 0) = 1
                 AND a.totassd >= 10000
                 AND EXISTS (
                     SELECT /*+ index(em, entmod_sid_ix) */ 1
                     FROM entmod em
                     WHERE em.emodsid = b.tinsid
                       AND em.status = 'O'
                       AND em.mft IN (1, 9, 11, 12, 13, 14, 16, 64)
                       AND b.assnro + 150 <
                           CASE
                               WHEN em.mft IN (1,10,11,14) THEN add_months(em.period, 1)
                               WHEN em.mft IN (5,30)       THEN add_months(em.period, 3) + 15
                               WHEN em.mft = 2             THEN add_months(em.period, 2) + 15
                               WHEN em.mft = 3             THEN add_months(em.period, 2)
                               ELSE TO_DATE('01-JAN-1900','DD-MON-YYYY')
                           END
                 )
                THEN 1
                ELSE 0
            END,
            0
        ) AS PYRENT,

        DECODE(b.segind, 'A', a.PYRIND, 'C', a.PYRIND, 'I', a.PYRIND, 0) AS PYRIND,
        a.FATCAIND,
        a.FEDCONIND,
        a.FEDEMPIND,
        a.IRSEMPIND,
        a.L903,
        TO_NUMBER(NVL(a.LFIIND, 0)) AS LFIIND,
        a.LLCIND,
        DECODE(b.segind, 'A', a.RPTIND, 'C', a.RPTIND, 'I', a.RPTIND, 'F') AS RPTIND,
        a.THEFTIND,
        a.INSPCIND,
        a.OICACCYR,
        LPAD(NVL(a.RISK, 399) || NVL(a.ARISK, 'e'), 4, ' ') AS ARANK,
        0 AS TOT_IRP_INC,
        b.EMPTOUCH,
        b.LSTTOUCH,
        CASE
            WHEN b.ORG = 'CP' THEN a.CCPTOUCH
            ELSE GREATEST(a.TOTTOUCH, b.EMPTOUCH)
        END AS TOTTOUCH,
        NVL(a.STREET2, ' ') AS STREET2,
        b.PROID,
        0 AS TOT_INC_DELQ_YR,
        0 AS PRIOR_YR_RET_AGI_AMT,
        0 AS TXPER_TXPYR_AMT,
        0 AS PRIOR_ASSGMNT_NUM,
        a.AGI_AMT,
        TO_DATE('01/01/1900', 'mm/dd/yyyy') AS PRIOR_ASSGMNT_ACT_DT,
        c.BAL_941,

        -- SELCODE: from pre-computed CTE
        sc.selcode AS SELCODE,

        REPLACE(b.STATUS, 'X', 'C') AS STATUS,
        NVL(b.SEGIND, '?') AS CASEIND,
        NVL(b.DISPCD, -1) AS DISPCD,
        b.CC,
        DECODE(a.ASSNCFF, '01/01/1900', b.ASSNRO, a.ASSNCFF) AS ASSNCFF,
        NVL(b.ASSNRO, TO_DATE('01/01/1900', 'mm/dd/yyyy')) AS ASSNRO,
        DECODE(b.segind, 'A', a.LDIND, 'C', a.LDIND, 'I', a.LDIND, 'F') AS LDIND,
        a.TPCTRL,
        NVL(a.RISK, 399) AS RISK,
        NVL(a.CITY, ' ') AS CITY,
        a.STATE,
        SUBSTR(NVL(a.TP2, ' '), 1, 35) AS TP2,
        SUBSTR(NVL(a.STREET, ' '), 1, 35) AS STREET,

        -- ZIPCODE: Simplified logic
        CASE
            WHEN b.zipcde < 100000 THEN
                TO_NUMBER(TO_CHAR(b.ZIPCDE, '09999'))
            WHEN b.zipcde BETWEEN 99999 AND 999999999 THEN
                NVL(TO_NUMBER(SUBSTR(TO_CHAR(b.ZIPCDE, '099999999'), -9, 5)), 0)
            ELSE
                NVL(TO_NUMBER(SUBSTR(TO_CHAR(b.ZIPCDE, '099999999999'), -12, 5)), 0)
        END AS ZIPCDE

    -- ================================================================
    -- ALL comma-style joins (required for TABLE() function compatibility)
    -- ================================================================
    FROM ENT a,
         filtered_timetin tt,
         TRANTRAIL b,
         TABLE(mft_ind_vals(b.tinsid, a.tinfs)) c,
         seid_cte p,
         tour_cte tour_sub,
         selcode_cte sc,
         statind_cte si

    WHERE a.TINSID = tt.TIMESID
      AND a.TINSID = b.TINSID

      -- Outer joins using (+) syntax
      AND tt.roid = p.roid(+)
      AND tt.roid = tour_sub.roid(+)
      AND a.tinsid = sc.emodsid(+)
      AND a.TINSID = si.emodsid(+)

      -- Level-based filtering
      AND trunc(tt.roid/power(10, 8 - :elevel)) = :levelValue
      AND sysdate - tt.rptdt <= :daysUpperLimit
      AND rownum <= 500
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
