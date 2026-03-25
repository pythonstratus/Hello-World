-- ============================================================================
-- AViewCFByLevel_Regular.sql
-- Non-CTE version — fixes duplicates and count mismatches
-- ============================================================================
-- ROOT CAUSES FIXED:
--   1. TRANTRAIL join now filtered by segment (segind/mft match)
--   2. ROW_NUMBER ORDER BY has deterministic tiebreaker (extrdt DESC, ROWID)
--   3. Row limit moved to OUTER query (FETCH FIRST, after dedup)
--   4. All joins converted to comma-style for TABLE() compatibility
--   5. STATUS subquery: added ORDER BY for deterministic ROWNUM=1
--   6. SELCODE subquery: replaced ROWNUM=1 with MIN() for determinism
-- ============================================================================

SELECT *
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY tin, roid, aroid, tpctrl, actdt, mft, period,
                         typeid, m_type, dispcd, rptcd, tc, cc, amount, status
            -- *** FIX #2: Deterministic tiebreaker ***
            ORDER BY extrdt DESC, assnro DESC, ROWID
        ) AS row_num
    FROM (
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
            -- *** FIX #5: Deterministic STATUS subquery ***
            NVL(
                (SELECT status
                 FROM (
                     SELECT c.status,
                            ROW_NUMBER() OVER (ORDER BY c.EXTRDT DESC, c.ROWID) AS rn
                     FROM TRANTRAIL c
                     WHERE c.tinsid = a.actsid
                       AND c.roid = a.roid
                       AND c.EXTRDT = (
                           SELECT /*+ index(d, trantrail_tinsid_ix) */
                               NVL(MAX(d.EXTRDT), TO_DATE('01/01/1900', 'mm/dd/yyyy'))
                           FROM TRANTRAIL d
                           WHERE d.TINSID = c.TINSID
                             AND d.ROID = c.ROID
                             AND DECODE(d.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                                 DECODE(d.mft, 0, 0, 1)
                       )
                 ) WHERE rn = 1),
                'P')                                                   AS STATUS,
            e.LDIND                                                    AS LDIND,
            e.RISK                                                     AS RISK,
            b.MFT                                                      AS MFT,
            a.PERIOD                                                   AS PERIOD,
            b.TYPCD                                                    AS M_TYPE,
            a.AROID                                                    AS AROID,
            (SELECT MAX(t.ASSNRO)
             FROM TRANTRAIL t
             WHERE (t.roid = a.aroid OR t.roid = a.roid)
               AND t.tinsid = a.actsid
               AND DECODE(t.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                   DECODE(t.mft, 0, 0, 1))                            AS ASSNRO,
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
            -- *** FIX #6: Deterministic SELCODE (MIN instead of rownum=1) ***
            (SELECT MIN(selcode) FROM entmod WHERE emodsid = e.tinsid) AS SELCODE
        -- *** FIX #4: All comma-style joins for TABLE() compatibility ***
        FROM ENT e,
             ENTACT a,
             TRANTRAIL b,
             TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
        WHERE e.TINSID = a.ACTSID
          AND e.TINSID = b.TINSID
          -- *** FIX #1: Segment filter on TRANTRAIL ***
          AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(b.mft, 0, 0, 1)
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
          -- *** FIX #3: Removed rownum <= 2000 from inner query ***
    ) t
)
WHERE row_num = 1
-- *** FIX #3: Row limit on OUTER query, after dedup ***
FETCH FIRST 2000 ROWS ONLY;
