-- ============================================================================
-- AViewCFByLevel.sql — CORRECTED (v2)
-- ============================================================================
-- ROOT CAUSE (confirmed by diagnostics):
--   DIAG 7  = 8    → 8 ENTACT records have NO TRANTRAIL for CF
--   DIAG 8  = 33   → 33 have TRANTRAIL but no segind A/C/I match
--   DIAG 11 = 2642 → Without TRANTRAIL in FROM → 2642 + 8 = 2650 (EXACT)
--
-- PROBLEM: Modern query INNER JOINed TRANTRAIL in FROM with a segind/mft
--          DECODE filter. This dropped records AND multiplied others.
--          Legacy AVIEW never uses TRANTRAIL for base cardinality.
--
-- FIX: TRANTRAIL is now an OUTER-JOINED subquery that picks at most ONE
--      row per TINSID (most recent by EXTRDT, matching org). Records with
--      no TRANTRAIL match survive with NVL defaults.
--      STATUS and ASSNRO remain as correlated subqueries (legacy match).
--      mft_ind_vals uses a.ACTSID instead of b.tinsid.
--      Comma-style joins with (+) used throughout for TABLE() compatibility.
-- ============================================================================

SELECT *
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY tin, roid, aroid, tpctrl, actdt, mft, period,
                         typeid, m_type, dispcd, rptcd, tc, cc, amount, status
            ORDER BY extrdt DESC, assnro DESC, ROID
        ) AS row_num
    FROM (
        SELECT
            /*+ index_join(a, entact_actdt_ix, entact_roid_ix) */
            a.ROID                                                      AS ROID,
            a.TIN                                                       AS TIN,
            a.TINTT                                                     AS TINTT,
            a.TINFS                                                     AS TINFS,
            a.ACTSID                                                    AS TINSID,
            DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl)           AS TPCTRL,
            SUBSTR(e.TP, 1, 35)                                         AS TP,
            SUBSTR(e.TP2, 1, 35)                                        AS TP2,
            SUBSTR(e.STREET, 1, 35)                                     AS STREET,
            e.CITY                                                      AS CITY,
            e.STATE                                                     AS STATE,
            (CASE
                WHEN e.zipcde < 100000
                    THEN TO_NUMBER(TO_CHAR(e.ZIPCDE, '09999'))
                WHEN e.zipcde BETWEEN 99999 AND 999999999
                    THEN NVL(TO_NUMBER(SUBSTR(TO_CHAR(e.ZIPCDE, '099999999'), -9, 5)), 0)
                ELSE
                    NVL(TO_NUMBER(SUBSTR(TO_CHAR(e.ZIPCDE, '099999999999'), -12, 5)), 0)
            END)                                                        AS ZIPCDE,
            a.ACTDT                                                     AS ACTDT,
            a.CODE                                                      AS CASECODE,
            a.SUBCODE                                                   AS SUBCODE,
            -- STATUS: correlated subquery with NVL default 'P' (LEGACY MATCH)
            NVL(
                (SELECT status
                 FROM (
                     SELECT c2.status,
                            ROW_NUMBER() OVER (ORDER BY c2.EXTRDT DESC, c2.ROWID) AS rn
                     FROM TRANTRAIL c2
                     WHERE c2.tinsid = a.actsid
                         AND c2.roid = a.roid
                         AND c2.EXTRDT = (
                             SELECT /*+ index(d, trantrail_tinsid_ix) */
                                 NVL(MAX(d.EXTRDT), TO_DATE('01/01/1900', 'mm/dd/yyyy'))
                             FROM TRANTRAIL d
                             WHERE d.TINSID = c2.TINSID
                                 AND d.ROID = c2.ROID
                                 AND DECODE(d.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                                     DECODE(mft, 0, 0, 1)
                         )
                 ) WHERE rn = 1),
                'P')                                                    AS STATUS,
            e.LDIND                                                     AS LDIND,
            e.RISK                                                      AS RISK,
            MFT                                                         AS MFT,
            a.PERIOD                                                    AS PERIOD,
            TYPCD                                                       AS M_TYPE,
            a.AROID                                                     AS AROID,
            -- ASSNRO: correlated subquery (LEGACY MATCH)
            (SELECT MAX(t2.ASSNRO)
             FROM TRANTRAIL t2
             WHERE (t2.roid = a.aroid OR t2.roid = a.roid)
                 AND t2.tinsid = a.actsid
                 AND DECODE(t2.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                     DECODE(mft, 0, 0, 1))                              AS ASSNRO,
            e.ASSNCFF                                                   AS ASSNCFF,
            BODCD                                                       AS BODCD,
            AMOUNT                                                      AS AMOUNT,
            RTNSEC                                                      AS RTNSEC,
            DISPCODE                                                    AS DISPCD,
            GRPIND                                                      AS GRPIND,
            FORM809                                                     AS FORM809,
            RPTCD                                                       AS RPTCD,
            a.CC                                                        AS CC,
            TC                                                          AS TC,
            a.EXTRDT                                                    AS EXTRDT,
            a.TYPEID                                                    AS TYPEID,
            a.TSACTCD                                                   AS TSACTCD,
            TOTASSD                                                     AS TOTASSD,
            BAL_941_14                                                  AS BAL_941_14,
            e.GRADE                                                     AS CASEGRADE,
            CASEIND                                                     AS CASEIND,
            NVL(b.NAICSCD, '      ')                                    AS NAICSCD,
            PRGNAME1                                                    AS PRGNAME1,
            PRGNAME2                                                    AS PRGNAME2,
            CCNIPSELECTCD                                               AS CCNIPSELECTCD,
            CNT_941_14                                                  AS CNT_941_14,
            CNT_941                                                     AS CNT_941,
            TDI_CNT_941                                                 AS TDI_CNT_941,
            NVL(b.TDAcnt, 0)                                            AS TDACNT,
            NVL(b.TDIcnt, 0)                                            AS TDICNT,
            NVL((b.TDAcnt + b.TDIcnt), 0)                               AS MODCNT,
            DECODE(NVL(b.STATUS, 'X'), 'O', STATIND(a.ACTSID), 0)       AS STATIND,
            b.ASSNFLD                                                   AS ASSNFLD,
            (CASE
                WHEN NVL(b.segind, ' ') IN ('A', 'C', 'I')
                    THEN GETASSNQUE(e.TIN, e.TINTT, e.TINFS, e.ASSNCFF, NVL(b.ASSNRO, 0))
                ELSE NVL(b.ASSNRO, 0)
            END)                                                        AS ASSNQUE,
            NVL(
                DECODE(NVL(b.status, ' '),
                    'C', b.CLOSEDT,
                    'X', b.CLOSEDT,
                    TO_DATE('01/01/1900', 'mm/dd/yyyy')),
                TO_DATE('01/01/1900', 'mm/dd/yyyy'))                    AS CLOSEDT,
            NVL(DT_DOD, TO_DATE('01/01/1900', 'mm/dd/yyyy'))            AS DT_DOD,
            b.XXDT                                                      AS XXDT,
            b.INITDT                                                    AS INITDT,
            DT_OA                                                       AS DT_OA,
            DT_POA                                                      AS DT_POA,
            TRUNC(ASSNPICKDT(e.TIN, e.TINFS, e.TINTT,
                NVL(b.STATUS, 'P'), NVL(b.PROID, 0), a.ROID))           AS PICKDT,
            DVICTCD                                                     AS DVICTCD,
            DECODE(DECODE(NVL(b.ZIPCDE, 0), 00000,
                ASSNQPICK(e.TIN, e.TINFS, e.TINTT, INTLQPICK(e.TIN, e.TINFS, e.TINTT, NVL(b.STATUS, 'P'))),
                DECODE(e.CITY, 'APO', ASSNQPICK(e.TIN, e.TINFS, e.TINTT, INTLQPICK(e.TIN, e.TINFS, e.TINTT, NVL(b.STATUS, 'P'))),
                'FPO',
                ASSNQPICK(e.TIN, e.TINFS, e.TINTT, INTLQPICK(e.TIN, e.TINFS, e.TINTT, NVL(b.STATUS, 'P'))),
                ASSNQPICK(e.TIN, e.TINFS, e.TINTT, NVL(b.PROID, 0)))),
                '', 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, '?') AS QPICKIND,
            NVL(b.FLDHRS, 0)                                            AS FLDHRS,
            NVL(b.EMPHRS, 0)                                            AS EMPHRS,
            NVL(b.HRS, 0)                                               AS HRS,
            (CASE
                WHEN NVL(b.ORG, ' ') = 'CP' THEN CCPHRS
                ELSE GREATEST(NVL(TOTHRS, 0), NVL(b.EMPHRS, 0))
            END)                                                        AS TOTHRS,
            c.IND_941                                                   AS IND_941,
            (CASE WHEN c.ind_941 = 0 THEN 'No' ELSE 'Yes' END)         AS FORMATTED_IND_941,
            HINFIND                                                     AS HINFIND,
            DECODE(NVL(b.segind, ' '), 'A', AGEIND, 'C', AGEIND, 'I', AGEIND, 'C') AS AGEIND,
            TO_NUMBER(PDTIND)                                           AS CAUIND,
            DECODE(
                NVL(b.STATUS, 'X'),
                'O', (CASE
                    WHEN DECODE(NVL(b.SEGIND, ' '), 'C', 1, 'A', 1, 'I', 1, 0) = 1
                        AND DECODE(e.casecode, '201', 1, '301', 1, '401', 1, '601', 1, 0) = 1
                        AND totassd >= 10000
                        AND EXISTS (
                            SELECT /*+ index(entmod, entmod_sid_ix) */ 1
                            FROM entmod em
                            WHERE em.emodsid = b.tinsid
                                AND em.status = 'O'
                                AND em.mft IN (1, 9, 11, 12, 13, 14, 16, 64)
                                AND NVL(b.assnro, 0) + 150 < duedt(em.period, em.mft))
                    THEN 1
                    ELSE 0
                END),
                0)                                                      AS PYRENT,
            DECODE(NVL(b.segind, ' '), 'A', PYRIND, 'C', PYRIND, 'I', PYRIND, 0) AS PYRIND,
            FATCAIND                                                    AS FATCAIND,
            FEDCONIND                                                   AS FEDCONIND,
            FEDEMPIND                                                   AS FEDEMPIND,
            IRSEMPIND                                                   AS IRSEMPIND,
            L903                                                        AS L903,
            TO_NUMBER(NVL(e.LFIIND, 0))                                  AS LFIIND,
            LLCIND                                                      AS LLCIND,
            DECODE(NVL(b.segind, ' '), 'A', RPTIND, 'C', RPTIND, 'I', RPTIND, 'F') AS RPTIND,
            THEFTIND                                                    AS THEFTIND,
            INSPCIND                                                    AS INSPCIND,
            OICACCYR                                                    AS OICACCYR,
            LPAD(NVL(e.RISK, 399) || NVL(e.ARISK, 'e'), 4, ' ')        AS ARANK,
            0                                                           AS TOT_IRP_INC,
            b.EMPTOUCH                                                  AS EMPTOUCH,
            b.LSTTOUCH                                                  AS LSTTOUCH,
            (CASE
                WHEN NVL(b.ORG, ' ') = 'CP' THEN CCPTOUCH
                ELSE GREATEST(TOTTOUCH, b.EMPTOUCH)
            END)                                                        AS TOTTOUCH,
            NVL(e.STREET2, ' ')                                         AS STREET2,
            NVL(b.PROID, 0)                                             AS PROID,
            0                                                           AS TOT_INC_DELQ_YR,
            0                                                           AS PRIOR_YR_RET_AGI_AMT,
            0                                                           AS TXPER_TXPYR_AMT,
            0                                                           AS PRIOR_ASSGMNT_NUM,
            AGI_AMT                                                     AS AGI_AMT,
            TO_DATE('01/01/1900', 'mm/dd/yyyy')                         AS PRIOR_ASSGMNT_ACT_DT,
            BAL_941                                                     AS BAL_941,
            (SELECT MIN(selcode) FROM entmod WHERE emodsid = e.tinsid)  AS SELCODE
        -- =================================================================
        -- FROM clause: TRANTRAIL is OUTER-JOINED as a ranked subquery.
        -- Picks at most ONE row per TINSID (most recent EXTRDT for org).
        -- Records with no TRANTRAIL match survive — b.columns become NULL
        -- and are handled by NVL defaults above.
        -- mft_ind_vals uses a.ACTSID (not b.tinsid) since b may be NULL.
        -- =================================================================
        FROM ENT e,
             ENTACT a,
             TABLE(mft_ind_vals(a.ACTSID, e.tinfs)) c,
             (SELECT tb.*,
                     ROW_NUMBER() OVER (
                         PARTITION BY tb.TINSID
                         ORDER BY tb.EXTRDT DESC, tb.ROWID DESC
                     ) AS trail_rn
              FROM TRANTRAIL tb
              WHERE tb.org = :org
             ) b
        WHERE e.TINSID = a.ACTSID
            -- OUTER JOIN: TRANTRAIL is optional, records survive without it
            AND a.ACTSID = b.TINSID (+)
            AND b.trail_rn (+) = 1
            -- Standard ENTACT filters
            AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                      AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                      AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
                OR  (     a.roid BETWEEN 21011000 AND 35165899
                      AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                      AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
            AND EXTRACT(YEAR FROM a.period) > 1901
            AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
            AND SYSDATE - a.actdt <= :daysUpperLimit
    ) t
)
WHERE row_num = 1
FETCH FIRST 2000 ROWS ONLY
