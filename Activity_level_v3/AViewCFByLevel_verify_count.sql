-- ============================================================================
-- VERIFICATION: Corrected AViewCFByLevel count for GM 251435
-- Expected: ≈ 2650 (matching legacy)
-- ============================================================================

-- VERIFY 1: Quick count using corrected structure (simplified columns)
SELECT COUNT(*) AS corrected_count
FROM (
    SELECT
        a.TIN, a.ROID, a.AROID, a.ACTDT, a.PERIOD, a.TYPEID,
        a.CODE, a.CC, a.EXTRDT,
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) AS tpctrl,
        MFT,
        TYPCD AS m_type,
        DISPCODE AS dispcd,
        RPTCD,
        TC,
        AMOUNT,
        -- STATUS via correlated subquery with NVL 'P' default
        NVL(
            (SELECT status FROM (
                SELECT c2.status,
                       ROW_NUMBER() OVER (ORDER BY c2.EXTRDT DESC, c2.ROWID) AS rn
                FROM TRANTRAIL c2
                WHERE c2.tinsid = a.actsid AND c2.roid = a.roid
                  AND c2.EXTRDT = (
                      SELECT NVL(MAX(d.EXTRDT), TO_DATE('01/01/1900','mm/dd/yyyy'))
                      FROM TRANTRAIL d
                      WHERE d.TINSID = c2.TINSID AND d.ROID = c2.ROID
                        AND DECODE(d.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                            DECODE(mft, 0, 0, 1))
            ) WHERE rn = 1), 'P') AS status,
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, MFT, a.PERIOD, a.TYPEID,
                         TYPCD, DISPCODE, RPTCD, TC, a.CC, AMOUNT,
                         NVL(
                            (SELECT status FROM (
                                SELECT c3.status,
                                       ROW_NUMBER() OVER (ORDER BY c3.EXTRDT DESC, c3.ROWID) AS rn
                                FROM TRANTRAIL c3
                                WHERE c3.tinsid = a.actsid AND c3.roid = a.roid
                                  AND c3.EXTRDT = (
                                      SELECT NVL(MAX(d2.EXTRDT), TO_DATE('01/01/1900','mm/dd/yyyy'))
                                      FROM TRANTRAIL d2
                                      WHERE d2.TINSID = c3.TINSID AND d2.ROID = c3.ROID
                                        AND DECODE(d2.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                                            DECODE(mft, 0, 0, 1))
                            ) WHERE rn = 1), 'P')
            ORDER BY a.EXTRDT DESC,
                     (SELECT MAX(t2.ASSNRO) FROM TRANTRAIL t2
                      WHERE (t2.roid = a.aroid OR t2.roid = a.roid)
                        AND t2.tinsid = a.actsid
                        AND DECODE(t2.segind,'A',1,'C',1,'I',1,0) = DECODE(mft,0,0,1)) DESC,
                     a.ROID
        ) AS row_num
    FROM ENT e,
         ENTACT a,
         TABLE(mft_ind_vals(a.ACTSID, e.tinfs)) c
    WHERE e.TINSID = a.ACTSID
      AND a.roid BETWEEN 25143500 AND 25143599
      AND SYSDATE - a.actdt <= 90
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
) WHERE row_num = 1;

-- NOTE: This will be SLOW (correlated subqueries fire per row).
-- But it will tell us if the count matches legacy.
-- If count ≈ 2650, the corrected AViewCFByLevel_corrected_v2.sql is ready.
