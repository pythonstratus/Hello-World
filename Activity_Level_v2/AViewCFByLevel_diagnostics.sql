-- ============================================================================
-- ACTIVITY VIEW DIAGNOSTIC SCRIPTS
-- AViewCFByLevel.sql: Legacy=2650, Modern=1853, Gap=797 for GM 251435
-- ============================================================================
-- INSTRUCTIONS: Run each diagnostic in order. Record the count from each.
-- Replace :org, :elevel, :levelValue, :daysUpperLimit with your actual values.
-- For GM 251435 at E-level:
--   :org = 'CF' (assumed)
--   :elevel = ? (likely 6 or 7 — confirm)
--   :levelValue = 251435 (or 2514350 — confirm)
--   :daysUpperLimit = ? (confirm the value used in legacy)
-- ============================================================================


-- ============================================================================
-- SECTION 0: CONFIRM BIND VARIABLE VALUES
-- Run these first to make sure we know exactly what the legacy is using.
-- ============================================================================

-- 0A: What E-level value makes TRUNC(roid/POWER(10,8-elevel)) = 251435?
-- If levelValue = 251435 (6 digits), elevel must make POWER(10, 8-elevel)
-- produce enough truncation. Test:
SELECT
    6 AS test_elevel,
    POWER(10, 8-6) AS divisor_e6,
    '-- TRUNC(roid/' || POWER(10, 8-6) || ') = 251435 means roid in 25143500..25143599' AS roid_range_e6,
    7 AS test_elevel2,
    POWER(10, 8-7) AS divisor_e7,
    '-- TRUNC(roid/' || POWER(10, 8-7) || ') = 251435 means roid in 2514350..2514359 (TOO SHORT)' AS roid_range_e7
FROM DUAL;

-- 0B: Verify the ENTACT roid range for this GM
SELECT COUNT(*) AS entact_rows_for_gm,
       MIN(a.roid) AS min_roid,
       MAX(a.roid) AS max_roid,
       COUNT(DISTINCT a.roid) AS distinct_roids
FROM ENTACT a
WHERE TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue;


-- ============================================================================
-- SECTION 1: LAYER-BY-LAYER COUNT ANALYSIS
-- Peel back each join/filter to find where the 797 records are lost.
-- ============================================================================

-- DIAG 1: ENTACT alone with date + roid filters (no joins)
-- This is the universe of activity records for this GM.
SELECT COUNT(*) AS diag1_entact_only
FROM ENTACT a
WHERE TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));

-- DIAG 2: ENTACT + ENT join (still no TRANTRAIL)
SELECT COUNT(*) AS diag2_entact_plus_ent
FROM ENT e,
     ENTACT a
WHERE e.TINSID = a.ACTSID
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));

-- DIAG 3: ENTACT + ENT + TRANTRAIL (INNER JOIN on TINSID only, no other filters on TRANTRAIL)
-- This shows the cartesian expansion from TRANTRAIL
SELECT COUNT(*) AS diag3_with_trantrail_no_filter
FROM ENT e,
     ENTACT a,
     TRANTRAIL b
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));

-- DIAG 4: Add b.org = :org filter to TRANTRAIL
SELECT COUNT(*) AS diag4_trantrail_org_filter
FROM ENT e,
     ENTACT a,
     TRANTRAIL b
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND b.org = :org
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));

-- DIAG 5: Add segind/mft DECODE filter
SELECT COUNT(*) AS diag5_segind_mft_filter
FROM ENT e,
     ENTACT a,
     TRANTRAIL b,
     TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
  AND b.org = :org
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));

-- DIAG 6: Full modern query COUNT (before dedup) — should match DIAG 5
-- This is the inner "t" subquery count
SELECT COUNT(*) AS diag6_pre_dedup_count
FROM ENT e,
     ENTACT a,
     TRANTRAIL b,
     TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
  AND b.org = :org
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));

-- DIAG 7: Full modern query COUNT (AFTER dedup) — this should give 1853
SELECT COUNT(*) AS diag7_post_dedup_count
FROM (
    SELECT
        a.TIN, a.ROID, a.AROID, a.ACTDT, a.PERIOD, a.TYPEID,
        a.CODE, a.SUBCODE, a.CC, a.EXTRDT,
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) AS TPCTRL,
        MFT, TYPCD AS M_TYPE, DISPCODE AS DISPCD, RPTCD, TC, AMOUNT,
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
                        AND DECODE(d.segind,'A',1,'C',1,'I',1,0) = DECODE(mft,0,0,1))
            ) WHERE rn = 1), 'P') AS STATUS,
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, MFT, a.PERIOD,
                         a.TYPEID, TYPCD, DISPCODE, RPTCD, TC, a.CC, AMOUNT,
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
                                        AND DECODE(d2.segind,'A',1,'C',1,'I',1,0) = DECODE(mft,0,0,1))
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
         TRANTRAIL b,
         TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
    WHERE e.TINSID = a.ACTSID
      AND e.TINSID = b.TINSID
      AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
      AND b.org = :org
      AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
) WHERE row_num = 1;


-- ============================================================================
-- SECTION 2: FIND THE MISSING 797 RECORDS
-- These diagnostics identify ENTACT records that exist in the base set
-- but get dropped when joining to TRANTRAIL.
-- ============================================================================

-- DIAG 8: ***CRITICAL*** How many ENTACT records have NO matching TRANTRAIL?
-- If this is ~797, we found the root cause.
SELECT COUNT(*) AS diag8_entact_with_no_trantrail
FROM ENT e,
     ENTACT a
WHERE e.TINSID = a.ACTSID
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID
        AND b.org = :org
  );

-- DIAG 9: Same but check with segind/mft condition too
-- ENTACT records where TRANTRAIL exists for org but NOT for matching segind/mft
SELECT COUNT(*) AS diag9_no_matching_segind_mft
FROM ENT e,
     ENTACT a
WHERE e.TINSID = a.ACTSID
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
  AND EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID AND b.org = :org
  )
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b,
           TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
      WHERE b.TINSID = e.TINSID
        AND b.org = :org
        AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
  );


-- ============================================================================
-- SECTION 3: TRANTRAIL JOIN CARDINALITY ANALYSIS
-- Understand how TRANTRAIL multiplies or filters the result set.
-- ============================================================================

-- DIAG 10: For the GM's ENTACT records, how many TRANTRAIL rows exist per TINSID?
SELECT
    CASE
        WHEN trail_count = 0 THEN '0 (NO MATCH - DROPPED!)'
        WHEN trail_count = 1 THEN '1 (single match)'
        WHEN trail_count BETWEEN 2 AND 5 THEN '2-5'
        WHEN trail_count BETWEEN 6 AND 20 THEN '6-20'
        ELSE '20+'
    END AS trantrail_rows_per_tinsid,
    COUNT(*) AS entact_record_count
FROM (
    SELECT a.ACTSID,
           (SELECT COUNT(*) FROM TRANTRAIL b
            WHERE b.TINSID = a.ACTSID
              AND b.org = :org) AS trail_count
    FROM ENTACT a
    WHERE TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
)
GROUP BY CASE
        WHEN trail_count = 0 THEN '0 (NO MATCH - DROPPED!)'
        WHEN trail_count = 1 THEN '1 (single match)'
        WHEN trail_count BETWEEN 2 AND 5 THEN '2-5'
        WHEN trail_count BETWEEN 6 AND 20 THEN '6-20'
        ELSE '20+'
    END
ORDER BY 1;


-- ============================================================================
-- SECTION 4: mft_ind_vals MULTIPLIER ANALYSIS
-- Check how the pipelined table function affects cardinality.
-- ============================================================================

-- DIAG 11: How many rows does mft_ind_vals produce per TINSID?
SELECT
    mft_count,
    COUNT(*) AS tinsid_count
FROM (
    SELECT b.TINSID,
           COUNT(*) AS mft_count
    FROM TRANTRAIL b,
         ENT e,
         TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
    WHERE e.TINSID = b.TINSID
      AND b.org = :org
      AND b.TINSID IN (
          SELECT DISTINCT a.ACTSID FROM ENTACT a
          WHERE TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
            AND SYSDATE - a.actdt <= :daysUpperLimit
      )
    GROUP BY b.TINSID
)
GROUP BY mft_count
ORDER BY mft_count;


-- ============================================================================
-- SECTION 5: ROW_NUMBER DEDUP IMPACT
-- Check how many records are eliminated by the dedup.
-- ============================================================================

-- DIAG 12: Count pre-dedup vs post-dedup (simplified — just partition key columns)
SELECT
    COUNT(*) AS total_pre_dedup,
    COUNT(DISTINCT tin || '|' || roid || '|' || aroid || '|' || tpctrl || '|' ||
          TO_CHAR(actdt,'YYYYMMDD') || '|' || mft || '|' ||
          TO_CHAR(period,'YYYYMMDD') || '|' || typeid || '|' ||
          m_type || '|' || dispcd || '|' || rptcd || '|' ||
          tc || '|' || cc || '|' || amount || '|' || status
    ) AS distinct_partition_keys
FROM (
    SELECT
        a.TIN AS tin,
        a.ROID AS roid,
        a.AROID AS aroid,
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) AS tpctrl,
        a.ACTDT AS actdt,
        MFT AS mft,
        a.PERIOD AS period,
        a.TYPEID AS typeid,
        TYPCD AS m_type,
        DISPCODE AS dispcd,
        RPTCD AS rptcd,
        TC AS tc,
        a.CC AS cc,
        AMOUNT AS amount,
        'X' AS status  -- placeholder to avoid expensive subquery
    FROM ENT e,
         ENTACT a,
         TRANTRAIL b,
         TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
    WHERE e.TINSID = a.ACTSID
      AND e.TINSID = b.TINSID
      AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
      AND b.org = :org
      AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
);


-- ============================================================================
-- SECTION 6: CHECK IF LEGACY USES LEFT JOIN SEMANTICS
-- The legacy AVIEW accessed TRANTRAIL only via correlated subqueries
-- (NVL(SELECT status...,'P')), meaning records WITHOUT matching TRANTRAIL
-- rows SURVIVED with STATUS='P'. The modern uses INNER JOIN, dropping them.
-- ============================================================================

-- DIAG 13: Simulate LEFT JOIN behavior — count with TRANTRAIL as optional
-- If this approaches 2650, the INNER JOIN is the root cause.
SELECT COUNT(*) AS diag13_left_join_simulation
FROM (
    SELECT
        a.TIN, a.ROID, a.AROID, a.ACTDT, a.PERIOD, a.TYPEID,
        a.CODE, a.CC, a.EXTRDT,
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) AS tpctrl,
        TYPCD AS m_type, DISPCODE AS dispcd, RPTCD, TC, AMOUNT, MFT,
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, MFT, a.PERIOD, a.TYPEID,
                         TYPCD, DISPCODE, RPTCD, TC, a.CC, AMOUNT
            ORDER BY a.EXTRDT DESC, a.ROID
        ) AS row_num
    FROM ENT e,
         ENTACT a,
         TABLE(mft_ind_vals(a.ACTSID, e.tinfs)) c
         -- NOTE: No TRANTRAIL join at all!
    WHERE e.TINSID = a.ACTSID
      AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
) WHERE row_num = 1;

-- DIAG 13B: Alternative — if mft_ind_vals needs b.tinsid, try with LEFT JOIN
SELECT COUNT(*) AS diag13b_explicit_left_join
FROM (
    SELECT
        a.TIN, a.ROID, a.AROID, a.ACTDT, a.PERIOD, a.TYPEID,
        a.CODE, a.CC, a.EXTRDT,
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) AS tpctrl,
        TYPCD AS m_type, DISPCODE AS dispcd, RPTCD, TC, AMOUNT, MFT,
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, MFT, a.PERIOD, a.TYPEID,
                         TYPCD, DISPCODE, RPTCD, TC, a.CC, AMOUNT
            ORDER BY a.EXTRDT DESC, a.ROID
        ) AS row_num
    FROM ENT e
    INNER JOIN ENTACT a ON e.TINSID = a.ACTSID
    LEFT JOIN TRANTRAIL b ON e.TINSID = b.TINSID AND b.org = :org,
    TABLE(mft_ind_vals(NVL(b.tinsid, a.ACTSID), e.tinfs)) c
    WHERE DECODE(NVL(b.segind,' '), 'A', 1, 'C', 1, 'I', 1, ' ', 1, 0) = DECODE(mft, 0, 0, 1)
      AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
) WHERE row_num = 1;


-- ============================================================================
-- SECTION 7: SAMPLE THE MISSING RECORDS
-- Get a sample of ENTACT records that exist but get dropped by TRANTRAIL join.
-- ============================================================================

-- DIAG 14: Show 20 sample ENTACT records with no matching TRANTRAIL
SELECT a.ACTSID, a.TIN, a.ROID, a.AROID, a.ACTDT, a.CODE, a.PERIOD
FROM ENT e,
     ENTACT a
WHERE e.TINSID = a.ACTSID
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID
        AND b.org = :org
  )
FETCH FIRST 20 ROWS ONLY;

-- DIAG 15: For the missing records, check what org values exist in TRANTRAIL
SELECT b.org, COUNT(*) AS trail_count
FROM ENT e,
     ENTACT a,
     TRANTRAIL b
WHERE e.TINSID = a.ACTSID
  AND b.TINSID = e.TINSID
  AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
  AND SYSDATE - a.actdt <= :daysUpperLimit
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b2
      WHERE b2.TINSID = e.TINSID
        AND b2.org = :org
  )
GROUP BY b.org
ORDER BY trail_count DESC;


-- ============================================================================
-- SECTION 8: VERIFY AGAINST LEGACY COUNT
-- Run the legacy-equivalent query to confirm it produces 2650.
-- ============================================================================

-- DIAG 16: Legacy-style query (ENT + ENTACT only, TRANTRAIL via subqueries)
-- This mimics the legacy AVIEW structure where TRANTRAIL is NOT in FROM.
-- STATUS and ASSNRO come from correlated subqueries with NVL defaults.
SELECT COUNT(*) AS diag16_legacy_style_count
FROM (
    SELECT
        a.TIN, a.ROID, a.AROID, a.ACTDT, a.PERIOD, a.TYPEID,
        a.CODE, a.CC, a.EXTRDT,
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) AS tpctrl,
        a.ACTDT AS actdt,
        NVL(
            (SELECT status FROM (
                SELECT c.status,
                       ROW_NUMBER() OVER (ORDER BY c.EXTRDT DESC, c.ROWID) AS rn
                FROM TRANTRAIL c
                WHERE c.tinsid = a.actsid AND c.roid = a.roid
                  AND c.EXTRDT = (
                      SELECT NVL(MAX(d.EXTRDT), TO_DATE('01/01/1900','mm/dd/yyyy'))
                      FROM TRANTRAIL d
                      WHERE d.TINSID = c.TINSID AND d.ROID = c.ROID)
            ) WHERE rn = 1), 'P') AS status,
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, a.PERIOD, a.TYPEID, a.CC,
                         NVL(
                            (SELECT status FROM (
                                SELECT c2.status,
                                       ROW_NUMBER() OVER (ORDER BY c2.EXTRDT DESC, c2.ROWID) AS rn
                                FROM TRANTRAIL c2
                                WHERE c2.tinsid = a.actsid AND c2.roid = a.roid
                                  AND c2.EXTRDT = (
                                      SELECT NVL(MAX(d2.EXTRDT), TO_DATE('01/01/1900','mm/dd/yyyy'))
                                      FROM TRANTRAIL d2
                                      WHERE d2.TINSID = c2.TINSID AND d2.ROID = c2.ROID)
                            ) WHERE rn = 1), 'P')
            ORDER BY a.EXTRDT DESC, a.ROID
        ) AS row_num
    FROM ENT e,
         ENTACT a
    WHERE e.TINSID = a.ACTSID
      AND TRUNC(a.roid / POWER(10, 8 - :elevel)) = :levelValue
      AND SYSDATE - a.actdt <= :daysUpperLimit
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
) WHERE row_num = 1;


-- ============================================================================
-- SECTION 9: QUICK SUMMARY TABLE
-- After running all diagnostics, fill in this table:
-- ============================================================================
/*
+--------+----------------------------------------------+-------+-------+
| DIAG # | DESCRIPTION                                  | COUNT | DELTA |
+--------+----------------------------------------------+-------+-------+
|      1 | ENTACT only (date + roid filters)             |       |       |
|      2 | + ENT join                                   |       |       |
|      3 | + TRANTRAIL (no filters on b)                |       |       |
|      4 | + b.org = :org                               |       |       |
|      5 | + segind/mft DECODE + mft_ind_vals            |       |       |
|      6 | Pre-dedup (same as 5)                        |       |       |
|      7 | Post-dedup (ROW_NUMBER = 1) — expect 1853    |       |       |
|      8 | ENTACT with NO matching TRANTRAIL             |       | <<<   |
|      9 | Has TRANTRAIL but no matching segind/mft      |       | <<<   |
|     10 | TRANTRAIL rows per TINSID distribution        | (tbl) |       |
|     11 | mft_ind_vals rows per TINSID                  | (tbl) |       |
|     12 | Pre vs post dedup (partition key cardinality) |       |       |
|     13 | LEFT JOIN simulation (no TRANTRAIL req)       |       | <<<   |
|    13B | Explicit LEFT JOIN TRANTRAIL                  |       | <<<   |
|     14 | Sample missing records (20 rows)              | (tbl) |       |
|     15 | Org values for missing TINSID records         | (tbl) |       |
|     16 | Legacy-style (ENT+ENTACT, no TRANTRAIL FROM)  |       | <<<   |
+--------+----------------------------------------------+-------+-------+

KEY INDICATORS:
- If DIAG 8 ≈ 797 → Root cause: INNER JOIN to TRANTRAIL dropping records
- If DIAG 13 ≈ 2650 → Confirms: legacy doesn't require TRANTRAIL match
- If DIAG 16 ≈ 2650 → Confirms: legacy structure uses subqueries, not FROM join
- If DIAG 12 shows big gap → Deduplication is also a factor
*/
