-- ============================================================================
-- ACTIVITY VIEW DIAGNOSTIC SCRIPTS — GM 251435 (CORRECTED)
-- AViewCFByLevel.sql: Legacy=2650, Modern=1853, Gap=797
-- Corrected params: org='CF', elevel=6, levelValue=251435, daysUpperLimit=90
-- ROID filter: TRUNC(a.roid/100) = 251435  →  a.roid BETWEEN 25143500 AND 25143599
-- ============================================================================
-- RUN EACH IN ORDER. Record the count. Fill in the summary table at the end.
-- ============================================================================


-- ============================================================================
-- DIAG 1: ENTACT alone (date + roid + MOD filters, no joins)
-- The total universe of activity records for this GM.
-- ============================================================================
SELECT COUNT(*) AS diag1_entact_only
FROM ENTACT a
WHERE a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));


-- ============================================================================
-- DIAG 2: ENTACT + ENT join (still no TRANTRAIL)
-- ============================================================================
SELECT COUNT(*) AS diag2_entact_plus_ent
FROM ENT e,
     ENTACT a
WHERE e.TINSID = a.ACTSID
  AND a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));


-- ============================================================================
-- DIAG 3: + TRANTRAIL inner join (TINSID only, no other b filters)
-- Shows the cartesian expansion from TRANTRAIL.
-- ============================================================================
SELECT COUNT(*) AS diag3_trantrail_no_filter
FROM ENT e,
     ENTACT a,
     TRANTRAIL b
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));


-- ============================================================================
-- DIAG 4: + b.org = 'CF' filter on TRANTRAIL
-- ============================================================================
SELECT COUNT(*) AS diag4_trantrail_org_cf
FROM ENT e,
     ENTACT a,
     TRANTRAIL b
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND b.org = 'CF'
  AND a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));


-- ============================================================================
-- DIAG 5: + segind/mft DECODE + mft_ind_vals table function
-- Full modern WHERE clause BEFORE dedup.
-- ============================================================================
SELECT COUNT(*) AS diag5_full_where_pre_dedup
FROM ENT e,
     ENTACT a,
     TRANTRAIL b,
     TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
  AND b.org = 'CF'
  AND a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));


-- ============================================================================
-- DIAG 6: Post-dedup (ROW_NUMBER = 1) — should give 1853
-- Simplified partition (no STATUS subquery) to run fast.
-- ============================================================================
SELECT COUNT(*) AS diag6_post_dedup
FROM (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, MFT, a.PERIOD, a.TYPEID,
                         TYPCD, DISPCODE, RPTCD, TC, a.CC, AMOUNT
            ORDER BY a.EXTRDT DESC, b.ASSNRO DESC, a.ROID
        ) AS row_num
    FROM ENT e,
         ENTACT a,
         TRANTRAIL b,
         TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
    WHERE e.TINSID = a.ACTSID
      AND e.TINSID = b.TINSID
      AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
      AND b.org = 'CF'
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


-- ============================================================================
-- DIAG 7: ***CRITICAL*** ENTACT records with ZERO matching TRANTRAIL for CF
-- If this ≈ 797, THIS IS THE ROOT CAUSE.
-- ============================================================================
SELECT COUNT(*) AS diag7_entact_no_trantrail_cf
FROM ENT e,
     ENTACT a
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
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID
        AND b.org = 'CF'
  );


-- ============================================================================
-- DIAG 8: Has TRANTRAIL for CF but no matching segind/mft combo
-- ============================================================================
SELECT COUNT(*) AS diag8_has_trail_no_segind_match
FROM ENT e,
     ENTACT a
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
  AND EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID AND b.org = 'CF'
  )
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID
        AND b.org = 'CF'
        AND b.segind IN ('A', 'C', 'I')
  );


-- ============================================================================
-- DIAG 9: TRANTRAIL rows-per-TINSID distribution
-- Shows how INNER JOIN multiplies or filters the result set.
-- ============================================================================
SELECT
    CASE
        WHEN trail_count = 0 THEN '0_NO_MATCH_DROPPED'
        WHEN trail_count = 1 THEN '1_single'
        WHEN trail_count BETWEEN 2 AND 5 THEN '2to5'
        WHEN trail_count BETWEEN 6 AND 20 THEN '6to20'
        ELSE '20plus'
    END AS trail_bucket,
    COUNT(*) AS entact_record_count,
    SUM(trail_count) AS total_joined_rows
FROM (
    SELECT a.ACTSID, a.ROWID AS act_rowid,
           (SELECT COUNT(*) FROM TRANTRAIL b
            WHERE b.TINSID = a.ACTSID AND b.org = 'CF') AS trail_count
    FROM ENTACT a
    WHERE a.roid BETWEEN 25143500 AND 25143599
      AND SYSDATE - a.actdt <= 90
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
)
GROUP BY CASE
        WHEN trail_count = 0 THEN '0_NO_MATCH_DROPPED'
        WHEN trail_count = 1 THEN '1_single'
        WHEN trail_count BETWEEN 2 AND 5 THEN '2to5'
        WHEN trail_count BETWEEN 6 AND 20 THEN '6to20'
        ELSE '20plus'
    END
ORDER BY 1;


-- ============================================================================
-- DIAG 10: Pre-dedup vs post-dedup cardinality
-- ============================================================================
SELECT
    COUNT(*) AS total_pre_dedup,
    COUNT(DISTINCT
        a.TIN || '|' || a.ROID || '|' || a.AROID || '|' ||
        DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl) || '|' ||
        TO_CHAR(a.ACTDT, 'YYYYMMDD') || '|' || MFT || '|' ||
        TO_CHAR(a.PERIOD, 'YYYYMMDD') || '|' || a.TYPEID || '|' ||
        TYPCD || '|' || DISPCODE || '|' || RPTCD || '|' ||
        TC || '|' || a.CC || '|' || AMOUNT
    ) AS distinct_partition_keys
FROM ENT e,
     ENTACT a,
     TRANTRAIL b,
     TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
  AND b.org = 'CF'
  AND a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899));


-- ============================================================================
-- DIAG 11: ***CRITICAL*** NO TRANTRAIL in FROM — legacy-style count
-- If this ≈ 2650, confirms INNER JOIN is root cause.
-- Uses a.ACTSID instead of b.tinsid for mft_ind_vals since b is absent.
-- ============================================================================
SELECT COUNT(*) AS diag11_no_trantrail_in_from
FROM (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY a.TIN, a.ROID, a.AROID,
                         DECODE(e.TPCTRL, 'E3', ' ', 'E7', ' ', e.tpctrl),
                         a.ACTDT, MFT, a.PERIOD, a.TYPEID,
                         TYPCD, RPTCD, TC, a.CC, AMOUNT
            ORDER BY a.EXTRDT DESC, a.ROID
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


-- ============================================================================
-- DIAG 12: For missing records, what ORG values exist in TRANTRAIL?
-- ============================================================================
SELECT NVL(b.org, '(NO_TRAIL)') AS trail_org, COUNT(*) AS cnt
FROM ENT e
INNER JOIN ENTACT a ON e.TINSID = a.ACTSID
LEFT JOIN TRANTRAIL b ON b.TINSID = e.TINSID
WHERE a.roid BETWEEN 25143500 AND 25143599
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b2
      WHERE b2.TINSID = e.TINSID AND b2.org = 'CF'
  )
GROUP BY NVL(b.org, '(NO_TRAIL)')
ORDER BY cnt DESC;


-- ============================================================================
-- DIAG 13: Sample 20 missing ENTACT records (no TRANTRAIL for CF)
-- ============================================================================
SELECT a.ACTSID, a.TIN, a.ROID, a.AROID, a.ACTDT, a.CODE, a.SUBCODE, a.PERIOD
FROM ENT e,
     ENTACT a
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
  AND NOT EXISTS (
      SELECT 1 FROM TRANTRAIL b
      WHERE b.TINSID = e.TINSID AND b.org = 'CF'
  )
FETCH FIRST 20 ROWS ONLY;


-- ============================================================================
-- DIAG 14: ***CRITICAL*** Full legacy-style query
-- TRANTRAIL only via correlated subqueries, NOT in FROM.
-- STATUS defaults to 'P' when no match. ASSNRO via subquery.
-- If count ≈ 2650, we have DEFINITIVE root cause + confirmation.
-- NOTE: May run slow due to correlated subqueries — be patient.
-- ============================================================================
SELECT COUNT(*) AS diag14_legacy_style_count
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
        NVL(
            (SELECT status FROM (
                SELECT c.status,
                       ROW_NUMBER() OVER (ORDER BY c.EXTRDT DESC, c.ROWID) AS rn
                FROM TRANTRAIL c
                WHERE c.tinsid = a.actsid AND c.roid = a.roid
                  AND c.EXTRDT = (
                      SELECT NVL(MAX(d.EXTRDT), TO_DATE('01/01/1900','mm/dd/yyyy'))
                      FROM TRANTRAIL d
                      WHERE d.TINSID = c.TINSID AND d.ROID = c.ROID
                        AND DECODE(d.segind, 'A', 1, 'C', 1, 'I', 1, 0) =
                            DECODE(mft, 0, 0, 1))
            ) WHERE rn = 1), 'P') AS status,
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


-- ============================================================================
-- DIAG 15: Is FETCH FIRST 2000 truncating?
-- Same as DIAG 6 but checking total count vs 2000 limit.
-- If count > 2000, the modern query's FETCH FIRST 2000 is silently cutting.
-- ============================================================================
-- (DIAG 6 already answers this — if DIAG 6 > 2000, that's the problem.)


-- ============================================================================
-- DIAG 16: Column ownership check — ENTACT vs TRANTRAIL
-- Unqualified columns in the modern query could resolve differently
-- depending on which tables are in FROM.
-- ============================================================================
SELECT 'ENTACT' AS source, column_name
FROM all_tab_columns
WHERE table_name = 'ENTACT'
  AND column_name IN ('DISPCODE','TYPCD','RPTCD','TC','AMOUNT','BODCD',
                       'MFT','TOTASSD','RTNSEC','GRPIND','FORM809',
                       'CASEIND','BAL_941_14','CNT_941_14','CNT_941',
                       'TDI_CNT_941','CCNIPSELECTCD','PRGNAME1','PRGNAME2',
                       'HINFIND','AGEIND','PDTIND','PYRIND','FATCAIND',
                       'FEDCONIND','FEDEMPIND','IRSEMPIND','L903','LLCIND',
                       'THEFTIND','INSPCIND','OICACCYR',
                       'DVICTCD','CCPHRS','TOTHRS','TOTTOUCH',
                       'CCPTOUCH','AGI_AMT','BAL_941','DT_OA','DT_POA',
                       'DT_DOD')
UNION ALL
SELECT 'TRANTRAIL' AS source, column_name
FROM all_tab_columns
WHERE table_name = 'TRANTRAIL'
  AND column_name IN ('DISPCODE','TYPCD','RPTCD','TC','AMOUNT','BODCD',
                       'MFT','TOTASSD','RTNSEC','GRPIND','FORM809',
                       'CASEIND','BAL_941_14','CNT_941_14','CNT_941',
                       'TDI_CNT_941','CCNIPSELECTCD','PRGNAME1','PRGNAME2',
                       'HINFIND','AGEIND','PDTIND','PYRIND','FATCAIND',
                       'FEDCONIND','FEDEMPIND','IRSEMPIND','L903','LLCIND',
                       'THEFTIND','INSPCIND','OICACCYR',
                       'DVICTCD','CCPHRS','TOTHRS','TOTTOUCH',
                       'CCPTOUCH','AGI_AMT','BAL_941','DT_OA','DT_POA',
                       'DT_DOD')
ORDER BY column_name, source;


-- ============================================================================
-- SUMMARY TABLE — Fill in after running all diagnostics
-- ============================================================================
/*
+--------+----------------------------------------------+---------+-------------------+
| DIAG # | WHAT IT MEASURES                             | COUNT   | INTERPRETATION    |
+--------+----------------------------------------------+---------+-------------------+
|      1 | ENTACT only (base universe)                  |         | Starting point    |
|      2 | + ENT join                                   |         | Should ≈ DIAG 1   |
|      3 | + TRANTRAIL (no b filters)                   |         | Expansion factor  |
|      4 | + b.org = 'CF'                               |         | Org filter impact |
|      5 | + segind/mft + mft_ind_vals                  |         | Full WHERE pre-dup|
|      6 | Post-dedup (ROW_NUMBER = 1)                  |         | Should ≈ 1853     |
|      7 | ENTACT with NO TRANTRAIL for CF              |         | *** ≈797? ***     |
|      8 | Has trail but no segind A/C/I match          |         | Secondary filter  |
|      9 | Trail rows per TINSID distribution            | (table) | Cardinality       |
|     10 | Pre vs post dedup cardinality                 |         | Dedup impact      |
|     11 | NO TRANTRAIL in FROM (legacy style count)     |         | *** ≈2650? ***    |
|     12 | ORG values for missing records                | (table) | Why no CF match   |
|     13 | Sample 20 missing records                     | (table) | Visual inspection |
|     14 | Legacy-style full query with subqueries       |         | *** ≈2650? ***    |
|     16 | Column ownership (ENTACT vs TRANTRAIL)        | (table) | Ambiguity check   |
+--------+----------------------------------------------+---------+-------------------+

DECISION TREE AFTER RESULTS:
═══════════════════════════

  If DIAG 7 ≈ 797 AND DIAG 11 ≈ 2650:
    ┌─────────────────────────────────────────────────────────────────┐
    │  ROOT CAUSE CONFIRMED:                                        │
    │  Modern query INNER JOINs TRANTRAIL in FROM clause.            │
    │  Legacy query NEVER puts TRANTRAIL in FROM — it only accesses  │
    │  TRANTRAIL via correlated subqueries with NVL defaults.        │
    │  ENTACT records without a CF TRANTRAIL match get STATUS='P'    │
    │  in legacy but are DROPPED ENTIRELY in modern.                 │
    │                                                                │
    │  FIX: Remove TRANTRAIL from FROM clause. Keep STATUS and       │
    │  ASSNRO as correlated subqueries with NVL defaults.            │
    │  OR: Convert to LEFT JOIN with NVL fallback for STATUS/ASSNRO. │
    └─────────────────────────────────────────────────────────────────┘

  If DIAG 7 ≈ 0 but DIAG 5 >> DIAG 6:
    → Dedup (ROW_NUMBER partition) too aggressive
    → Check partition column list vs legacy

  If DIAG 6 > 2000:
    → FETCH FIRST 2000 is truncating results
    → Increase or remove the fetch limit

  If DIAG 7 is small AND DIAG 11 ≈ DIAG 6:
    → Issue is elsewhere — check DIAG 16 for column ambiguity,
      or the elevel/daysUpperLimit values passed by Java
*/
