-- ============================================================================
-- DATA COMPARISON: Verify column-level accuracy, not just counts
-- Pick a small sample and compare key columns between old and new query.
-- ============================================================================

-- STEP 1: Run the OLD query (with TRANTRAIL INNER JOIN) for a specific TIN
-- and capture the row. Then run the NEW query for the same TIN and compare.

-- OLD QUERY (original modern — with TRANTRAIL inner join)
-- This is what was returning 1853. Pick a specific TIN to compare.
SELECT
    a.TIN, a.ROID, a.AROID, a.ACTDT, a.CODE AS CASECODE,
    -- Key b. columns that might differ:
    b.segind AS b_segind,
    b.STATUS AS b_status,
    b.ASSNRO AS b_assnro,
    b.CLOSEDT AS b_closedt,
    b.PROID AS b_proid,
    b.INITDT AS b_initdt,
    b.XXDT AS b_xxdt,
    b.EMPHRS AS b_emphrs,
    b.FLDHRS AS b_fldhrs,
    b.EMPTOUCH AS b_emptouch,
    b.LSTTOUCH AS b_lsttouch,
    b.ASSNFLD AS b_assnfld,
    b.TDAcnt AS b_tdacnt,
    b.TDIcnt AS b_tdicnt,
    b.NAICSCD AS b_naicscd,
    b.ORG AS b_org,
    b.ZIPCDE AS b_zipcde,
    -- Computed columns that depend on b.:
    DECODE(b.segind, 'A', AGEIND, 'C', AGEIND, 'I', AGEIND, 'C') AS computed_ageind,
    DECODE(b.segind, 'A', RPTIND, 'C', RPTIND, 'I', RPTIND, 'F') AS computed_rptind,
    DECODE(b.segind, 'A', PYRIND, 'C', PYRIND, 'I', PYRIND, 0) AS computed_pyrind
FROM ENT e,
     ENTACT a,
     TRANTRAIL b,
     TABLE(mft_ind_vals(b.tinsid, e.tinfs)) c
WHERE e.TINSID = a.ACTSID
  AND e.TINSID = b.TINSID
  AND DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) = DECODE(mft, 0, 0, 1)
  AND b.org = 'CF'
  AND a.roid BETWEEN 26133700 AND 26133799
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
FETCH FIRST 50 ROWS ONLY;


-- ============================================================================
-- NEW QUERY (corrected — TRANTRAIL as outer join subquery)
-- Same sample — compare columns side by side
-- ============================================================================
SELECT
    a.TIN, a.ROID, a.AROID, a.ACTDT, a.CODE AS CASECODE,
    -- Key b. columns:
    b.segind AS b_segind,
    b.STATUS AS b_status,
    b.ASSNRO AS b_assnro,
    b.CLOSEDT AS b_closedt,
    b.PROID AS b_proid,
    b.INITDT AS b_initdt,
    b.XXDT AS b_xxdt,
    b.EMPHRS AS b_emphrs,
    b.FLDHRS AS b_fldhrs,
    b.EMPTOUCH AS b_emptouch,
    b.LSTTOUCH AS b_lsttouch,
    b.ASSNFLD AS b_assnfld,
    b.TDAcnt AS b_tdacnt,
    b.TDIcnt AS b_tdicnt,
    b.NAICSCD AS b_naicscd,
    b.ORG AS b_org,
    b.ZIPCDE AS b_zipcde,
    -- Computed columns:
    DECODE(NVL(b.segind, ' '), 'A', AGEIND, 'C', AGEIND, 'I', AGEIND, 'C') AS computed_ageind,
    DECODE(NVL(b.segind, ' '), 'A', RPTIND, 'C', RPTIND, 'I', RPTIND, 'F') AS computed_rptind,
    DECODE(NVL(b.segind, ' '), 'A', PYRIND, 'C', PYRIND, 'I', PYRIND, 0) AS computed_pyrind
FROM ENT e,
     ENTACT a,
     TABLE(mft_ind_vals(a.ACTSID, e.tinfs)) c,
     (SELECT tb.*,
             ROW_NUMBER() OVER (
                 PARTITION BY tb.TINSID
                 ORDER BY tb.EXTRDT DESC, tb.ROWID DESC
             ) AS trail_rn
      FROM TRANTRAIL tb
      WHERE tb.org = 'CF'
     ) b
WHERE e.TINSID = a.ACTSID
  AND a.ACTSID = b.TINSID (+)
  AND b.trail_rn (+) = 1
  AND a.roid BETWEEN 26133700 AND 26133799
  AND SYSDATE - a.actdt <= 90
  AND EXTRACT(YEAR FROM a.period) > 1901
  AND (   (     a.aroid BETWEEN 21011000 AND 35165899
            AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
      OR  (     a.roid BETWEEN 21011000 AND 35165899
            AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
            AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
FETCH FIRST 50 ROWS ONLY;


-- ============================================================================
-- STEP 2: QUICK CHECK — How many TINSID have multiple TRANTRAIL rows for CF?
-- If most have just 1, the row-selection difference doesn't matter.
-- If many have 2+, the data could differ for those.
-- ============================================================================
SELECT
    trail_count,
    COUNT(*) AS tinsid_count
FROM (
    SELECT a.ACTSID,
           (SELECT COUNT(*) FROM TRANTRAIL b
            WHERE b.TINSID = a.ACTSID AND b.org = 'CF') AS trail_count
    FROM (SELECT DISTINCT ACTSID FROM ENTACT
          WHERE roid BETWEEN 26133700 AND 26133799
            AND SYSDATE - actdt <= 90) a
)
GROUP BY trail_count
ORDER BY trail_count;


-- ============================================================================
-- STEP 3: For TINSID with MULTIPLE TRANTRAIL rows, compare which row
-- the OLD vs NEW query picks.
-- OLD: picks row matching DECODE(segind) = DECODE(mft) — could be multiple
-- NEW: picks single row by MAX(EXTRDT) regardless of segind
-- ============================================================================
SELECT b.TINSID, b.ROID, b.segind, b.STATUS, b.ASSNRO, b.EXTRDT, b.org,
       DECODE(b.segind, 'A', 1, 'C', 1, 'I', 1, 0) AS segind_flag
FROM TRANTRAIL b
WHERE b.TINSID IN (
    -- Pick a TINSID with multiple CF rows
    SELECT ACTSID FROM (
        SELECT a.ACTSID,
               (SELECT COUNT(*) FROM TRANTRAIL b
                WHERE b.TINSID = a.ACTSID AND b.org = 'CF') AS trail_count
        FROM (SELECT DISTINCT ACTSID FROM ENTACT
              WHERE roid BETWEEN 26133700 AND 26133799
                AND SYSDATE - actdt <= 90) a
    ) WHERE trail_count > 1
    FETCH FIRST 5 ROWS ONLY
)
AND b.org = 'CF'
ORDER BY b.TINSID, b.EXTRDT DESC;


-- ============================================================================
-- STEP 4: THE REAL QUESTION — What does the LEGACY system use?
-- The legacy AVIEW does NOT join TRANTRAIL in FROM at all.
-- So where do columns like CLOSEDT, PROID, EMPHRS, etc. actually come from
-- in the legacy? They must come from either:
--   (a) A different table entirely (ENTMOD? ENT?)
--   (b) Additional correlated subqueries we haven't seen
--   (c) The legacy view's b alias points to a DIFFERENT table
--
-- TO VERIFY: Check what table alias 'b' refers to in the legacy AVIEW
-- view definition. Run:
-- ============================================================================
SELECT text
FROM all_views
WHERE view_name = 'AVIEW'
  AND owner = 'ENTITYDEV';
-- Or if it's a different schema:
-- SELECT text FROM all_views WHERE view_name LIKE '%AVIEW%';
