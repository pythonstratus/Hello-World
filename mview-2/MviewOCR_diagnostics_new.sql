-- ============================================================================
-- MODULE VIEW DIAGNOSTICS — MviewOCRByLevel.sql
-- ============================================================================
-- Legacy count:  1773  (Group Manager 251435)
-- Modern count:  3018
-- Surplus:       1245  (modern is OVER-counting)
--
-- Bind variable substitutions (hardcoded for diagnostics):
--   :org        = 'CF'
--   :elevel     = 6
--   :levelValue = 251435
--
-- Run each DIAG in order. Record the count. The deltas between DIAGs
-- will tell us exactly which filter/join is responsible for the surplus.
-- ============================================================================


-- ============================================================================
-- SECTION A: UNDERSTAND THE CURRENT QUERY'S JOIN MULTIPLICATION
-- ============================================================================

-- DIAG 1: How many ENT records match the level filter?
-- This is our base population size.
-- ============================================================================
SELECT 'DIAG 1: ENT base population' as DIAGNOSTIC, COUNT(*) as CNT
FROM ENT a
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435;


-- DIAG 2: How many ENTMOD records per ENT (multiplier check)?
-- If avg > 1, ENTMOD join is multiplying rows. Each module type (TDA_A, TDA_B,
-- TDI_F, OI, etc.) creates a separate row. This is EXPECTED for Module View.
-- But how many modules per entity?
-- ============================================================================
SELECT 'DIAG 2: ENTMOD multiplier' as DIAGNOSTIC,
       COUNT(*) as TOTAL_ENTMOD_ROWS,
       COUNT(DISTINCT a.TINSID) as DISTINCT_ENTITIES,
       ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT a.TINSID), 0), 2) as AVG_MODULES_PER_ENTITY
FROM ENT a
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435;


-- DIAG 3: ENTMOD b.STATUS distribution — what statuses exist?
-- Legacy filters: b.STATUS NOT IN ('E', 'Q')
-- Current query: NO filter on b.STATUS
-- If E/Q records exist, they're inflating the modern count.
-- ============================================================================
SELECT 'DIAG 3: ENTMOD STATUS distribution' as DIAGNOSTIC,
       b.STATUS as ENTMOD_STATUS,
       COUNT(*) as CNT
FROM ENT a
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
GROUP BY b.STATUS
ORDER BY COUNT(*) DESC;


-- DIAG 4: Impact of adding b.STATUS NOT IN ('E', 'Q')
-- Compare this to DIAG 1. The difference = records being included
-- that the legacy would exclude.
-- ============================================================================
SELECT 'DIAG 4: After b.STATUS filter' as DIAGNOSTIC, COUNT(*) as CNT
FROM ENT a
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND b.STATUS NOT IN ('E', 'Q');


-- ============================================================================
-- SECTION B: TRANTRAIL JOIN & TRAIL MATCHING
-- ============================================================================

-- DIAG 5: TRANTRAIL join — how many rows after inner join?
-- Multiple TRANTRAIL rows per TINSID means row multiplication BEFORE
-- trailmatch_new filters them down.
-- ============================================================================
SELECT 'DIAG 5: After TRANTRAIL join (no trail filter)' as DIAGNOSTIC,
       COUNT(*) as TOTAL_ROWS,
       COUNT(DISTINCT a.TINSID) as DISTINCT_ENTITIES,
       COUNT(DISTINCT c.ROWID) as DISTINCT_TRAILS
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND b.STATUS NOT IN ('E', 'Q');


-- DIAG 6: TRANTRAIL c.STATUS distribution
-- Legacy allows: 'O', 'C', 'c', 'R'
-- Current query: 'O', 'C', 'R' (missing lowercase 'c')
-- ============================================================================
SELECT 'DIAG 6: TRANTRAIL STATUS distribution' as DIAGNOSTIC,
       c.STATUS as TRAIL_STATUS,
       COUNT(*) as CNT
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND b.STATUS NOT IN ('E', 'Q')
  AND ORG = 'CF'
GROUP BY c.STATUS
ORDER BY COUNT(*) DESC;


-- DIAG 7: trailmatch_new matching rate
-- How many rows does trailmatch_new match vs reject?
-- If trailmatch_new matches MORE than SWITCHROID would, that's over-counting.
-- ============================================================================
SELECT 'DIAG 7: trailmatch_new match rate' as DIAGNOSTIC,
       COUNT(*) as TOTAL_ROWS,
       SUM(CASE WHEN c.ROWID = CHARTOROWID(trailmatch_new(c.tinsid,
                                                           b.roid,
                                                           b.status,
                                                           b.assnro,
                                                           b.clsdt,
                                                           'CF'))
                THEN 1 ELSE 0 END) as TRAILMATCH_MATCHED,
       SUM(CASE WHEN c.ROWID != CHARTOROWID(trailmatch_new(c.tinsid,
                                                            b.roid,
                                                            b.status,
                                                            b.assnro,
                                                            b.clsdt,
                                                            'CF'))
                THEN 1 ELSE 0 END) as TRAILMATCH_REJECTED
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND b.STATUS NOT IN ('E', 'Q')
  AND ORG = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R');


-- DIAG 8: SWITCHROID matching rate (legacy approach)
-- Compare this count to DIAG 7 matched count.
-- If SWITCHROID < trailmatch_new, trailmatch_new is too permissive.
-- ============================================================================
SELECT 'DIAG 8: SWITCHROID match rate' as DIAGNOSTIC,
       COUNT(*) as SWITCHROID_MATCHED
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND b.STATUS NOT IN ('E', 'Q')
  AND ORG = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID));


-- ============================================================================
-- SECTION C: THE MISSING LEGACY FILTER — max_extrdt
-- ============================================================================

-- DIAG 9: Impact of max_extrdt DECODE filter (with SWITCHROID matching)
-- Legacy: DECODE(c.STATUS, 'O', 1, 'C', (c.extrdt - max_extrdt(...)), ...) >= 0
-- This ensures only the LATEST trail extraction row per case is kept.
-- Without this, closed cases with multiple trail rows ALL appear.
-- ============================================================================
SELECT 'DIAG 9: SWITCHROID + max_extrdt filter' as DIAGNOSTIC,
       COUNT(*) as CNT
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
  AND DECODE(c.STATUS,
        'O', 1,
        'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        -1) >= 0
  AND ORG = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND b.STATUS NOT IN ('E', 'Q');


-- DIAG 10: Add case_org filter (on top of DIAG 9)
-- This is the full legacy WHERE clause for the ENTMOD branch.
-- ============================================================================
SELECT 'DIAG 10: + case_org filter (full legacy WHERE)' as DIAGNOSTIC,
       COUNT(*) as CNT
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
  AND DECODE(c.STATUS,
        'O', 1,
        'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        -1) >= 0
  AND ORG = 'CF'
  AND case_org(b.roid) = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND b.STATUS NOT IN ('E', 'Q');


-- ============================================================================
-- SECTION D: mft_ind_vals PIPELINED FUNCTION MULTIPLIER
-- ============================================================================

-- DIAG 11: Does mft_ind_vals multiply rows?
-- The TABLE(mft_ind_vals(c.tinsid, a.tinfs)) pipelined function in FROM
-- could return >1 row per entity, multiplying the result set.
-- ============================================================================
SELECT 'DIAG 11: mft_ind_vals multiplier' as DIAGNOSTIC,
       COUNT(*) as TOTAL_WITH_PIPELINED,
       COUNT(DISTINCT a.TINSID || '-' || b.EMODSID || '-' || b.MFT) as DISTINCT_COMBOS
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
, TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
  AND DECODE(c.STATUS,
        'O', 1,
        'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
        -1) >= 0
  AND ORG = 'CF'
  AND case_org(b.roid) = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND b.STATUS NOT IN ('E', 'Q');


-- ============================================================================
-- SECTION E: ROW_NUMBER() DEDUPLICATION
-- ============================================================================

-- DIAG 12: Final count after ROW_NUMBER() dedup (should match legacy ENTMOD branch)
-- This is the full ENTMOD branch with all legacy filters + dedup.
-- ============================================================================
SELECT 'DIAG 12: After ROW_NUMBER dedup (ENTMOD branch final)' as DIAGNOSTIC,
       COUNT(*) as CNT
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  FROM (
    SELECT
      TIN, c.TINSID, b.ROID as M_ROID, c.ROID,
      TPCTRL, MFT, PERIOD, TYPE, BALANCE, TOTASSD,
      b.ASSNRO as M_ASSNRO,
      TO_NUMBER(NVL(a.LFIIND, 0)) as LFIIND,
      NVL(FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OIASED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE ASED END) as ASED,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OICSED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE CSED END) as CSED
    FROM ENT a
    INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    , TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
    WHERE trunc(b.roid/power(10, 8-6)) = 251435
      AND to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
      AND DECODE(c.STATUS,
            'O', 1,
            'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            -1) >= 0
      AND ORG = 'CF'
      AND case_org(b.roid) = 'CF'
      AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
      AND b.STATUS NOT IN ('E', 'Q')
  ) t
)
WHERE row_num = 1;


-- ============================================================================
-- SECTION F: COMPARE CURRENT QUERY vs FIXED QUERY
-- ============================================================================

-- DIAG 13: Current query count (as-is from the deployed code)
-- This should return ~3018 (your reported modern count)
-- ============================================================================
SELECT 'DIAG 13: Current query (as-is)' as DIAGNOSTIC, COUNT(*) as CNT
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  FROM (
    SELECT
      TIN, c.TINSID, b.ROID as M_ROID, c.ROID,
      TPCTRL, MFT, PERIOD, TYPE, BALANCE, TOTASSD,
      b.ASSNRO as M_ASSNRO,
      TO_NUMBER(NVL(a.LFIIND, 0)) as LFIIND,
      NVL(FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OIASED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE ASED END) as ASED,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OICSED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE CSED END) as CSED
    FROM ENT a
    INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    , TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
    WHERE c.ROWID = CHARTOROWID(trailmatch_new(c.tinsid,
                                                b.roid,
                                                b.status,
                                                b.assnro,
                                                b.clsdt,
                                                'CF'))
      AND ORG = 'CF'
      AND case_org(b.roid) = 'CF'
      AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'R')
      AND trunc(b.roid/power(10, 8-6)) = 251435
  ) t
)
WHERE row_num = 1;


-- ============================================================================
-- SECTION G: ISOLATE EACH MISSING FILTER'S INDIVIDUAL IMPACT
-- ============================================================================

-- DIAG 14: Current query + ONLY add b.STATUS NOT IN ('E','Q')
-- Delta from DIAG 13 = records with ENTMOD status E or Q
-- ============================================================================
SELECT 'DIAG 14: Current + b.STATUS filter only' as DIAGNOSTIC, COUNT(*) as CNT
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  FROM (
    SELECT
      TIN, c.TINSID, b.ROID as M_ROID, c.ROID,
      TPCTRL, MFT, PERIOD, TYPE, BALANCE, TOTASSD,
      b.ASSNRO as M_ASSNRO,
      TO_NUMBER(NVL(a.LFIIND, 0)) as LFIIND,
      NVL(FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OIASED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE ASED END) as ASED,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OICSED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE CSED END) as CSED
    FROM ENT a
    INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    , TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
    WHERE c.ROWID = CHARTOROWID(trailmatch_new(c.tinsid,
                                                b.roid,
                                                b.status,
                                                b.assnro,
                                                b.clsdt,
                                                'CF'))
      AND ORG = 'CF'
      AND case_org(b.roid) = 'CF'
      AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'R')
      AND b.STATUS NOT IN ('E', 'Q')
      AND trunc(b.roid/power(10, 8-6)) = 251435
  ) t
)
WHERE row_num = 1;


-- DIAG 15: Current query + ONLY add lowercase 'c' STATUS
-- Delta from DIAG 13 = records with lowercase 'c' status
-- ============================================================================
SELECT 'DIAG 15: Current + lowercase c STATUS' as DIAGNOSTIC, COUNT(*) as CNT
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  FROM (
    SELECT
      TIN, c.TINSID, b.ROID as M_ROID, c.ROID,
      TPCTRL, MFT, PERIOD, TYPE, BALANCE, TOTASSD,
      b.ASSNRO as M_ASSNRO,
      TO_NUMBER(NVL(a.LFIIND, 0)) as LFIIND,
      NVL(FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OIASED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE ASED END) as ASED,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OICSED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE CSED END) as CSED
    FROM ENT a
    INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    , TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
    WHERE c.ROWID = CHARTOROWID(trailmatch_new(c.tinsid,
                                                b.roid,
                                                b.status,
                                                b.assnro,
                                                b.clsdt,
                                                'CF'))
      AND ORG = 'CF'
      AND case_org(b.roid) = 'CF'
      AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
      AND trunc(b.roid/power(10, 8-6)) = 251435
  ) t
)
WHERE row_num = 1;


-- ============================================================================
-- SECTION H: TRAIL MATCHING COMPARISON — trailmatch_new vs SWITCHROID
-- ============================================================================

-- DIAG 16: Replace trailmatch_new with SWITCHROID (keep all other current filters)
-- If this count is significantly different from DIAG 13, the trail matching
-- strategy itself is the problem.
-- ============================================================================
SELECT 'DIAG 16: SWITCHROID instead of trailmatch_new' as DIAGNOSTIC, COUNT(*) as CNT
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  FROM (
    SELECT
      TIN, c.TINSID, b.ROID as M_ROID, c.ROID,
      TPCTRL, MFT, PERIOD, TYPE, BALANCE, TOTASSD,
      b.ASSNRO as M_ASSNRO,
      TO_NUMBER(NVL(a.LFIIND, 0)) as LFIIND,
      NVL(FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OIASED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE ASED END) as ASED,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OICSED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE CSED END) as CSED
    FROM ENT a
    INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    , TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
    WHERE to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
      AND ORG = 'CF'
      AND case_org(b.roid) = 'CF'
      AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'R')
      AND trunc(b.roid/power(10, 8-6)) = 251435
  ) t
)
WHERE row_num = 1;


-- DIAG 17: SWITCHROID + max_extrdt + b.STATUS filter (full legacy filters)
-- This should be close to legacy ENTMOD-branch count.
-- ============================================================================
SELECT 'DIAG 17: SWITCHROID + max_extrdt + b.STATUS (full legacy)' as DIAGNOSTIC,
       COUNT(*) as CNT
FROM (
  SELECT t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  FROM (
    SELECT
      TIN, c.TINSID, b.ROID as M_ROID, c.ROID,
      TPCTRL, MFT, PERIOD, TYPE, BALANCE, TOTASSD,
      b.ASSNRO as M_ASSNRO,
      TO_NUMBER(NVL(a.LFIIND, 0)) as LFIIND,
      NVL(FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OIASED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE ASED END) as ASED,
      (CASE WHEN TYPE IN ('O','N') THEN NVL(OICSED, to_date('01/01/1900','mm/dd/yyyy'))
            ELSE CSED END) as CSED
    FROM ENT a
    INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    , TABLE(mft_ind_vals(c.tinsid, a.tinfs)) d
    WHERE to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
      AND DECODE(c.STATUS,
            'O', 1,
            'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            -1) >= 0
      AND ORG = 'CF'
      AND case_org(b.roid) = 'CF'
      AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
      AND b.STATUS NOT IN ('E', 'Q')
      AND trunc(b.roid/power(10, 8-6)) = 251435
  ) t
)
WHERE row_num = 1;


-- ============================================================================
-- SECTION I: QUICK REFERENCE — EXPECTED RESULTS MAP
-- ============================================================================
/*
  EXPECTED FLOW (based on our previous Module View diagnostics):

  DIAG 1  = Large number (all ENTMOD rows for GM 251435)
  DIAG 3  = Shows E/Q records exist → these are inflating count
  DIAG 4  = DIAG 1 minus E/Q records
  DIAG 5  = Much larger (TRANTRAIL multiplies rows)
  DIAG 7  = trailmatch_new matches (current approach)
  DIAG 8  = SWITCHROID matches (legacy approach) — compare to DIAG 7
  DIAG 9  = After max_extrdt filter — biggest drop expected here
  DIAG 10 = After case_org — final ENTMOD branch base
  DIAG 11 = After mft_ind_vals — check if pipelined fn multiplies
  DIAG 12 = After ROW_NUMBER dedup — ENTMOD branch final count
  DIAG 13 = ~3018 (current deployed query, confirms the problem)
  DIAG 14 = DIAG 13 minus E/Q records (first fix impact)
  DIAG 16 = SWITCHROID replacement (trail matching fix impact)
  DIAG 17 = Full legacy filters → should be close to 1773
            (or the ENTMOD branch portion of 1773 if QUEUE branch
             records are part of legacy count)

  KEY DELTAS TO WATCH:
  - DIAG 13 → DIAG 14 = Impact of b.STATUS NOT IN ('E','Q')
  - DIAG 13 → DIAG 16 = Impact of switching trail matching strategy
  - DIAG 16 → DIAG 17 = Impact of max_extrdt filter
  - DIAG 12 vs DIAG 17 = Should be very close (both use full legacy filters)
  - If DIAG 17 < 1773, the gap is QUEUE branch records (need UNION ALL)
*/
