-- ============================================================
-- Quick validation: test the fixed WHERE clause for count
-- Expected: somewhere between 19 (Open only) and ~950 (all valid)
-- ============================================================

-- STEP 1: Test SWITCHROID matching + no myorg (should return rows)
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
  AND ORG = 'CF'
  AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
  AND b.STATUS NOT IN ('E', 'Q');

-- STEP 2: Add max_extrdt filter (should reduce count — picks latest trail per case)
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
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

-- STEP 3: Add case_org (final filter)
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
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
