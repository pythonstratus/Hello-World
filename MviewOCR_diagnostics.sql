-- ============================================================
-- MviewOCRByLevel Diagnostics — ORG='CF', elevel=8, levelValue=25143500
-- Run these in order. Each adds one filter back.
-- The query where the count drops to 0 tells you which filter is the culprit.
-- ============================================================

-- DIAG 1: Base join only — do rows exist at all for this ROID at level 8?
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500;

-- DIAG 2: Add ORG filter
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF';

-- DIAG 3: Add myorg filter (SUSPECT #1 — killed results last time)
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
  AND myorg = 'CF';

-- DIAG 4: Add case_org filter
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
  AND myorg = 'CF'
  AND case_org(b.roid) = 'CF';

-- DIAG 5: Add TRANTRAIL status filter
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
  AND myorg = 'CF'
  AND case_org(b.roid) = 'CF'
  AND c.STATUS IN ('O', 'C', 'c', 'R');

-- DIAG 6: Add ENTMOD status filter
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
  AND myorg = 'CF'
  AND case_org(b.roid) = 'CF'
  AND c.STATUS IN ('O', 'C', 'c', 'R')
  AND b.STATUS NOT IN ('E', 'Q');

-- DIAG 7: Add trailmatch_new (SUSPECT #2 — returned dummy ROWIDs last time)
SELECT COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
  AND myorg = 'CF'
  AND case_org(b.roid) = 'CF'
  AND c.STATUS IN ('O', 'C', 'c', 'R')
  AND b.STATUS NOT IN ('E', 'Q')
  AND c.ROWID = CHARTOROWID(trailmatch_new(c.tinsid, b.roid, b.status, b.assnro, b.clsdt, 'CF'));

-- DIAG 8: Check what myorg values actually exist for this data
SELECT myorg, COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
GROUP BY myorg
ORDER BY COUNT(*) DESC;

-- DIAG 9: Check what TRANTRAIL statuses exist
SELECT c.STATUS, COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
GROUP BY c.STATUS
ORDER BY COUNT(*) DESC;

-- DIAG 10: Check what ENTMOD statuses exist
SELECT b.STATUS, COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
GROUP BY b.STATUS
ORDER BY COUNT(*) DESC;

-- DIAG 11: Test trailmatch_new — does it return real or dummy ROWIDs?
SELECT COUNT(*),
       SUM(CASE WHEN c.ROWID = CHARTOROWID(trailmatch_new(c.tinsid, b.roid, b.status, b.assnro, b.clsdt, 'CF'))
                THEN 1 ELSE 0 END) as matched
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-8)) = 25143500
  AND ORG = 'CF'
  AND c.STATUS IN ('O', 'C', 'c', 'R')
  AND b.STATUS NOT IN ('E', 'Q')
  AND ROWNUM <= 100;
