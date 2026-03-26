That's expected — DIAG 17 has four per-row PL/SQL function calls (SWITCHROID, max_extrdt, case_org, mft_ind_vals) which are all expensive. Let it run, but while it's cooking, run these fast lightweight checks so we're not wasting time:

```sql
-- FAST CHECK 1: Sanity — does the level filter find rows? (no PL/SQL, instant)
SELECT COUNT(*)
FROM ENTMOD b
WHERE trunc(b.roid/power(10, 8-6)) = 251435;

-- FAST CHECK 2: b.STATUS distribution (no PL/SQL, instant)
SELECT b.STATUS, COUNT(*)
FROM ENTMOD b
WHERE trunc(b.roid/power(10, 8-6)) = 251435
GROUP BY b.STATUS
ORDER BY COUNT(*) DESC;

-- FAST CHECK 3: TRANTRAIL multiplier — how many trail rows per entity?
SELECT COUNT(*) as TOTAL_ROWS,
       COUNT(DISTINCT a.TINSID) as DISTINCT_ENTITIES
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND ORG = 'CF'
  AND b.STATUS NOT IN ('E', 'Q');

-- FAST CHECK 4: c.STATUS distribution (which trail statuses exist?)
SELECT c.STATUS, COUNT(*)
FROM ENT a
INNER JOIN TRANTRAIL c ON a.TINSID = c.TINSID
INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
WHERE trunc(b.roid/power(10, 8-6)) = 251435
  AND ORG = 'CF'
  AND b.STATUS NOT IN ('E', 'Q')
GROUP BY c.STATUS
ORDER BY COUNT(*) DESC;
```

These four have zero PL/SQL calls so they should return in seconds. The numbers will tell us immediately how much row multiplication is happening from the TRANTRAIL join and how many E/Q records exist — so by the time DIAG 17 finishes, we'll already know the story.

Report back whatever comes first — these fast checks or DIAG 17.
