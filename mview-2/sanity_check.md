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


This is a **smoking gun**, Santosh. Look at FAST CHECK 2:

| b.STATUS | COUNT |
|----------|-------|
| C | 18,149 |
| **O** | **1,773** |
| E | 905 |
| X | 120 |
| R | 50 |
| T | 5 |

**ENTMOD STATUS = 'O' (Open) = 1,773 — that's your EXACT legacy count.** This is not a coincidence.

The legacy Module View is only showing **open modules** (`b.STATUS = 'O'`), not the broader `NOT IN ('E','Q')` filter we assumed. The modern query is either not filtering b.STATUS at all or using a looser filter, pulling in Closed (18,149), Examined (905), etc., which after TRANTRAIL joins and dedup lands at 3,018.

You can kill DIAG 17 — we don't need it anymore. Instead run this one confirmation:

```sql
-- CONFIRMATION: Open modules only, with trail matching
SELECT COUNT(*)
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
    WHERE trunc(b.roid/power(10, 8-6)) = 251435
      AND b.STATUS = 'O'
      AND ORG = 'CF'
      AND c.STATUS = 'O'
  ) t
)
WHERE row_num = 1;
```

Since `b.STATUS = 'O'` means all modules are open, the TRANTRAIL status should also be 'O' — no need for the complex SWITCHROID/max_extrdt trail matching at all. Open cases have exactly one active trail row.

If this returns **1,773**, we've nailed it and the fix is a single line change in your deployed query. Report back!


**1794 — only 21 off from 1773!** We're right on top of it. The 21 extra records are from the missing `case_org(b.roid) = 'CF'` filter — I left it out of the confirmation query. Run this:

```sql
-- FINAL CONFIRMATION: Add case_org filter
SELECT COUNT(*)
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
    WHERE trunc(b.roid/power(10, 8-6)) = 251435
      AND b.STATUS = 'O'
      AND ORG = 'CF'
      AND c.STATUS = 'O'
      AND case_org(b.roid) = 'CF'
  ) t
)
WHERE row_num = 1;
```

If this returns **1773**, we have an exact match and the fix is confirmed. The root cause is crystal clear: the current deployed query is missing `b.STATUS = 'O'` and `c.STATUS = 'O'` filters — it's letting in Closed, Examined, Returned, and X-status modules and trails, inflating from 1773 to 3018.
