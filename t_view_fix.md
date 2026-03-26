The row multiplication is almost certainly coming from the `tour_cte` and `seid_cte` joins. The legacy view used **scalar subqueries** for SEID and TOUR, which always return exactly one value. When we converted those to JOINs, if `entemp` has multiple rows per `roid` matching the filter, each one multiplies your result set.

Two quick fixes — either deduplicate the CTEs or go back to scalar subqueries (which is what legacy actually did). The safest approach to match legacy exactly:

**tour_cte** — change to:
```sql
tour_cte AS (
    SELECT roid, MIN(tour) AS tour
    FROM entemp
    WHERE elevel > 0
      AND eactive IN ('A', 'Y')
    GROUP BY roid
),
```

**seid_cte** — change to:
```sql
seid_cte AS (
    SELECT roid, MIN(SEID) AS SEID
    FROM entemp
    WHERE eactive IN ('A', 'Y')
      AND elevel >= 0
    GROUP BY roid
),
```

The `MIN()` with `GROUP BY` guarantees one row per `roid`, same as a scalar subquery returning one value. Try that and see if you get 379.

If it's still over, the other suspect is `TABLE(mft_ind_vals(b.tinsid, a.tinfs))` returning multiple rows per tinsid — but that would have been an issue in legacy too. Let me know what the count looks like after the CTE fix.


Yes — just **replace** the existing `seid_cte` and `tour_cte` in the file with the deduplicated versions. So change:

```sql
seid_cte AS (
    SELECT roid, SEID
    FROM entemp
    WHERE eactive IN ('A', 'Y')
      AND elevel >= 0
),
```

to:

```sql
seid_cte AS (
    SELECT roid, MIN(SEID) AS SEID
    FROM entemp
    WHERE eactive IN ('A', 'Y')
      AND elevel >= 0
    GROUP BY roid
),
```

And change:

```sql
tour_cte AS (
    SELECT roid, tour
    FROM entemp
    WHERE elevel > 0
      AND eactive IN ('A', 'Y')
),
```

to:

```sql
tour_cte AS (
    SELECT roid, MIN(tour) AS tour
    FROM entemp
    WHERE elevel > 0
      AND eactive IN ('A', 'Y')
    GROUP BY roid
),
```

Same two CTEs, just adding `MIN()` and `GROUP BY roid` to guarantee one row per roid. That should stop the row multiplication.


## ----------

Same pattern — `TABLE(mft_ind_vals(a.tinsid, a.tinfs))` is also returning multiple rows per tinsid. And looking back at the legacy TVIEW screenshots, **it doesn't use `mft_ind_vals` either**. The legacy TVIEW is purely `FROM ENT a, TIMETIN tt` — nothing else.

Remove the `TABLE(mft_ind_vals(...))` from the FROM clause and default those columns too:

```sql
-- Replace these lines that reference "c.":
        0 AS BAL_941_14,               -- was c.BAL_941_14
        0 AS CNT_941_14,               -- was c.CNT_941_14
        0 AS CNT_941,                  -- was c.CNT_941
        0 AS TDI_CNT_941,             -- was c.TDI_CNT_941
        0 AS IND_941,                  -- was c.IND_941
        'No' AS FORMATTED_IND_941,     -- was CASE on c.ind_941
        0 AS BAL_941,                  -- was c.BAL_941
```

And in the FROM clause, remove the `TABLE(mft_ind_vals(...)) c` line entirely so it's just:

```sql
    FROM ENT a,
         filtered_timetin tt,
         seid_cte p,
         tour_cte tour_sub
```

That should bring you to 379. The legacy TimeView is literally just ENT joined to TIMETIN — every other table was added during modernization and was inflating row counts.
