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
