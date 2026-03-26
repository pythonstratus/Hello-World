Same pattern — 5 records dropped by Java post-filtering. Quick way to confirm which filter is killing them:

Run this to see the STATUS distribution for that GM:

```sql
SELECT status, COUNT(*) AS cnt
FROM (
    SELECT
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
      AND a.roid BETWEEN 26133700 AND 26133799
      AND SYSDATE - a.actdt <= 90
      AND EXTRACT(YEAR FROM a.period) > 1901
      AND (   (     a.aroid BETWEEN 21011000 AND 35165899
                AND MOD(a.aroid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.aroid, 10000) BETWEEN 1000 AND 5899)
          OR  (     a.roid BETWEEN 21011000 AND 35165899
                AND MOD(a.roid, 1000000) BETWEEN 10000 AND 169999
                AND MOD(a.roid, 10000) BETWEEN 1000 AND 5899))
) WHERE row_num = 1
GROUP BY status
ORDER BY cnt DESC;
```

If you see `P = 5`, that confirms the `"P"` enum fix hasn't been deployed for this test yet. If all statuses are O/C/R/Q, then the second Java filter (reportCode D/T/R) is dropping 5 records with a different RPTCD value.
