-- ============================================================
-- VERIFICATION: Corrected MviewOCRByLevel count
-- Expected: 1773 (exact match to legacy for GM 251435)
-- ============================================================
SELECT COUNT(*) as CORRECTED_COUNT
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
    INNER JOIN (
      SELECT c2.*,
             ROW_NUMBER() OVER (PARTITION BY c2.TINSID ORDER BY c2.EXTRDT DESC, c2.ROWID DESC) as trail_rn
      FROM TRANTRAIL c2
      WHERE c2.STATUS = 'O'
        AND c2.ORG = 'CF'
    ) c ON a.TINSID = c.TINSID AND c.trail_rn = 1
    INNER JOIN ENTMOD b ON a.TINSID = b.EMODSID
    WHERE trunc(b.roid/power(10, 8-6)) = 251435
      AND b.STATUS = 'O'
      AND case_org(b.roid) = 'CF'
  ) t
)
WHERE row_num = 1;
