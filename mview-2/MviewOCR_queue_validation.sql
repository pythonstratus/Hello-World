-- ============================================================
-- QUEUE Branch Validation — test independently before UNION ALL
-- Expected: ~1663 rows (1832 legacy total - 169 ENTMOD = ~1663)
-- ============================================================

-- STEP 1: Base count — how many QUEUE rows exist for this ROID?
SELECT COUNT(*)
FROM
    DIAL.COREDIAL a,
    DIAL.TINSUMMARY,
    DIAL.DIALENT,
    DIAL.DIALMOD
WHERE
    grnum = 70
    AND proid > 0
    AND CORESID = MODSID
    AND CORESID = ENTSID
    AND CORETIN = EMISTIN
    AND CORETT  = EMISTT
    AND COREFS  = EMISFS
    AND TINSUMMARY.RISK IS NOT NULL
    AND dis_vic NOT IN (2, 3)
    AND pen_ent_cd != 1
    AND pdc_id_cd = 0
    AND trunc(
        TO_NUMBER(TO_CHAR(ASSIGN_AO,'FM09')||TO_CHAR(ASSIGN_TO,'FM09')||7000)
        / power(10, 8 - 8)
    ) = 25143500;

-- STEP 2: Without pdc_id_cd filter (in case it over-filters)
SELECT COUNT(*)
FROM
    DIAL.COREDIAL a,
    DIAL.TINSUMMARY,
    DIAL.DIALENT,
    DIAL.DIALMOD
WHERE
    grnum = 70
    AND proid > 0
    AND CORESID = MODSID
    AND CORESID = ENTSID
    AND CORETIN = EMISTIN
    AND CORETT  = EMISTT
    AND COREFS  = EMISFS
    AND TINSUMMARY.RISK IS NOT NULL
    AND dis_vic NOT IN (2, 3)
    AND pen_ent_cd != 1
    AND trunc(
        TO_NUMBER(TO_CHAR(ASSIGN_AO,'FM09')||TO_CHAR(ASSIGN_TO,'FM09')||7000)
        / power(10, 8 - 8)
    ) = 25143500;

-- STEP 3: Check what data is in DIAL tables for this ROID (sanity check)
SELECT COUNT(*) as total_coredial
FROM DIAL.COREDIAL
WHERE grnum = 70
  AND proid > 0
  AND trunc(
      TO_NUMBER(TO_CHAR(ASSIGN_AO,'FM09')||TO_CHAR(ASSIGN_TO,'FM09')||7000)
      / power(10, 8 - 8)
  ) = 25143500;
