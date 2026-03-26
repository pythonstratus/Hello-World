-- ============================================================================
-- Priority Queue Query - Optimized (Inline Subqueries, No CTEs)
-- Replaces per-row PL/SQL functions: INTLQPICK, ASSNQPICK, ASSNROID
-- with inline scalar subqueries on the ASSN table
-- ============================================================================
-- Key insight: INTLQPICK always queries area=35, territory=02 and returns
-- '3502'||grnum||ronum. ASSNQPICK then decomposes that back to area=35,
-- territory=02, grnum=X. So we skip the TO_NUMBER/TO_CHAR round-trip
-- and just use grnum directly.
-- ============================================================================

SELECT /*+ index(a, dial_roid_ix) index(b, emissid_ix) */
    '' AS queue_indicator,
    TO_DATE('01/01/1900', 'mm/dd/yyyy') AS date_assigned_queue,
    (CASE
        WHEN c.ind_941 = 0 AND b.hinfind = 0 THEN ''
        WHEN c.ind_941 = 1 THEN '941'
        WHEN b.hinfind = 1 THEN 'HINF'
        ELSE '941/HINF'
    END) AS hinf_941_ind,
    'Q' AS STATUS,
    'Queue' AS assignment_number,
    CORETIN AS TIN,
    CORETT,
    COREFS AS tinfs,
    (CASE COREFS
        WHEN 0 THEN 'No MF Designation'
        WHEN 1 THEN 'IMF'
        WHEN 2 THEN 'BMF'
        WHEN 3 THEN 'EPMF'
        WHEN 4 THEN 'IRAF'
        WHEN 6 THEN 'NMF'
        WHEN 9 THEN 'CAF'
        ELSE 'No MF Designation'
    END) AS tin_file_source,
    TINSID,
    (CASE
        WHEN TDACNT > 0 AND TDICNT > 0 THEN 'Combo'
        WHEN TDACNT > 0 AND TDICNT = 0 THEN 'TDA'
        WHEN TDICNT > 0 AND TDACNT = 0 THEN 'TDI'
        ELSE 'Other'
    END) AS CASE_TYPE,
    NACTL,
    SUBSTR(NATP, 1, 35) AS TAXPAYER_NAME,
    SUBSTR(NVL(ADTP, ' '), 1, 35) AS address,
    SUBSTR(NVL(ADTP2, ' '), 1, 35),
    RTRIM(NVL(CITP, ' ')) AS city,
    STTP AS state,
    TO_NUMBER(SUBSTR(TO_CHAR(ZPTP, '099999999999'), -12, 5)) AS zipcode,
    NVL(TO_NUMBER(SUBSTR(TO_CHAR(ZPTP, '099999999999'), -12, 9)), '0'),
    CASE
        WHEN ZPTP < 100000 THEN TO_NUMBER(TO_CHAR(ZPTP, '09999'))
        ELSE ZPTP
    END ZIPCODE_12,
    LFI_FLAG,
    PROID,
    TO_NUMBER(
        TO_CHAR(ASSIGN_AO, 'FM09')
        || TO_CHAR(ASSIGN_TO, 'FM09')
        || 7000) AS ROID,
    TO_DATE('01/01/1900', 'mm/dd/yyyy'),
    ASSACTDT,
    DECODE(dtassign, '01/01/1900', ASSACTDT, DTASSIGN),
    AGGBALDUE AS balance_due,
    GL AS CASEGRADE,
    (SELECT BODCD
        FROM DIALDEV.DIALMOD
        WHERE MODSID = CORESID AND ROWNUM = 1),
    NVL(RISK, 399) AS RANK,
    LPAD(NVL(RISK, 'e') || NVL(ARISK, 'e'), 4, ' ') priority_alpha,
    ARISK,
    NVL(MODELRANK, 0) AS MODELRANK,
    (SELECT seid FROM entemp
        WHERE seid IS NOT NULL
          AND EACTIVE IN ('A', 'Y')
          AND TYPE IN ('P', 'R')
          AND POSTYPE IN ('I', 'N', 'T', 'U', 'S', 'O')
          AND GRADE = 11 AND ROID = 25143513 AND ROWNUM = 1) seid,
    (SELECT unix FROM entemp
        WHERE seid IS NOT NULL
          AND EACTIVE IN ('A', 'Y')
          AND TYPE IN ('P', 'R')
          AND POSTYPE IN ('I', 'N', 'T', 'U', 'S', 'O')
          AND GRADE = 11 AND ROID = 25143513 AND ROWNUM = 1) unix,
    QGRP,
    STAT_FLAG,
    DECODE(LARGE_FLAG, '$', 'T', ' ', 'F', LARGE_FLAG),
    ENT_SEL_CD,
    PDT,
    CORESID,
    '000',
    DIS_VIC,
    (SELECT MAX(LOADDT) FROM DIALDEV.DIALAUD),
    ' ',
    NVL(FD_CNTRCT_IND, 0),
    NVL(OIC_ACC_YR, 0),
    '00',
    ' ',
    TO_DATE('01/01/1900', 'mm/dd/yyyy'),
    (SELECT BODCLCD
        FROM DIALDEV.DIALMOD
        WHERE MODSID = CORESID AND ROWNUM = 1),
    NVL(TDAcnt, 0),
    NVL(TDIcnt, 0),
    NVL((TDAcnt + TDIcnt), 0) AS MODcnt,
    b.HINFIND,
    CNT_941,
    BAL_941,
    CNT_941_14,
    BAL_941_14,
    c.IND_941
FROM DIALDEV.COREDIAL                             a,
     DIALDEV.TINSUMMARY                            b,
     DIALDEV.DIALENT,
     TABLE(Q_mft_ind_vals(a.coresid, a.corefs))    c
WHERE
    grnum = 70
    AND proid BETWEEN 25143500 AND 25143599
    AND a.CORESID = ENTSID
    AND a.CORESID = tinsid
    AND b.RISK IS NOT NULL
    -- ----------------------------------------------------------------
    -- ASSNQPICK replacement (inline scalar subquery)
    -- Original: ASSNQPICK(CORETIN,COREFS,CORETT,
    --             INTLQPICK(CORETIN,COREFS,CORETT,'Q')) not in (1,3,4)
    --
    -- INTLQPICK always queries assn with area=35, territory=02.
    -- ASSNQPICK decomposes the returned proid back to area=35, terr=02,
    -- grnum=X. So we just grab grnum directly from the INTLQPICK lookup,
    -- skipping the TO_NUMBER/TO_CHAR round-trip.
    -- ----------------------------------------------------------------
    AND (
        NVL(
            (SELECT qpickind FROM (
                SELECT qpickind,
                       ROW_NUMBER() OVER (ORDER BY sel_date DESC) rn
                FROM assn
                WHERE tin  = a.CORETIN
                  AND fs   = a.COREFS
                  AND tt   = a.CORETT
                  AND area = 35
                  AND territory = 02
                  AND grnum = NVL(
                      -- nested INTLQPICK: get grnum from most recent Q-status row
                      (SELECT grnum FROM (
                          SELECT grnum,
                                 ROW_NUMBER() OVER (ORDER BY sel_date DESC) rn
                          FROM assn
                          WHERE tin    = a.CORETIN
                            AND fs     = a.COREFS
                            AND tt     = a.CORETT
                            AND status = 'Q'
                            AND area   = 35
                            AND territory = 02
                      ) WHERE rn = 1),
                      0)  -- default grnum=0 (from INTLQPICK default 35020000)
                  AND sel_date >= DECODE(qpickind, 5, SYSDATE - 15, sel_date)
            ) WHERE rn = 1),
            0)  -- ASSNQPICK default: 0
        NOT IN (1, 3, 4)

        OR (
            -- --------------------------------------------------------
            -- ASSNROID replacement (inline scalar subquery)
            -- Original: NVL(ASSNROID(CORETIN,COREFS,CORETT,'Q',
            --              INTLQPICK(...), ROID), 0) = 26133700
            -- Since status is always 'Q', always takes the IF branch:
            --   look for qpickind in (3,4) within 15 days, return
            --   concatenated area||territory||grnum||ronum as number
            -- Falls back to input ROID if no data found
            -- --------------------------------------------------------
            NVL(
                (SELECT TO_NUMBER(
                            TO_CHAR(area, 'FM09') ||
                            TO_CHAR(territory, 'FM09') ||
                            TO_CHAR(grnum, 'FM09') ||
                            TO_CHAR(ronum, 'FM09'))
                 FROM (
                    SELECT area, territory, grnum, ronum,
                           ROW_NUMBER() OVER (ORDER BY sel_date DESC) rn
                    FROM assn
                    WHERE tin  = a.CORETIN
                      AND fs   = a.COREFS
                      AND tt   = a.CORETT
                      AND qpickind IN (3, 4)
                      AND sel_date >= SYSDATE - 15
                      AND area = 35
                      AND territory = 02
                      AND grnum = NVL(
                          -- nested INTLQPICK: same grnum lookup
                          (SELECT grnum FROM (
                              SELECT grnum,
                                     ROW_NUMBER() OVER (ORDER BY sel_date DESC) rn
                              FROM assn
                              WHERE tin    = a.CORETIN
                                AND fs     = a.COREFS
                                AND tt     = a.CORETT
                                AND status = 'Q'
                                AND area   = 35
                                AND territory = 02
                          ) WHERE rn = 1),
                          0)
                 ) WHERE rn = 1),
                -- fallback: return computed ROID (same as input roidin)
                TO_NUMBER(TO_CHAR(ASSIGN_AO, 'FM09') ||
                          TO_CHAR(ASSIGN_TO, 'FM09') || 7000)
            ) = 26133700

            -- Original: AND ASSNQPICK(...) in (1,3,4)
            AND NVL(
                (SELECT qpickind FROM (
                    SELECT qpickind,
                           ROW_NUMBER() OVER (ORDER BY sel_date DESC) rn
                    FROM assn
                    WHERE tin  = a.CORETIN
                      AND fs   = a.COREFS
                      AND tt   = a.CORETT
                      AND area = 35
                      AND territory = 02
                      AND grnum = NVL(
                          (SELECT grnum FROM (
                              SELECT grnum,
                                     ROW_NUMBER() OVER (ORDER BY sel_date DESC) rn
                              FROM assn
                              WHERE tin    = a.CORETIN
                                AND fs     = a.COREFS
                                AND tt     = a.CORETT
                                AND status = 'Q'
                                AND area   = 35
                                AND territory = 02
                          ) WHERE rn = 1),
                          0)
                      AND sel_date >= DECODE(qpickind, 5, SYSDATE - 15, sel_date)
                ) WHERE rn = 1),
                0)
            IN (1, 3, 4)
        )
    )
    -- ----------------------------------------------------------------
    -- Remaining original WHERE conditions
    -- ----------------------------------------------------------------
    AND dis_vic NOT IN (2, 3)
    AND pen_ent_cd <> 1
    -- OR branch for PROID=35000000 special case
    OR (ASSIGN_AO = 35 AND ASSIGN_TO = 0 AND PROID = 35000000
        AND (TO_NUMBER(SUBSTR(TO_CHAR(ZPTP, '099999999999'), -12, 5)) = 00000
             OR RTRIM(NVL(CITP, ' ')) IN ('APO', 'FPO', 'DPO')))
    AND GL BETWEEN 9 AND 13
    AND RISK BETWEEN 0 AND 400
ORDER BY RISK, priority_alpha, MODELRANK DESC
