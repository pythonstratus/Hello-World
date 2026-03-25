/*
 * MviewOCRByLevel.sql — v3 with UNION ALL
 *
 * BRANCH 1: ENTMOD (ENT/TRANTRAIL/ENTMOD) — confirmed 169 rows
 * BRANCH 2: QUEUE  (DIAL.COREDIAL/TINSUMMARY/DIALENT/DIALMOD) — legacy lines 325-493
 *
 * Together should approximate legacy count of ~1832.
 *
 * myorg removed from BOTH branches (returns 'XX' in Java pool environment).
 * trailmatch_new replaced with SWITCHROID matching in ENTMOD branch.
 */
select *
from
(
select t.*,
    ROW_NUMBER() OVER (
        PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                     m_assnro, lfiind, ftldetdt, ased, csed
        ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                 m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
from
(
/* ======================================================================
   BRANCH 1: ENTMOD — modules from ENT/TRANTRAIL/ENTMOD
   ====================================================================== */
SELECT /*+ index_join(b, ENTMOD_STATUS_IX ENTMOD_ROID_IX),
           opt_param('_optimizer_use_feedback' 'false'),
           index_join(c, TRANTRAIL_STATUS_IX TRANTRAIL_ROID_IX) */
    TIN,                                                                     -- 1
    TINTT,                                                                   -- 2
    TINFS,                                                                   -- 3
    c.TINSID as TINSID,                                                      -- 4
    BODCD,                                                                   -- 5
    DECODE (c.STATUS, 'O', STATIND (a.TINSID), 0) as STATIND,               -- 6
    TOTASSD,                                                                 -- 7
    a.GRADE as CASEGRADE,                                                    -- 8
    DECODE(b.STATUS, 'X', 'C', 'c', 'R', b.STATUS) as STATUS,               -- 9
    SEGIND as CASEIND,                                                       -- 10
    DECODE (a.ASSNCFF, to_date('01/01/1900','mm/dd/yyyy'), b.ASSNRO, a.ASSNCFF) as ASSNCFF,  -- 11
    (CASE                                                                    -- 12
        WHEN SEGIND IN ('A', 'C', 'I')
        THEN GETASSNQUE(TIN, TINTT, TINFS, a.ASSNCFF, b.ASSNRO)
        ELSE b.ASSNRO
    END) as ASSNQUE,
    DVICTCD,                                                                 -- 13
    EMPHRS,                                                                  -- 14
    TOTHRS,                                                                  -- 15
    HRS,                                                                     -- 16
    TO_NUMBER (PDTIND) as CAUIND,                                            -- 17
    LDIND,                                                                   -- 18
    a.PYRIND as PYRIND,                                                      -- 19
    RPTIND,                                                                  -- 20
    TPCTRL,                                                                  -- 21
    RWMS,                                                                    -- 22
    NVL (a.RISK, 399) as RISK,                                               -- 23
    LSTTOUCH,                                                                -- 24
    (CASE                                                                    -- 25
        WHEN a.zipcde < 100000
        THEN TO_NUMBER (TO_CHAR (a.ZIPCDE, '09999'))
        WHEN a.zipcde BETWEEN 99999 AND 999999999
        THEN NVL (TO_NUMBER (SUBSTR (TO_CHAR (a.ZIPCDE, '099999999'), -9, 5)), '0')
        ELSE NVL (TO_NUMBER (SUBSTR (TO_CHAR (a.ZIPCDE, '099999999999'), -12, 5)), '0')
    END) as ZIPCDE,
    SUBSTR (TP, 1, 35) as TP,                                               -- 26
    SUBSTR (TP2, 1, 35) as TP2,                                             -- 27
    SUBSTR (STREET, 1, 35) as STREET,                                        -- 28
    STREET2,                                                                 -- 29
    CITY,                                                                    -- 30
    STATE,                                                                   -- 31
    PREDCD,                                                                  -- 32
    PRED_UPDT_CYC,                                                           -- 33
    EMPTOUCH,                                                                -- 34
    TO_NUMBER (b.PYRIND) as M_PYRIND,                                        -- 35
    TOTTOUCH,                                                                -- 36
    c.CLOSEDT as CLOSEDT,                                                    -- 37
    MFT,                                                                     -- 38
    PERIOD,                                                                  -- 39
    TYPE,                                                                    -- 40
    CYCLE,                                                                   -- 41
    BALANCE,                                                                 -- 42
    NVL (RTNDT, to_date('01/01/1900','mm/dd/yyyy')) as RTNDT,               -- 43
    TO_NUMBER (NVL (b.LFIIND, 0)) as M_LFIIND,                              -- 44
    TO_NUMBER (NVL (a.LFIIND, 0)) as LFIIND,                                -- 45
    DECODE (NVL (b.AGEIND, ' '), ' ', 'C', '0', 'C', b.AGEIND) as AGEIND,  -- 46
    DECODE (tinfs, 2, TO_NUMBER (CCNIPSELECTCD), TO_NUMBER (SELCODE)) as SELCODE,  -- 47
    (CASE                                                                    -- 48
        WHEN TYPE IN ('O', 'N')
        THEN NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy'))
        ELSE CSED
    END) as CSED,
    CSEDIND,                                                                 -- 49
    LRA,                                                                     -- 50
    (CASE                                                                    -- 51
        WHEN TYPE IN ('O', 'N')
        THEN NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy'))
        ELSE ASED
    END) as ASED,
    ASEDIND,                                                                 -- 52
    b.ROID as M_ROID,                                                        -- 53
    PROID,                                                                   -- 54
    c.ROID as ROID,                                                          -- 55
    b.STATUS as M_STATUS,                                                    -- 56
    CASECODE,                                                                -- 57
    SUBCODE,                                                                 -- 58
    CIVPCD,                                                                  -- 59
    b.ASSNRO as M_ASSNRO,                                                    -- 60
    c.ASSNRO as ASSNRO,                                                      -- 61
    DUEDATE,                                                                 -- 62
    CREATEDT,                                                                -- 63
    NVL (FTLCD, '0') as FTLCD,                                              -- 64
    NVL (FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,          -- 65
    DECODE (b.status, 'C', CLSDT, NULL) as CLSDT,                           -- 66
    DECODE (b.status, 'C', b.DISPCODE, NULL) as DISPCODE,                    -- 67
    CASE                                                                     -- 68
        WHEN b.STATUS = 'C' THEN NVL(cc.ABBREV, '          ')
        ELSE '          '
    END as ABBREV,
    F1058DT1,                                                                -- 69
    L3174DT1,                                                                -- 70
    b.NAICSCD as M_NAICSCD,                                                  -- 71
    c.NAICSCD as NAICSCD,                                                    -- 72
    SUBSTR(DECODE(TYPE,                                                      -- 73
        'A', NVL(POAIND, ' '),
        'B', NVL(POAIND, ' '),
        'C', NVL(POAIND, ' '),
        'D', NVL(POAIND, ' '),
        'F', NVL(POAIND, ' '),
        'G', NVL(POAIND, ' '),
        'I', NVL(POAIND, ' ')), 1, 1) as POAIND,
    NVL (TO_CHAR (CAFCD), ' ') as CAFCD,                                    -- 74
    NVL (CYCAGE2A, 0) as CYCAGE2A,                                          -- 75
    NVL (CYCAGE2I, 0) as CYCAGE2I,                                          -- 76
    NVL (CYCMOD2I, 0) as CYCMOD2I,                                          -- 77
    b.INSPCIND as INSPCIND,                                                  -- 78
    b.ERRFDIND as ERRFDIND,                                                  -- 79
    b.TSACTCD as TSACTCD,                                                    -- 80
    SPECPRJCD,                                                               -- 81
    NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy')) as OICSED,             -- 82
    NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy')) as OIASED,             -- 83
    NVL (F1058DT2, to_date('01/01/1900','mm/dd/yyyy')) as F1058DT2,         -- 84
    NVL (L3174DT2, to_date('01/01/1900','mm/dd/yyyy')) as L3174DT2,         -- 85
    FMSLVCD,                                                                 -- 86
    b.NAICSVLD as NAICSVLD,                                                  -- 87
    b.NAICSYR as NAICSYR,                                                    -- 88
    IAFTPIND,                                                                -- 89
    NVL (TDAcnt, 0) as TDACNT,                                              -- 90
    NVL (TDIcnt, 0) as TDICNT,                                              -- 91
    NVL ((TDAcnt + TDIcnt), 0) as MODCNT,                                   -- 92
    NVL (OIcnt, 0) as OICNT,                                                -- 93
    NVL (FTDcnt, 0) as FTDCNT,                                              -- 94
    NVL (OICcnt, 0) as OICCNT,                                              -- 95
    NVL (nIDRScnt, 0) as NIDRSCNT,                                          -- 96
    INITDT,                                                                  -- 97
    XXDT,                                                                    -- 98
    ASSNGRP,                                                                 -- 99
    b.EXTRDT as EXTRDT,                                                      -- 100
    BODCLCD,                                                                 -- 101
    ICSCC,                                                                   -- 102
    TC,                                                                      -- 103
    NVL (DT_OAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_OA,            -- 104
    NVL (DT_POAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_POA,          -- 105
    SUBSTR(REPLACE(PRGNAME1, 'BOUND', ' '), 1, 20) as PRGNAME1,             -- 106
    SUBSTR(REPLACE(PRGNAME2, 'BOUND', ' '), 1, 20) as PRGNAME2,             -- 107
    ASSNFLD,                                                                 -- 108
    FLDHRS,                                                                  -- 109
    HINFIND,                                                                 -- 110
    CNT_941,                                                                 -- 111
    BAL_941,                                                                 -- 112
    CNT_941_14,                                                              -- 113
    BAL_941_14,                                                              -- 114
    d.IND_941 as IND_941,                                                    -- 115
    TDI_CNT_941,                                                             -- 116
    NVL (L725DT, to_date('01/01/1900','mm/dd/yyyy')) as L725DT,             -- 117
    MODFATCAIND as FATCAIND,                                                 -- 118
    0 as PASSPORT_LEVY_IND,                                                  -- 119
    AGI_AMT,                                                                 -- 120
    TPI_AMT,                                                                 -- 121
    AGI_TPI_TX_YR,                                                           -- 122
    AGI_TPI_IND,                                                             -- 123
    CC,                                                                      -- 124
    NVL (DT_DOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_DOD,             -- 125
    TRUNC(ASSNPICKDT(TIN, TINFS, TINTT, c.STATUS, PROID, c.ROID)) as PICKDT, -- 126
    DECODE(DECODE(c.ZIPCDE, 00000,                                           -- 127
        ASSNQPICK(TIN, TINFS, TINTT, INTLQPICK(TIN, TINFS, TINTT, c.STATUS)),
        DECODE(CITY, 'APO',
            ASSNQPICK(TIN, TINFS, TINTT, INTLQPICK(TIN, TINFS, TINTT, c.STATUS)),
            'FPO',
            ASSNQPICK(TIN, TINFS, TINTT, INTLQPICK(TIN, TINFS, TINTT, c.STATUS)),
            ASSNQPICK(TIN, TINFS, TINTT, PROID))),
        '', 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, '?') as QPICKIND,
    FEDCONIND,                                                               -- 128
    FEDEMPIND,                                                               -- 129
    IRSEMPIND,                                                               -- 130
    L903,                                                                    -- 131
    LLCIND,                                                                  -- 132
    THEFTIND,                                                                -- 133
    OICACCYR,                                                                -- 134
    0 as TOT_IRP_INC,                                                        -- 135
    LPAD (NVL (a.RISK, 399) || NVL (a.ARISK, 'e'), 4, ' ') as ARANK,       -- 136
    (case when d.ind_941 = 0 then 'No' else 'Yes' end) as FORMATTED_IND_941, -- 137
    0 as TOT_INC_DELQ_YR,                                                   -- 138
    0 as PRIOR_YR_RET_AGI_AMT,                                              -- 139
    0 as TXPER_TXPYR_AMT,                                                    -- 140
    0 as PRIOR_ASSGMNT_NUM,                                                  -- 141
    to_date('01/01/1900','mm/dd/yyyy') as PRIOR_ASSGMNT_ACT_DT,             -- 142
    (case when hinfind = 0 then 'No' else 'Yes' end) FORMATTED_HINFIND,     -- 143
    (case when a.lfiind = 0 then '*' else 'OK' end) as FORMATTED_LFIIND,    -- 144
    DECODE(TYPE,                                                             -- 145
        'A', 'TDA_A ', 'B', 'TDA_B ', 'C', 'TDA_C ', 'D', 'TDA_D ', 'E', 'TDA_E ',
        'F', 'TDI_F ', 'G', 'TDI_G ', 'I', 'TDI_I ',
        'N', 'OI-in ', 'O', 'OI-out', 'R', 'CIP   ', 'T', 'FTD   ',
        'X', 'TCMP  ', 'Y', 'OIC   ', ' ') as MODULETYPE,
    CCNIPSELECTCD,                                                           -- 146
    DECODE (                                                                 -- 147
        c.STATUS,
        'O', (CASE
                WHEN SEGIND IN ('C', 'A', 'I')
                AND a.casecode IN ('201', '301', '401', '601')
                AND totassd >= 10000
                AND EXISTS
                    (SELECT /*+ index(entmod, entmod_sid_ix) */ 1
                     FROM entmod em
                     WHERE em.emodsid = c.tinsid
                        AND em.status = 'O'
                        AND em.mft IN (1, 9, 11, 12, 13, 14, 16, 64)
                        AND b.assnro + 150 < duedt (em.period, em.mft))
                THEN 1
                ELSE 0
            END),
        0) as PYRENT

FROM ENT a
    inner join TRANTRAIL c
        on a.TINSID = c.TINSID
    inner join ENTMOD b
        on a.TINSID = b.EMODSID
    LEFT JOIN (
        SELECT DISPCODE, ABBREV,
            ROW_NUMBER() OVER (PARTITION BY DISPCODE ORDER BY ROWID) rn
        FROM CLSCODE
    ) cc ON cc.DISPCODE = b.DISPCODE AND cc.rn = 1
    , TABLE (mft_ind_vals (c.tinsid, a.tinfs)) d
WHERE
    to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
    AND DECODE(c.STATUS,
            'O', 1,
            'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            -1) >= 0
    AND ORG = :org
    AND case_org (b.roid) = :org
    AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
    AND b.STATUS NOT IN ('E', 'Q')
    AND trunc(b.roid/power(10,8-:elevel)) = :levelValue

UNION ALL

/* ======================================================================
   BRANCH 2: QUEUE — modules from DIAL.COREDIAL/TINSUMMARY/DIALENT/DIALMOD
   Legacy mview_realign lines 325-493
   ====================================================================== */
SELECT /*+ first_rows */
    /*+ INDEX(COREDIAL,DIAL_ROID_IX) index(a, grnum_ix) */
    CORETIN as TIN,                                                          -- 1
    CORETT as TINTT,                                                         -- 2
    COREFS as TINFS,                                                         -- 3
    DIALENT.ENTSID as TINSID,                                                -- 4
    BODCD,                                                                   -- 5
    STAT_FLAG as STATIND,                                                    -- 6
    AGGBALDUE as TOTASSD,                                                    -- 7
    GL as CASEGRADE,                                                         -- 8
    'Q' as STATUS,                                                           -- 9
    CASE                                                                     -- 10
        WHEN TDACNT > 0 AND TDICNT > 0 THEN 'C'
        WHEN TDACNT > 0 AND TDICNT = 0 THEN 'A'
        WHEN TDICNT > 0 AND TDACNT = 0 THEN 'I'
        ELSE ' '
    END as CASEIND,
    DECODE(DTASSIGN, TO_DATE('01/01/1900','mm/dd/yyyy'), ASSACTDT, DTASSIGN) as ASSNCFF,  -- 11
    ASSACTDT as ASSNQUE,                                                     -- 12
    DIS_VIC as DVICTCD,                                                      -- 13
    0 as EMPHRS,                                                             -- 14
    0 as TOTHRS,                                                             -- 15
    0 as HRS,                                                                -- 16
    PDT as CAUIND,                                                           -- 17
    DECODE(LARGE_FLAG, '$', 'T', ' ', 'F', LARGE_FLAG) as LDIND,            -- 18
    CASE                                                                     -- 19
        WHEN CURR_PYR = ' ' AND INIT_PYR = 'Y' THEN 1
        WHEN CURR_PYR = 'Y' AND INIT_PYR = ' ' THEN 4
        WHEN CURR_PYR = 'Y' AND INIT_PYR = 'Y' THEN 4
        ELSE 0
    END as PYRIND,
    DECODE(REPEAT, '0', 'F', '1', 'P', '2', 'T', 'F') as RPTIND,          -- 20
    NACTL as TPCTRL,                                                         -- 21
    RWMS,                                                                    -- 22
    NVL(RISK, 399) as RISK,                                                  -- 23
    TO_DATE('01/01/1900','mm/dd/yyyy') as LSTTOUCH,                          -- 24
    TO_NUMBER(SUBSTR(TO_CHAR(ZPTP,'099999999999'),-12,5)) as ZIPCDE,        -- 25
    SUBSTR(NATP, 1, 35) as TP,                                              -- 26
    SUBSTR(NATP2, 1, 35) as TP2,                                            -- 27
    SUBSTR(ADTP, 1, 35) as STREET,                                          -- 28
    ' ' as STREET2,                                                          -- 29
    RTRIM(CITP) as CITY,                                                     -- 30
    STTP as STATE,                                                           -- 31
    EMIS_PREDIC_CD as PREDCD,                                                -- 32
    EMIS_PREDIC_CYC as PRED_UPDT_CYC,                                       -- 33
    0 as EMPTOUCH,                                                           -- 34
    TO_NUMBER(MOD_PYR_IND) as M_PYRIND,                                     -- 35
    0 as TOTTOUCH,                                                           -- 36
    TO_DATE('01/01/1900','mm/dd/yyyy') as CLOSEDT,                           -- 37
    MFT,                                                                     -- 38
    DTPER as PERIOD,                                                         -- 39
    DECODE(RECTYPE, 0, 'I', 5, 'A', ' ') as TYPE,                           -- 40
    STAT_CYC as CYCLE,                                                       -- 41
    BALDUE as BALANCE,                                                       -- 42
    NVL(DTASSD, TO_DATE('01/01/1900','mm/dd/yyyy')) as RTNDT,               -- 43
    TO_NUMBER(DECODE(LIEN, '1', '1', '0')) as M_LFIIND,                     -- 44
    TO_NUMBER(DECODE(LFI_FLAG, '1', '1', '0')) as LFIIND,                   -- 45
    ' ' as AGEIND,                                                           -- 46
    TO_NUMBER(SELECT_CD) as SELCODE,                                        -- 47
    GREATEST(CSED, LATEST_MOD_CSED) as CSED,                                -- 48
    NVL(CSED_REV, ' ') as CSEDIND,                                          -- 49
    LAST_AMT as LRA,                                                         -- 50
    TO_DATE('01/01/1900','mm/dd/yyyy') as ASED,                             -- 51
    NVL(ASED_REV, ' ') as ASEDIND,                                          -- 52
    TO_NUMBER(TO_CHAR(ASSIGN_AO,'FM09')||TO_CHAR(ASSIGN_TO,'FM09')||7000) as M_ROID,  -- 53
    SWITCHROID(PROID) as PROID,                                              -- 54
    TO_NUMBER(TO_CHAR(ASSIGN_AO,'FM09')||TO_CHAR(ASSIGN_TO,'FM09')||7000) as ROID,    -- 55
    'Q' as M_STATUS,                                                         -- 56
    '000' as CASECODE,                                                       -- 57
    '000' as SUBCODE,                                                        -- 58
    TO_CHAR(CIVP) as CIVPCD,                                                 -- 59
    ASSACTDT as M_ASSNRO,                                                    -- 60
    ASSACTDT as ASSNRO,                                                      -- 61
    TO_DATE('01/01/1900','mm/dd/yyyy') as DUEDATE,                          -- 62
    TO_DATE('01/01/1900','mm/dd/yyyy') as CREATEDT,                         -- 63
    '0' as FTLCD,                                                            -- 64
    TO_DATE('01/01/1900','mm/dd/yyyy') as FTLDETDT,                         -- 65
    TO_DATE('01/01/1900','mm/dd/yyyy') as CLSDT,                            -- 66
    TO_CHAR(NULL) as DISPCODE,                                               -- 67
    '          ' as ABBREV,                                                  -- 68
    TO_DATE('01/01/1900','mm/dd/yyyy') as F1058DT1,                         -- 69
    TO_DATE('01/01/1900','mm/dd/yyyy') as L3174DT1,                         -- 70
    ' ' as M_NAICSCD,                                                        -- 71
    ' ' as NAICSCD,                                                          -- 72
    ' ' as POAIND,                                                           -- 73
    NVL(TO_CHAR(CAF), ' ') as CAFCD,                                         -- 74
    NVL(TDI_AG_CYC, 0) as CYCAGE2A,                                         -- 75
    NVL(TDICYC, 0) as CYCAGE2I,                                             -- 76
    0 as CYCMOD2I,                                                         -- 77
    'F' as INSPCIND,                                                         -- 78
    'F' as ERRFDIND,                                                         -- 79
    ' ' as TSACTCD,                                                          -- 80
    SPECIAL_PROJ_CD as SPECPRJCD,                                            -- 81
    TO_DATE('01/01/1900','mm/dd/yyyy') as OICSED,                           -- 82
    TO_DATE('01/01/1900','mm/dd/yyyy') as OIASED,                           -- 83
    TO_DATE('01/01/1900','mm/dd/yyyy') as F1058DT2,                         -- 84
    TO_DATE('01/01/1900','mm/dd/yyyy') as L3174DT2,                         -- 85
    ' ' as FMSLVCD,                                                          -- 86
    ' ' as NAICSVLD,                                                         -- 87
    0 as NAICSYR,                                                            -- 88
    ' ' as IAFTPIND,                                                         -- 89
    NVL(TDAcnt, 0) as TDACNT,                                               -- 90
    NVL(TDIcnt, 0) as TDICNT,                                               -- 91
    NVL((TDAcnt + TDIcnt), 0) as MODCNT,                                    -- 92
    0 as OICNT,                                                              -- 93
    0 as FTDCNT,                                                             -- 94
    0 as OICCNT,                                                             -- 95
    0 as NIDRSCNT,                                                           -- 96
    TO_DATE('01/01/1900','mm/dd/yyyy') as INITDT,                           -- 97
    TO_DATE('01/01/1900','mm/dd/yyyy') as XXDT,                             -- 98
    TO_DATE('01/01/1900','mm/dd/yyyy') as ASSNGRP,                          -- 99
    (SELECT MAX(LOADDT) FROM DIAL.DIALAUD) as EXTRDT,                        -- 100
    BODCLCD,                                                                 -- 101
    0 as ICSCC,                                                              -- 102
    0 as TC,                                                                 -- 103
    TO_DATE('01/01/1900','mm/dd/yyyy') as DT_OA,                            -- 104
    TO_DATE('01/01/1900','mm/dd/yyyy') as DT_POA,                           -- 105
    ' ' as PRGNAME1,                                                         -- 106
    ' ' as PRGNAME2,                                                         -- 107
    TO_DATE('01/01/1900','mm/dd/yyyy') as ASSNFLD,                          -- 108
    0 as FLDHRS,                                                             -- 109
    0 as HINFIND,                                                            -- 110
    0 as CNT_941,                                                            -- 111
    0 as BAL_941,                                                            -- 112
    0 as CNT_941_14,                                                         -- 113
    0 as BAL_941_14,                                                         -- 114
    0 as IND_941,                                                            -- 115
    0 as TDI_CNT_941,                                                        -- 116
    TO_DATE('01/01/1900','mm/dd/yyyy') as L725DT,                           -- 117
    ' ' as FATCAIND,                                                       -- 118
    0 as PASSPORT_LEVY_IND,                                                  -- 119
    0 as AGI_AMT,                                                            -- 120
    0 as TPI_AMT,                                                            -- 121
    0 as AGI_TPI_TX_YR,                                                      -- 122
    0 as AGI_TPI_IND,                                                        -- 123
    0 as CC,                                                                 -- 124
    TO_DATE('01/01/1900','mm/dd/yyyy') as DT_DOD,                           -- 125
    TO_DATE('01/01/1900','mm/dd/yyyy') as PICKDT,                           -- 126
    0 as QPICKIND,                                                          -- 127
    0 as FEDCONIND,                                                          -- 128
    0 as FEDEMPIND,                                                          -- 129
    0 as IRSEMPIND,                                                          -- 130
    0 as L903,                                                               -- 131
    ' ' as LLCIND,                                                          -- 132
    0 as THEFTIND,                                                          -- 133
    0 as OICACCYR,                                                           -- 134
    0 as TOT_IRP_INC,                                                        -- 135
    LPAD(NVL(RISK, 399), 4, ' ') as ARANK,                                  -- 136
    'No' as FORMATTED_IND_941,                                               -- 137
    0 as TOT_INC_DELQ_YR,                                                   -- 138
    0 as PRIOR_YR_RET_AGI_AMT,                                              -- 139
    0 as TXPER_TXPYR_AMT,                                                    -- 140
    0 as PRIOR_ASSGMNT_NUM,                                                  -- 141
    TO_DATE('01/01/1900','mm/dd/yyyy') as PRIOR_ASSGMNT_ACT_DT,             -- 142
    'No' as FORMATTED_HINFIND,                                               -- 143
    '*' as FORMATTED_LFIIND,                                                 -- 144
    DECODE(RECTYPE,                                                          -- 145
        0, 'TDI_I ', 5, 'TDA_A ', ' ') as MODULETYPE,
    ' ' as CCNIPSELECTCD,                                                   -- 146
    PYRDIALENT(CORESID) as PYRENT                                            -- 147

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
    -- e-level filter using derived ROID from ASSIGN_AO/ASSIGN_TO
    AND trunc(
        TO_NUMBER(TO_CHAR(ASSIGN_AO,'FM09')||TO_CHAR(ASSIGN_TO,'FM09')||7000)
        / power(10, 8 - :elevel)
    ) = :levelValue

) t
)
where row_num = 1
