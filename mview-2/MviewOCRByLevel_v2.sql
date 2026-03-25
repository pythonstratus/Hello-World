/*
 * MviewOCRByLevel.sql — CORRECTED v2 (post-diagnostics)
 *
 * DIAGNOSTIC RESULTS (ROID 25143500, ORG=CF):
 *   DIAG 1: 1454 rows exist (base join)
 *   DIAG 2: 1112 with ORG='CF'
 *   DIAG 3: 0 with myorg='CF' — ALL rows have myorg='XX' → REMOVE myorg filter
 *   DIAG 8: Confirms myorg='XX' (1112 rows)
 *   DIAG 11: trailmatch_new matched 0 of 100 → REPLACE with SWITCHROID matching
 *   DIAG 9: c.STATUS: C=778, R=157, T=71, X=59, E=28, O=19 (no lowercase 'c')
 *   DIAG 10: b.STATUS: C=960, E=152 (no O, Q, R)
 *
 * Changes from v1:
 *   - Removed AND myorg = :org (data has 'XX', not 'CF')
 *   - Replaced trailmatch_new with SWITCHROID ROID matching (from legacy view)
 *   - Added DECODE(c.STATUS,...) max_extrdt filter (from legacy view lines 301-306)
 *   - All column-level fixes from v1 retained
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
SELECT /*+ index_join(b, ENTMOD_STATUS_IX ENTMOD_ROID_IX),
           opt_param('_optimizer_use_feedback' 'false'),
           index_join(c, TRANTRAIL_STATUS_IX TRANTRAIL_ROID_IX) */
    TIN,
    TINTT,
    TINFS,
    c.TINSID as TINSID,
    BODCD,
    DECODE (c.STATUS, 'O', STATIND (a.TINSID), 0) as STATIND,
    TOTASSD,
    a.GRADE as CASEGRADE,
    DECODE(b.STATUS, 'X', 'C', 'c', 'R', b.STATUS) as STATUS,
    SEGIND as CASEIND,
    DECODE (a.ASSNCFF, to_date('01/01/1900','mm/dd/yyyy'), b.ASSNRO, a.ASSNCFF) as ASSNCFF,
    (CASE
        WHEN SEGIND IN ('A', 'C', 'I')
        THEN GETASSNQUE(TIN, TINTT, TINFS, a.ASSNCFF, b.ASSNRO)
        ELSE b.ASSNRO
    END) as ASSNQUE,
    DVICTCD,
    EMPHRS,
    TOTHRS,
    HRS,
    TO_NUMBER (PDTIND) as CAUIND,
    LDIND,
    a.PYRIND as PYRIND,
    RPTIND,
    TPCTRL,
    RWMS,
    NVL (a.RISK, 399) as RISK,
    LSTTOUCH,
    (CASE
        WHEN a.zipcde < 100000
        THEN TO_NUMBER (TO_CHAR (a.ZIPCDE, '09999'))
        WHEN a.zipcde BETWEEN 99999 AND 999999999
        THEN NVL (TO_NUMBER (SUBSTR (TO_CHAR (a.ZIPCDE, '099999999'), -9, 5)), '0')
        ELSE NVL (TO_NUMBER (SUBSTR (TO_CHAR (a.ZIPCDE, '099999999999'), -12, 5)), '0')
    END) as ZIPCDE,
    SUBSTR (TP, 1, 35) as TP,
    SUBSTR (TP2, 1, 35) as TP2,
    SUBSTR (STREET, 1, 35) as STREET,
    STREET2,
    CITY,
    STATE,
    PREDCD,
    PRED_UPDT_CYC,
    EMPTOUCH,
    TO_NUMBER (b.PYRIND) as M_PYRIND,
    TOTTOUCH,
    c.CLOSEDT as CLOSEDT,
    MFT,
    PERIOD,
    TYPE,
    CYCLE,
    BALANCE,
    NVL (RTNDT, to_date('01/01/1900','mm/dd/yyyy')) as RTNDT,
    TO_NUMBER (NVL (b.LFIIND, 0)) as M_LFIIND,
    TO_NUMBER (NVL (a.LFIIND, 0)) as LFIIND,
    DECODE (NVL (b.AGEIND, ' '), ' ', 'C', '0', 'C', b.AGEIND) as AGEIND,
    DECODE (tinfs, 2, TO_NUMBER (CCNIPSELECTCD), TO_NUMBER (SELCODE)) as SELCODE,
    (CASE
        WHEN TYPE IN ('O', 'N')
        THEN NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy'))
        ELSE CSED
    END) as CSED,
    CSEDIND,
    LRA,
    (CASE
        WHEN TYPE IN ('O', 'N')
        THEN NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy'))
        ELSE ASED
    END) as ASED,
    ASEDIND,
    b.ROID as M_ROID,
    PROID,
    c.ROID as ROID,
    b.STATUS as M_STATUS,
    CASECODE,
    SUBCODE,
    CIVPCD,
    b.ASSNRO as M_ASSNRO,
    c.ASSNRO as ASSNRO,
    DUEDATE,
    CREATEDT,
    NVL (FTLCD, '0') as FTLCD,
    NVL (FTLDETDT, to_date('01/01/1900','mm/dd/yyyy')) as FTLDETDT,
    DECODE (b.status, 'C', CLSDT, NULL) as CLSDT,
    DECODE (b.status, 'C', b.DISPCODE, NULL) as DISPCODE,
    CASE
        WHEN b.STATUS = 'C' THEN NVL(cc.ABBREV, '          ')
        ELSE '          '
    END as ABBREV,
    F1058DT1,
    L3174DT1,
    b.NAICSCD as M_NAICSCD,
    c.NAICSCD as NAICSCD,
    SUBSTR(DECODE(TYPE,
        'A', NVL(POAIND, ' '),
        'B', NVL(POAIND, ' '),
        'C', NVL(POAIND, ' '),
        'D', NVL(POAIND, ' '),
        'F', NVL(POAIND, ' '),
        'G', NVL(POAIND, ' '),
        'I', NVL(POAIND, ' ')), 1, 1) as POAIND,
    NVL (TO_CHAR (CAFCD), ' ') as CAFCD,
    NVL (CYCAGE2A, 0) as CYCAGE2A,
    NVL (CYCAGE2I, 0) as CYCAGE2I,
    NVL (CYCMOD2I, 0) as CYCMOD2I,
    b.INSPCIND as INSPCIND,
    b.ERRFDIND as ERRFDIND,
    b.TSACTCD as TSACTCD,
    SPECPRJCD,
    NVL (OICSED, to_date('01/01/1900','mm/dd/yyyy')) as OICSED,
    NVL (OIASED, to_date('01/01/1900','mm/dd/yyyy')) as OIASED,
    NVL (F1058DT2, to_date('01/01/1900','mm/dd/yyyy')) as F1058DT2,
    NVL (L3174DT2, to_date('01/01/1900','mm/dd/yyyy')) as L3174DT2,
    FMSLVCD,
    b.NAICSVLD as NAICSVLD,
    b.NAICSYR as NAICSYR,
    IAFTPIND,
    NVL (TDAcnt, 0) as TDACNT,
    NVL (TDIcnt, 0) as TDICNT,
    NVL ((TDAcnt + TDIcnt), 0) as MODCNT,
    NVL (OIcnt, 0) as OICNT,
    NVL (FTDcnt, 0) as FTDCNT,
    NVL (OICcnt, 0) as OICCNT,
    NVL (nIDRScnt, 0) as NIDRSCNT,
    INITDT,
    XXDT,
    ASSNGRP,
    b.EXTRDT as EXTRDT,
    BODCLCD,
    ICSCC,
    TC,
    NVL (DT_OAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_OA,
    NVL (DT_POAMOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_POA,
    SUBSTR(REPLACE(PRGNAME1, 'BOUND', ' '), 1, 20) as PRGNAME1,
    SUBSTR(REPLACE(PRGNAME2, 'BOUND', ' '), 1, 20) as PRGNAME2,
    ASSNFLD,
    FLDHRS,
    HINFIND,
    CNT_941,
    BAL_941,
    CNT_941_14,
    BAL_941_14,
    d.IND_941 as IND_941,
    TDI_CNT_941,
    NVL (L725DT, to_date('01/01/1900','mm/dd/yyyy')) as L725DT,
    MODFATCAIND as FATCAIND,
    0 as PASSPORT_LEVY_IND,
    AGI_AMT,
    TPI_AMT,
    AGI_TPI_TX_YR,
    AGI_TPI_IND,
    CC,
    NVL (DT_DOD, to_date('01/01/1900','mm/dd/yyyy')) as DT_DOD,
    TRUNC(ASSNPICKDT(TIN, TINFS, TINTT, c.STATUS, PROID, c.ROID)) as PICKDT,
    DECODE(DECODE(c.ZIPCDE, 00000,
        ASSNQPICK(TIN, TINFS, TINTT, INTLQPICK(TIN, TINFS, TINTT, c.STATUS)),
        DECODE(CITY, 'APO',
            ASSNQPICK(TIN, TINFS, TINTT, INTLQPICK(TIN, TINFS, TINTT, c.STATUS)),
            'FPO',
            ASSNQPICK(TIN, TINFS, TINTT, INTLQPICK(TIN, TINFS, TINTT, c.STATUS)),
            ASSNQPICK(TIN, TINFS, TINTT, PROID))),
        '', 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, '?') as QPICKIND,
    FEDCONIND,
    FEDEMPIND,
    IRSEMPIND,
    L903,
    LLCIND,
    THEFTIND,
    OICACCYR,
    0 as TOT_IRP_INC,
    LPAD (NVL (a.RISK, 399) || NVL (a.ARISK, 'e'), 4, ' ') as ARANK,
    (case when d.ind_941 = 0 then 'No' else 'Yes' end) as FORMATTED_IND_941,
    0 as TOT_INC_DELQ_YR,
    0 as PRIOR_YR_RET_AGI_AMT,
    0 as TXPER_TXPYR_AMT,
    0 as PRIOR_ASSGMNT_NUM,
    to_date('01/01/1900','mm/dd/yyyy') as PRIOR_ASSGMNT_ACT_DT,
    (case when hinfind = 0 then 'No' else 'Yes' end) FORMATTED_HINFIND,
    (case when a.lfiind = 0 then '*' else 'OK' end) as FORMATTED_LFIIND,
    DECODE(TYPE,
        'A', 'TDA_A ', 'B', 'TDA_B ', 'C', 'TDA_C ', 'D', 'TDA_D ', 'E', 'TDA_E ',
        'F', 'TDI_F ', 'G', 'TDI_G ', 'I', 'TDI_I ',
        'N', 'OI-in ', 'O', 'OI-out', 'R', 'CIP   ', 'T', 'FTD   ',
        'X', 'TCMP  ', 'Y', 'OIC   ', ' ') as MODULETYPE,
    CCNIPSELECTCD,
    DECODE (
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
    -- Replaces trailmatch_new (which matched 0 of 100 rows per DIAG 11)
    -- Legacy view uses SWITCHROID to match ENTMOD ROID to TRANTRAIL ROID
    to_number(SWITCHROID(b.ROID)) = to_number(SWITCHROID(c.ROID))
    -- Legacy DECODE filter: for non-Open statuses, only pick latest EXTRDT row
    AND DECODE(c.STATUS,
            'O', 1,
            'C', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'c', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            'R', (c.extrdt - max_extrdt(c.tinsid, c.roid)),
            -1) >= 0
    AND ORG = :org
    -- myorg REMOVED: DIAG 8 shows all rows have myorg='XX', not 'CF'
    AND case_org (b.roid) = :org
    AND (c.STATUS = 'O' OR c.STATUS = 'C' OR c.STATUS = 'c' OR c.STATUS = 'R')
    AND b.STATUS NOT IN ('E', 'Q')
    AND trunc(b.roid/power(10,8-:elevel)) = :levelValue
) t
)
where row_num = 1
