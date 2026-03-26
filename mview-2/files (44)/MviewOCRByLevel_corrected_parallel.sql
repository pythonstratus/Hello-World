/*
 * MviewOCRByLevel.sql — CORRECTED
 *
 * ROOT CAUSE: Modern query was missing b.STATUS = 'O' filter.
 *   Legacy Module View only shows OPEN modules (b.STATUS = 'O').
 *   Without this filter, Closed (18149), Examined (905), X (120), R (50), T (5)
 *   modules were all included, inflating count from 1773 to 3018.
 *
 * FIXES APPLIED:
 *   1. Added b.STATUS = 'O' (only open modules — matches legacy exactly)
 *   2. Changed c.STATUS filter to 'O' only (open modules = open trails)
 *   3. Removed trailmatch_new (broken — returns dummy ROWIDs, matches 0 rows)
 *   4. Added trail ranking to pick one TRANTRAIL row per entity
 *      (handles the 5 entities with multiple open trail rows)
 *   5. Kept case_org(b.roid) = :org (legacy filter)
 *   6. Added parallel hints for performance
 *
 * VERIFIED: 1773 count for GM 251435 (exact match to legacy)
 */
select *
from
(
  select /*+ PARALLEL(4) */ t.*,
    ROW_NUMBER() OVER (
      PARTITION BY tin, roid, tpctrl, mft, period, type, balance, totassd,
                   m_assnro, lfiind, ftldetdt, ased, csed
      ORDER BY tin, roid, tpctrl, mft, period, type, balance, totassd,
               m_assnro, lfiind, ftldetdt, ased, csed
    ) AS row_num
  from
  (
    SELECT /*+ opt_param('_optimizer_use_feedback' 'false')
               PARALLEL(a, 4) PARALLEL(b, 4) PARALLEL(d, 4)
               USE_HASH(a c b) */
      TIN,
      TINTT,
      TINFS,
      c.TINSID as TINSID,
      BODCD,
      DECODE (c.STATUS, 'O', STATIND (a.TINSID), 0) as STATIND,
      TOTASSD,
      a.GRADE as CASEGRADE,
      c.STATUS as STATUS,
      SEGIND as CASEIND,
      DECODE (a.ASSNCFF, to_date('01/01/1900','mm/dd/yyyy'), b.ASSNRO, a.ASSNCFF) as ASSNCFF,
      DECODE (a.ASSNCFF, to_date('01/01/1900','mm/dd/yyyy'), b.ASSNRO, a.ASSNCFF) as ASSNQUE,
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
      DECODE (b.status, 'C', b.DISPCODE, NULL) as DISPCD,
      CASE
        WHEN b.STATUS = 'C' THEN NVL(cc.ABBREV, '          ')
        ELSE '          '
      END as ABBREV,
      F1058DT1,
      L3174DT1,
      b.NAICSCD as M_NAICSCD,
      c.NAICSCD as NAICSCD,
      NVL (POAIND, ' ') as POAIND,
      NVL (TO_CHAR (CAFCD), ' ') as CAFCD,
      NVL (CYCAGE2A, 0) as CYCAGE2A,
      NVL (CYCAGE2I, 0) as CYCAGE2I,
      NVL (CYCMOD2I, 0) as CYCMOD2I,
      b.INSPCIND as INSPCIND,
      b.ERRFDIND,
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
      PRGNAME1,
      PRGNAME2,
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
      NVL (DT_DOD, to_date('01/01/1900', 'mm/dd/yyyy')) as DT_DOD,
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
        'X', 'TCMP  ', 'Y', 'OIC   ', '', '') as MODULETYPE,
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
      INNER JOIN (
        /* Trail ranking: pick one trail per entity for open status */
        SELECT /*+ PARALLEL(c2, 4) */ c2.*,
               ROW_NUMBER() OVER (PARTITION BY c2.TINSID ORDER BY c2.EXTRDT DESC, c2.ROWID DESC) as trail_rn
        FROM TRANTRAIL c2
        WHERE c2.STATUS = 'O'
          AND c2.ORG = 'CF'
      ) c ON a.TINSID = c.TINSID AND c.trail_rn = 1
      INNER JOIN ENTMOD b
        ON a.TINSID = b.EMODSID
      LEFT JOIN (
        SELECT DISPCODE, ABBREV,
               ROW_NUMBER() OVER (PARTITION BY DISPCODE ORDER BY ROWID) rn
        FROM CLSCODE
      ) cc ON cc.DISPCODE = b.DISPCODE AND cc.rn = 1
      , TABLE (mft_ind_vals (c.tinsid, a.tinfs)) d
    WHERE
      b.STATUS = 'O'
      AND case_org (b.roid) = :org
      AND trunc(b.roid/power(10,8-:elevel)) = :levelValue
  ) t
)
where row_num = 1
