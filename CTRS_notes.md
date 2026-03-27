# CTRS — Comprehensive Notes from Meeting Discussions

---

## 1. CTRS ETL Component (Nov 13, 2025 — Meeting with Rick)

This was the most detailed CTRS discussion. The meeting centered on clarifying the status of the **CTRS ETL component**, which had multiple new/unassigned stories with unclear completion status.

### Tickets Under Review

- Retrieve end of month date (with substories)
- Generate group manager list (with substories)
- IQA — report emails sent to area manager
- National end of month file memory
- Retrieving historical EOM reports

### Status at the Time

- **Paul** confirmed development and unit testing were complete; weekly loads were finished months prior
- Items were listed as "in system testing" but testing documentation was sparse — no notes from Ravi
- **Kamal** (SIA Case Assignment) reported code complete but needing production data validation with Samuel and Eric

### Rick's Core Concern

Rick pushed for a clear definition of **"done"** — specifically whether production data validation was part of the completion criteria. He noted that test data will never match production 100%.

### Testing Approach Agreed

1. Receive production data files from Sam daily by 11am
2. Run the job, compare output against legacy
3. Identify and fix query discrepancies same-day
4. Retest immediately
5. **Exit criteria:** 100% match across 2 complete weekly cycles

### Key Challenge

No documented requirements existed for CTRS EOM. Sam was the sole source of knowledge, and he asked the team to **restructure the legacy code in Java** rather than do a straight port — making iterative testing essential.

### CTRS EOM Demo

The CTRS EOM had been **demoed to Sarah twice before** (during Sharon's time). Those recordings were available for reference.

---

## 2. CTRS Calendar Management (Oct 10, 2025 — Meeting with Sarah/Islam)

### The Problem

The CTRS calendar requires **annual updates** with fiscal month data, and at the time it required developer involvement each year. Diane had to do it three times in one cycle due to corrections — a major frustration.

### Required Calendar Data

- Fiscal month
- Posting cycles
- Dates for each week
- Work days per period
- Holidays
- Hours for the recording month

### Proposed Solution

Build a **self-service CTRS calendar** within the system so authorized users (not developers) can manually input calendar data each year. Once updated, the data should **propagate throughout the application automatically**.

### Frequency

The process repeats **12 times per year** (once per month). Sarah sent an email with detailed field requirements.

---

## 3. CTRS End of Month (EOM) Workflow (Oct 14, 2025 — IRS Time Tracking Design)

This was a detailed design discussion on how the EOM process should work in the modernized system.

### Weekly Time Verification Flow

1. Display weeks 1–5 as clickable links (not checkboxes)
2. Clicking a week shows that week's data table on the right
3. "Approve & Next" button per week — auto-marks as reviewed and advances
4. "Complete Weekly Time Verification" stays disabled until all weeks are reviewed
5. After all weeks approved → show **"Generate End of Month"** and **"Approve End of Month"** buttons

### Report Generation

- **Generate EOM** creates **2 reports** (hours + weekly) — downloads immediately
- **Approve EOM** creates **4 additional reports** (6 total) — triggers confirmation message

### Key Data Rule

EOM reports are the **exception to entity not being a historical database**. Once generated and approved, they are **permanently stored** and can be pulled years later. If a time correction is made post-approval, a new EOM overwrites the old one.

### Area EOM (IQA Only)

- 2-digit area number input
- Displays all groups with completion status (incomplete = red with asterisk)
- "Select All" disabled if ANY group is incomplete
- After approval, button changes to "Undo" (IQAs can undo their area for corrections)

### Data Persistence Constraint

Sara stated: **"The information cannot be stored"** — meaning the week-by-week click-through state is session-only. Only the final weekly verification completion is historically recorded.

---

## 4. CTRS as an Action Item (Sept 3, 2025 — ETL Working Session)

In the ETL working session, **CTRS EOM implementation** was listed as an immediate priority action item. Paul had a comprehensive PR ready for review, and the speaker was set to begin CTRS EOM work.

---

## 5. CTRS Weekly Time Verification Reports (Oct 10, 2025)

### Enhancements Discussed

- Make reports **exportable** with a dropdown to choose viewing level
- Add an **"Assignment Number"** input field (not dropdown due to volume at national level)
- Inputting **"0"** displays national-level data
- Hover guidance for input format (area, territory, group, RO codes)
- Reporting month/week fields may be redundant — consider consolidating

---

## Summary of CTRS Workstreams

| Workstream | Owner | Status (as of last discussion) |
|---|---|---|
| CTRS ETL / Weekly Loads | Paul | Dev & unit test complete |
| CTRS EOM Reports | Paul / Ganga / Kamal | System testing, needs production validation |
| CTRS Calendar Management | TBD (design phase) | Requirements from Sarah, needs self-service UI |
| CTRS EOM Workflow UI | Thomas / Brian / Paul | Design agreed, implementation pending |
| CTRS Time Verification Reports | Paul | Enhancement requirements captured |
