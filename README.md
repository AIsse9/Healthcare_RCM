# Healthcare RCM Analytics: Patient Access & Eligibility Leakage

![PySpark](https://img.shields.io/badge/PySpark-Databricks-E25A1C?logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&logoColor=black)
![Status](https://img.shields.io/badge/Stage%201-Complete-brightgreen)

This is a Revenue Cycle Management (RCM) analytics project I built end-to-end in **PySpark (Databricks)** and **Power BI**. The goal was to simulate the kind of eligibility and coverage-leakage analysis a BI Developer would actually deliver for a health plan or provider's Patient Access team.

This is **Stage 1** of a multi-stage RCM portfolio project I'm building out. Additional stages (Denial Analysis, Payment Summary) are in progress, see the [Roadmap](#roadmap) below.

---

## Table of Contents
- [Business Problem](#business-problem)
- [Key Findings](#key-findings)
- [Dashboard](#dashboard)
- [Technical Approach](#technical-approach)
- [Design Decisions](#design-decisions)
- [Tech Stack](#tech-stack)
- [Repo Structure](#repo-structure)
- [Roadmap](#roadmap)

---

## Business Problem

Health plans and providers lose real revenue, and take on compliance risk, when claims get paid for members who weren't actually eligible on their date of service. A member's coverage can terminate mid-cycle, but if Patient Access doesn't catch it at check-in, the visit still gets billed and processed as if coverage were active. By the time anyone notices, the claim has often already been paid, which creates a recoupment/clawback problem downstream.

I built this project to answer three questions a real RCM team asks:

1. **How much coverage leakage do we have?** Visits that occurred when the member wasn't eligible, and how much was paid out on them anyway.
2. **Is our denial documentation clean enough to act on?** Denied claims missing a reason code can't be worked or appealed.
3. **What's actually happening to these ineligible-visit claims?** Are they getting caught (denied) or slipping through (paid)?

---

## Key Findings

| Metric | Result |
|---|---|
| Visits flagged as occurring outside the member's active coverage window | **373** |
| Total dollars paid on claims tied to ineligible visits | **$55,126.94** |
| Denied claims with no documented denial reason | **16** |

**Headline finding:** Of the 373 ineligible-visit claims, the majority were still paid rather than denied. That represents direct financial exposure and a potential recoupment target for the payer. Separately, the 16 undocumented denials are claims that billing staff can't currently work or appeal, since there's no reason code to act on.

---

## Dashboard

I titled this **"Eligibility & Coverage Leakage Dashboard,"** built in Power BI Desktop using Import mode, sourced from gold Delta tables exported as CSV.

![Dashboard Screenshot](screenshots/patient_access_dashboard.png)

- **KPI cards:** Total ineligible visits, total paid claims on ineligible visits, denied claims missing a reason
- **Bar chart:** Member coverage status (Current vs. Terminated)
- **Donut chart:** Distribution of claim outcomes (Paid / Denied / Pending / Under Review) for ineligible-visit claims
- **Detail table:** Member name, member ID, visit ID, visit date, claim status, denial reason, and paid amount, drill-down ready

---

## Technical Approach

**Platform:** Databricks Free Edition (PySpark, Unity Catalog Delta tables)
**Source data:** `members`, `patient_visits`, `claims` tables under `workspace.default`

### 1. Determine coverage status
\`\`\`python
from pyspark.sql import functions as F

df_elig = df_members.withColumn(
    "coverage_status",
    F.when(F.col("termination_date").isNull(), "Current")
     .otherwise("Terminated")
)
\`\`\`

### 2. Data quality checks on registration fields
I used a two-level flagging approach: individual field-level flags, plus a composite roll-up flag.
\`\`\`python
df_dq = df_elig.withColumn(
    "missing_phone", F.col("phone").isNull()
).withColumn(
    "missing_email", F.col("email").isNull()
).withColumn(
    "missing_zip", F.col("zip_code").isNull()
).withColumn(
    "missing_dob", F.col("dob").isNull()
).withColumn(
    "missing_critical_info",
    F.col("missing_phone") | F.col("missing_email") | F.col("missing_zip") | F.col("missing_dob")
)
\`\`\`

### 3. Eligibility-at-time-of-service check
This is the core RCM logic: was the member actually active on their visit date, not just active *today*?
\`\`\`python
df_visits = spark.table("default.patient_visits")

df_check = df_visits.join(df_elig, "member_id") \\
    .withColumn(
        "was_eligible_at_visit",
        F.when(
            (F.col("visit_date") >= F.col("enrollment_date")) &
            (F.col("termination_date").isNull() | (F.col("visit_date") <= F.col("termination_date"))),
            True
        ).otherwise(False)
    )
\`\`\`

### 4. Financial leakage analysis
I joined the ineligible visits to claims to check whether they were paid despite the eligibility gap.
\`\`\`python
v = df_ineligible_visits.alias("v")
c = df_claims.alias("c")

df_leakage_check = v.join(c, v.visit_id == c.visit_id, "left") \\
    .select(
        "v.visit_id", "v.member_id", "v.member_name", "v.visit_date",
        "c.claim_status", "c.denial_reason", "c.paid_amount"
    )

df_leakage_summary = df_leakage_check.groupBy("claim_status").agg(
    F.count("*").alias("claim_count"),
    F.sum(F.coalesce(F.col("paid_amount"), F.lit(0.0))).alias("total_paid_amount")
)
\`\`\`

### 5. Denial documentation gap check
\`\`\`python
df_leakage_check.filter(
    (F.col("claim_status") == "Denied") & (F.col("denial_reason").isNull())
).select("visit_id", "member_id", "claim_status").display()
\`\`\`

### 6. Gold table export (for Power BI consumption)
I exported the Delta tables to Unity Catalog Volumes as CSV, then loaded them into Power BI Desktop using Import mode:
- `gold_coverage_status_summary.csv`
- `gold_data_quality_summary.csv`
- `gold_eligibility_leakage_detail.csv`
- `gold_eligibility_leakage_summary.csv`

---

## Design Decisions

**I never deleted anomalous records.** Members with missing registration fields and visits flagged as ineligible were retained and investigated, not filtered out. In an RCM context, these aren't bad data, they're the audit trail. Deleting them would erase the exact evidence a BI dashboard exists to surface, and would break referential integrity across `claims`, `visits`, `diagnoses`, and other tables keyed on `member_id`.

**Eligibility is deterministic, not estimated.** Every flag is a row-level date comparison against the member's actual enrollment and termination dates, not an aggregate or a guess.

---

## Tech Stack

- **PySpark** (Databricks Free Edition, Unity Catalog Delta tables)
- **Power BI Desktop** (Import mode via CSV, since Free Edition doesn't expose a SQL Warehouse endpoint for direct connection)
- **GitHub** for version control

---

## Repo Structure

\`\`\`
Healthcare_RCM/
├── README.md
├── notebooks/
│   └── Healthcare_Data_Insights.ipynb      # PySpark analysis, all stages
├── data/
│   └── gold/                                # Exported gold Delta tables (CSV)
│       ├── gold_coverage_status_summary.csv
│       ├── gold_data_quality_summary.csv
│       ├── gold_eligibility_leakage_detail.csv
│       └── gold_eligibility_leakage_summary.csv
├── dashboards/
│   └── RCM_Project.pbix                     # Power BI Desktop file
└── screenshots/
    └── patient_access_dashboard.png
\`\`\`

---

## Roadmap

- [x] Patient Access / Eligibility (this stage)
- [ ] Claims Adjudication / Denial Analysis (denial rate, Days in A/R, net collection rate)
- [ ] Payment Summary (net collection rate, monthly trend)
- [ ] Consolidated one-page Power BI dashboard across all stages

---

## Author

Built by Michael as part of an RCM/BI Developer portfolio project.
[GitHub Repo](https://github.com/Alsse9/Healthcare_RCM)
