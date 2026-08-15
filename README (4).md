# Healthcare RCM Analytics: Eligibility, Leakage & Denial Performance

![PySpark](https://img.shields.io/badge/PySpark-Databricks-orange) ![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-yellow) ![Stage 1](https://img.shields.io/badge/Stage%201-Complete-brightgreen) ![Stage 2](https://img.shields.io/badge/Stage%202-Complete-brightgreen)

This is a Revenue Cycle Management (RCM) analytics project I built end to end in PySpark (Databricks) and Power BI. The goal was to simulate the kind of eligibility, leakage, and denial analysis a BI Developer would actually deliver for a health plan or provider's Patient Access and Claims teams.

This is a multi-stage RCM portfolio project. Stage 1 (Patient Access & Eligibility Leakage) and Stage 2 (Claims Denial & Financial Performance) are complete. A consolidated cross-stage dashboard is in progress, see the Roadmap below.

## Table of Contents
- [Business Problem](#business-problem)
- [Stage 1: Patient Access & Eligibility Leakage](#stage-1-patient-access--eligibility-leakage)
- [Stage 2: Claims Denial & Financial Performance](#stage-2-claims-denial--financial-performance)
- [Design Decisions](#design-decisions)
- [Tech Stack](#tech-stack)
- [Repo Structure](#repo-structure)
- [Roadmap](#roadmap)

## Business Problem

Health plans and providers lose real revenue, and take on compliance risk, at two different points in the claims lifecycle. First, at Patient Access: a member's coverage can terminate mid-cycle, but if it isn't caught at check-in, the visit still gets billed as if coverage were active, sometimes it doesn't even generate a claim at all. Second, at Claims Adjudication: even claims from eligible members get denied, underpaid, or written off at rates that erode collections, and if the reasons behind that aren't tracked and understood, the same losses repeat month after month.

I built this project to answer the questions a real RCM team asks at both stages:

- How much coverage leakage do we have, and how much of it never even reached billing?
- What's our denial rate, and how fast are we collecting on the claims we do file?
- Where are denials concentrated, which procedures, which reasons, and is it getting better or worse over time?

---

## Stage 1: Patient Access & Eligibility Leakage

### Key Findings

| Metric | Result |
|---|---|
| Visits flagged as occurring outside the member's active coverage window | 373 (out of 1,895 total visits, 19.7%) |
| Total dollars paid on claims tied to ineligible visits | $55,126.94 |
| Ineligible visits that never generated a claim at all | 59 |
| Denied claims (within the ineligible-visit subset) with no documented denial reason | 16 |

**Headline finding:** of the 373 ineligible-visit claims, the majority were still paid rather than denied, direct financial exposure and a potential recoupment target. Separately, 59 of those ineligible visits never generated a claim in the first place, meaning they weren't just paid incorrectly, they escaped billing review entirely. That's arguably a more serious gap than a wrongly-paid claim: a paid claim at least shows up somewhere to investigate, a visit that never became a claim can slip through invisibly.

### Dashboard

Titled "Eligibility & Coverage Leakage Dashboard," built in Power BI Desktop using Import mode, sourced from gold Delta tables exported as CSV.

`screenshots/patient_access_dashboard.png`

- KPI cards: Total ineligible visits, total paid claims on ineligible visits, denied claims missing a reason
- Bar chart: Member coverage status (Current vs. Terminated)
- Donut chart: Distribution of claim outcomes (Paid / Denied / Pending / Under Review / **No Claim Filed**) for ineligible-visit claims
- Detail table: Member name, member ID, visit ID, visit date, claim status, denial reason, and paid amount, drill-down ready
- Callout text tying the KPIs together: *"373 of 1,895 total visits (19.7%) occurred outside active coverage windows, resulting in $55,126.94 paid on claims that should have been caught at check-in."*

### Technical Approach

Platform: Databricks Free Edition (PySpark, Unity Catalog Delta tables). Source data: `members`, `patient_visits`, `claims` tables under `workspace.default`.

<details>
<summary><b>1. Determine coverage status</b></summary>

```python
from pyspark.sql import functions as F

df_elig = df_members.withColumn(
    "coverage_status",
    F.when(F.col("termination_date").isNull(), "Current")
     .otherwise("Terminated")
)
```
</details>

<details>
<summary><b>2. Data quality checks on registration fields</b></summary>

I used a two-level flagging approach: individual field-level flags, plus a composite roll-up flag.

```python
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
```

No members were flagged with missing critical info, a clean result worth noting rather than a chart worth building.
</details>

<details>
<summary><b>3. Eligibility-at-time-of-service check</b></summary>

This is the core RCM logic: was the member actually active on their visit date, not just active today.

```python
df_visits = spark.table("default.patient_visits")

df_check = df_visits.join(df_elig, "member_id") \
    .withColumn(
        "was_eligible_at_visit",
        F.when(
            (F.col("visit_date") >= F.col("enrollment_date")) &
            (F.col("termination_date").isNull() | (F.col("visit_date") <= F.col("termination_date"))),
            True
        ).otherwise(False)
    )
```
</details>

<details>
<summary><b>4. Financial leakage analysis</b></summary>

I joined the ineligible visits to claims to check whether they were paid despite the eligibility gap. I used a left join intentionally, some ineligible visits never had a matching claim at all, and that gap is itself a finding, not something to filter out.

```python
v = df_ineligible_visits.alias("v")
c = df_claims.alias("c")

df_leakage_check = v.join(c, v.visit_id == c.visit_id, "left") \
    .select(
        "v.visit_id", "v.member_id", "v.member_name", "v.visit_date",
        "c.claim_status", "c.denial_reason", "c.paid_amount"
    )
```

**Data quality catch:** the left join produces nulls in `claim_status` for visits with no matching claim. Left unhandled, those nulls silently shift a Power BI legend, every category after the blank slice gets mislabeled by one position. I caught this because a donut chart slice rendered with no label, which happened to also throw off which color mapped to which category. Fixed with `coalesce`, giving unmatched visits an explicit, honest label rather than leaving them blank:

```python
df_leakage_check_clean = df_leakage_check.withColumn(
    "claim_status",
    F.coalesce(F.col("claim_status"), F.lit("No Claim Filed"))
)

df_leakage_summary = df_leakage_check_clean.groupBy("claim_status").agg(
    F.count("*").alias("claim_count"),
    F.sum(F.coalesce(F.col("paid_amount"), F.lit(0.0))).alias("total_paid_amount")
)
```
</details>

<details>
<summary><b>5. Denial documentation gap check</b></summary>

```python
df_leakage_check_clean.filter(
    (F.col("claim_status") == "Denied") & (F.col("denial_reason").isNull())
).select("visit_id", "member_id", "claim_status").display()
```
</details>

<details>
<summary><b>6. Gold table export</b></summary>

Exported to Unity Catalog Volumes as CSV, loaded into Power BI Desktop using Import mode:

- `gold_coverage_status_summary.csv`
- `gold_data_quality_summary.csv`
- `gold_eligibility_leakage_detail.csv`
- `gold_eligibility_leakage_summary.csv`
</details>

---

## Stage 2: Claims Denial & Financial Performance

### Key Findings

| Metric | Result |
|---|---|
| Denial rate | 13.21% (194 of 1,469 claims) |
| Average Days in A/R (Paid claims) | 24 days (median 24, tight distribution) |
| Net collection rate | 59.29% ($244,524.70 paid of $412,444.34 allowed) |
| Total write-off variance | $19,859.35 across 925 of 943 Paid claims |

**Headline finding:** denial reasons connect directly back to Stage 1, "Member not eligible on date of service" is a top-4 denial reason across the full claims population, independent confirmation that the eligibility gap found in Stage 1 isn't isolated, it's a recurring, measurable driver of denials. Separately, denials concentrate around complex or remote-care visit types (ER high-complexity, inpatient, telehealth) rather than routine diagnostics, pointing to a specific, actionable focus area for documentation improvement rather than a blanket process fix.

**Data limitation, documented rather than worked around:** Denied claims have no `claim_approved_date` or `claim_paid_date`, there's no `claim_denied_date` field in the source data at all. As a result, Days in A/R can only be calculated for Paid claims. This is a genuine gap in the source data, not a bug in the logic.

### Dashboard

Titled "Claims Denial & Financial Performance Dashboard," same Power BI Desktop / Import mode setup as Stage 1.

`screenshots/denial_analysis_dashboard.png`

- KPI cards: Denial Rate, Net Collection Rate, Net Claims Writeoff, Average Days in A/R
- Funnel bar: Claimed → Allowed → Paid, showing the full dollar drop-off across the claims lifecycle
- Line chart: Denial rate trend by month, filterable by year
- Bar chart: Total claims by denial reason
- Bar chart: Average denial rate by procedure (CPT code), with procedure name shown on hover via tooltip

### Technical Approach

Source data: `claims`, `claim_procedures` tables under `workspace.default`.

<details>
<summary><b>1. Denial rate</b></summary>

```python
df_denial_rate = df_claims.agg(
    F.count("*").alias("total_claims"),
    F.sum(F.when(F.col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims")
).withColumn(
    "denial_rate_pct",
    F.round(F.col("denied_claims") / F.col("total_claims") * 100, 2)
)
```
</details>

<details>
<summary><b>2. Days in A/R (Paid claims only)</b></summary>

```python
df_ar = df_claims.filter(F.col("claim_status") == "Paid") \
    .withColumn("days_in_ar", F.datediff(F.col("claim_paid_date"), F.col("claim_date")))

df_ar_summary = df_ar.agg(
    F.round(F.avg("days_in_ar"), 2).alias("avg_days_in_ar"),
    F.expr("percentile_approx(days_in_ar, 0.5)").alias("median_days_in_ar")
)
```
</details>

<details>
<summary><b>3. Net collection rate</b></summary>

Calculated against the full claim population, not just Paid claims, since the point is to measure how much of everything owed was actually collected, including claims still stuck in denial or pending.

```python
df_ncr = df_claims.agg(
    F.sum(F.coalesce(F.col("paid_amount"), F.lit(0.0))).alias("total_paid"),
    F.sum(F.col("allowed_amount")).alias("total_allowed")
).withColumn(
    "net_collection_rate_pct",
    F.round(F.col("total_paid") / F.col("total_allowed") * 100, 2)
)
```
</details>

<details>
<summary><b>4. Write-off variance</b></summary>

```python
df_writeoff = df_claims.filter(F.col("claim_status") == "Paid") \
    .withColumn("writeoff_amount", F.col("allowed_amount") - F.col("paid_amount")) \
    .agg(
        F.round(F.sum("writeoff_amount"), 2).alias("total_writeoff"),
        F.round(F.avg("writeoff_amount"), 2).alias("avg_writeoff_per_claim"),
        F.count(F.when(F.col("writeoff_amount") > 0, 1)).alias("claims_with_writeoff")
    )
```
</details>

<details>
<summary><b>5. Consolidated summary table (controlled crossJoin)</b></summary>

All four metrics land as single-row aggregates, so I combined them into one wide gold row using `crossJoin`. This is safe only because each intermediate DataFrame is confirmed single-row, a real cross join across multi-row tables would multiply every row against every other row, which is not what's happening here.

```python
df_denial_analysis_summary = denial_metrics.crossJoin(ar_metrics) \
    .crossJoin(ncr_metrics) \
    .crossJoin(writeoff_metrics) \
    .select(
        "total_claims", "denied_claims", "denial_rate_pct",
        "avg_days_in_ar", "median_days_in_ar",
        "total_paid", "total_allowed", "net_collection_rate_pct",
        "total_writeoff", "avg_writeoff_per_claim", "claims_with_writeoff"
    )
```
</details>

<details>
<summary><b>6. Denial rate by procedure (CPT code)</b></summary>

`claim_procedures` had inconsistent free-text `procedure_name` values against the same `cpt_code` (e.g. two different labels for CPT 99215). I grouped by the standardized code and used `F.first()` to resolve the naming inconsistency, a real data quality finding worth surfacing on its own.

```python
cp = df_proc.alias("cp")
c = df_claims.alias("c")

df_procedure_denials = cp.join(c, cp.claim_id == c.claim_id, "left") \
    .select("cp.claim_id", "cp.cpt_code", "cp.procedure_name", "c.claim_status") \
    .dropDuplicates(["claim_id", "cpt_code"]) \
    .groupBy("cpt_code") \
    .agg(
        F.first("procedure_name").alias("procedure_name"),
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims")
    ) \
    .withColumn("denial_rate_pct", F.round(F.col("denied_claims") / F.col("total_claims") * 100, 2)) \
    .orderBy(F.col("denial_rate_pct").desc())
```
</details>

<details>
<summary><b>7. Denial reason breakdown</b></summary>

```python
df_denial_reasons = df_claims.filter(
    (F.col("claim_status") == "Denied") & (F.col("denial_reason").isNotNull())
).groupBy("denial_reason").agg(
    F.count("*").alias("claim_count")
).orderBy(F.col("claim_count").desc())
```
</details>

<details>
<summary><b>8. Monthly claims volume & denial rate trend</b></summary>

```python
df_monthly_trend = df_claims.withColumn(
    "claim_month", F.date_format(F.col("claim_date"), "yyyy-MM")
).groupBy("claim_month").agg(
    F.count("*").alias("total_claims"),
    F.sum(F.when(F.col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims")
).withColumn(
    "denial_rate_pct", F.round(F.col("denied_claims") / F.col("total_claims") * 100, 2)
).orderBy("claim_month")
```
</details>

<details>
<summary><b>9. Claimed vs. Allowed vs. Paid funnel</b></summary>

Built in long format, one row per stage, since that's the shape Power BI needs for a simple categorical bar/funnel visual.

```python
totals = df_claims.agg(
    F.sum("claimed_amount").alias("total_claimed"),
    F.sum("allowed_amount").alias("total_allowed"),
    F.sum(F.coalesce(F.col("paid_amount"), F.lit(0.0))).alias("total_paid")
).collect()[0]

df_funnel = spark.createDataFrame([
    ("Claimed", totals["total_claimed"]),
    ("Allowed", totals["total_allowed"]),
    ("Paid", totals["total_paid"])
], ["stage", "amount"])
```
</details>

<details>
<summary><b>10. Gold table export</b></summary>

- `gold_denial_analysis_summary.csv`
- `gold_procedure_denial_rates.csv`
- `gold_denial_reasons.csv`
- `gold_monthly_claims_trend.csv`
- `gold_claims_funnel.csv`
</details>

---

## Design Decisions

**I never deleted anomalous records.** Members with missing registration fields and visits flagged as ineligible were retained and investigated, not filtered out. In an RCM context, these aren't bad data, they're the audit trail. Deleting them would erase the exact evidence a BI dashboard exists to surface, and would break referential integrity across claims, visits, diagnoses, and other tables keyed on `member_id`.

**Eligibility is deterministic, not estimated.** Every flag is a row-level date comparison against the member's actual enrollment and termination dates, not an aggregate or a guess.

**Nulls get labeled, not hidden.** Whether it was ineligible visits with no matching claim (Stage 1) or denied claims with no denial date (Stage 2), the approach was the same: surface the gap explicitly, `coalesce` to a real label or document the limitation, rather than letting a blank value silently distort a chart or get filtered out.

**`crossJoin` is safe only on confirmed single-row DataFrames.** Used deliberately in Stage 2 to stitch four single-row metric summaries into one gold row, not as a general-purpose join pattern.

## Tech Stack

- PySpark (Databricks Free Edition, Unity Catalog Delta tables)
- Power BI Desktop (Import mode via CSV, since Free Edition doesn't expose a SQL Warehouse endpoint for direct connection)
- GitHub for version control

## Repo Structure

```
Healthcare_RCM/
├── README.md
├── Healthcare_Data_Insights.ipynb      # PySpark analysis, all stages
└── screenshots/
    ├── patient_access_dashboard.png
    └── denial_analysis_dashboard.png
```

## Roadmap

- [x] Patient Access / Eligibility (Stage 1)
- [x] Claims Adjudication / Denial Analysis (Stage 2)
- [ ] Payment Summary (Stage 3, net collection rate, monthly trend)
- [ ] Consolidated one-page Power BI dashboard across all stages

---

**Author:** Ahmed Isse | [GitHub Repo](https://github.com/Alsse9/Healthcare_RCM)
