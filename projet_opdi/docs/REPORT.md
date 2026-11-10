# DAT535 Final Project Report — Group 08
## Flight Arrival Efficiency, Fuel Penalties & Machine Learning on European Air Traffic (EUROCONTROL OPDI)

**Course:** DAT535 — Data Engineering | **Institution:** University of Stavanger (UiS)  
**Authors:** Mathis Nancey & Thomas Canisares--Morère (Group 08) | **Date:** Autumn Semester 2026  
**Repository:** `mathis6938/group08` | **Production Run:** `prod-8` (2026-11-09)  

---

## Executive Summary

This report presents an end-to-end distributed data engineering pipeline and an operational aviation study analyzing flight arrival efficiency across European airspace. Operating on **58 consecutive days** of real radar and flight trajectory data from **EUROCONTROL OPDI v0.0.2** and aerodrome master records from **OurAirports** (June 1 – July 28, 2026), the system processes **3,304,717 flights**, **86,670,133 flight events**, and **118,779,222 physical trajectory measurements** (5.46 GB raw data).

Built with **PySpark 3.5** on an Ubuntu virtual machine with constrained resources (4 vCPUs, 7.8 GB RAM), the pipeline implements a robust **Medallion architecture** (Bronze → Silver → Gold) enhanced by dedicated **Machine Learning**, **Data Quarantine**, and **Automated Quality Gate** layers. Beyond descriptive traffic aggregation, this study answers an essential operational and environmental research question:

> **« How much time and fuel do commercial flights lose before landing at Paris Charles-de-Gaulle (LFPG), Toulouse-Blagnac (LFBO), and Stavanger (ENZV) — with London-Heathrow (EGLL) and Frankfurt (EDDF) as international benchmarks — when, why, and what operational interventions could improve this? »**

```
+----------------------------------------------------------------------------------------------------+
|                                    KEY EMPIRICAL FINDINGS (prod-8)                                 |
+----------------------------------------------------------------------------------------------------+
| 1. Heathrow Runway Bottleneck (EGLL):                                                              |
|    - 12.7 % of all arriving flights enter holding patterns (32,802 holding minutes over 58 days).   |
|    - Arriving flights experience an average of 4.9 min of additional arrival time.                 |
|    - Pronounced saturation threshold: delays climb from 0.2 min (<20 arr/h) to 6.8 min (40-49 arr/h).  |
|    - Estimated fuel loss: ~37 tonnes of jet fuel per day (~117 tonnes of CO2/day).                 |
|                                                                                                    |
| 2. Descent Level-Off Inefficiencies at Paris-CDG (LFPG) & Frankfurt (EDDF):                       |
|    - Low airborne holding rates (~2.9 %), but significant vertical step-downs during descent.      |
|    - Aircraft spend 4.6 min (CDG) and 4.1 min (FRA) in level flight during descent per arrival.    |
|    - Estimated daily fuel loss: ~30 tonnes/day at Paris-CDG and ~34 tonnes/day at Frankfurt.       |
|    - Adopting Continuous Descent Operations (CDO) at CDG could save ~11,000 tonnes of fuel/year.   |
|                                                                                                    |
| 3. High Efficiency at Regional Aerodromes (LFBO & ENZV):                                           |
|    - Toulouse-Blagnac displays fluid arrivals (1.9 min additional time, 0.7 % holding rate).       |
|    - Stavanger displays near-perfect continuous descents (0.2 min level-off, 0.2 % holding rate).  |
|                                                                                                    |
| 4. Machine Learning & Peak Demand Smoothing:                                                       |
|    - A Spark MLlib GBTRegressor predicts arrival delays with MAE = 2.74 min, beating baseline.    |
|    - Destination aerodrome layout and airspace explain 55.0 % of arrival delay variance.          |
|    - Counterfactual P90 demand smoothing projects daily savings of 7.0 min at FRA and 6.8 min at LFBO.|
+----------------------------------------------------------------------------------------------------+
```

---

## 1. Introduction & Operational Problem

### 1.1 Context and Motivation
Air traffic across European skies operates under rigorous economic, environmental, and physical constraints. While cruise flight is largely optimized by flight management computers (FMS) for fuel conservation at high altitudes, the **arrival phase**—spanning the transition from cruising flight level down to the runway threshold—is inherently susceptible to tactical controller intervention, terminal airspace bottlenecks, and runway separation constraints.

When airspace demand exceeds runway throughput capacity, air traffic control (ATC) manages the queue using two primary tactical mechanisms:
1. **Airborne Holding Patterns ("Holdings")**: Forcing aircraft to fly racetrack-shaped orbits at assigned altitudes while awaiting landing slots.
2. **Intermediate Level-Offs (Step-Down Descents)**: Interrupting a fuel-efficient continuous glide with extended horizontal flight segments at intermediate altitudes to maintain safe separation from crossing traffic.

Both maneuvers impose heavy environmental and operational penalties:
- **Aerodynamic Drag and Fuel Inefficiency**: At lower altitudes (e.g., between 7,000 and 10,000 feet), the denser atmosphere increases drag. Furthermore, jet turbine engines operate with substantially lower thermal efficiency at low altitudes compared to cruising levels.
- **Excess Emissions**: Every additional minute spent flying level in terminal airspace directly burns extra kerosene and emits greenhouse gases ($3.16\text{ kg CO}_2\text{ per kg of jet fuel}$).
- **Airspace Congestion**: Unplanned tactical delay in the terminal area ripples upstream, causing ground-holding delays across Europe.

### 1.2 The Research Problem
This project evaluates the arrival efficiency across five distinct European airports:

| Airport Code | Airport Name | Country | Operational Role / Airspace Context |
|---|---|---|---|
| **`LFPG`** | Paris Charles-de-Gaulle | France | Major European continental mega-hub; complex dual-parallel runway system. |
| **`LFBO`** | Toulouse-Blagnac | France | Medium regional airport; mixed commercial traffic and Airbus industrial flight tests. |
| **`ENZV`** | Stavanger Sola | Norway | Regional coastal aerodrome serving oil/gas offshore and domestic traffic; home of UiS. |
| **`EGLL`** | London-Heathrow | United Kingdom | Heavily congested international hub; operates at >98 % runway capacity on two runways. |
| **`EDDF`** | Frankfurt Main | Germany | High-capacity European hub; complex multi-runway layout with dedicated landing runways. |

### 1.3 Key Methodological Framework (EUROCONTROL PRU Alignment)
Standard commercial delay metrics (such as airline punctuality statistics) compare actual gate arrival against scheduled gate arrival. However, airline schedules deliberately include variable time buffers (often 15 to 30 minutes) that conceal in-flight tactical delays. 

Furthermore, the **EUROCONTROL OPDI v0.0.2** open dataset provides trajectory radar points and flight milestone events, but contains **no airline schedule data**. Following the official guidelines of the EUROCONTROL Performance Review Unit (PRU):
- **Delay is Defined as Additional Transit Time**: We compare actual transit time through terminal airspace against an empirical, unconstrained ("unimpeded") baseline flight of the same aircraft category flying the same route under light traffic conditions.
- **Statistical Associations, Not Causal Claims**: Because OPDI does not capture real-time weather vectors (e.g., thunderstorms, surface winds) or tactical ATC sector staffing, our findings quantify statistical relationships and operational factors rather than deterministic causality.

---

## 2. System Architecture & Data Engineering

### 2.1 The Medallion Architecture
The data pipeline is designed according to the industry-standard **Medallion Architecture**, structured into a modular Python package (`projet_opdi/src/opdi_pipeline/`) driven by a unified CLI (`run_pipeline.py`) and parameterizable TOML configuration files (`dev.toml` for the 10-day development window, `prod.toml` for the 58-day production window).

```
                            EUROCONTROL OPDI v0.0.2
                     (16 raw files, 5.46 GB, SHA-256 checks)
                                        |
                                        v
+--------------------------------------------------------------------------------+
| 1. INGESTION LAYER (`ingestion.py`)                                            |
|    - HTTP streaming download with exponential backoff                          |
|    - Checksum manifest verification (SHA-256, size, Last-Modified)              |
|    - Automated storage safety threshold (min_free_gb = 4.0 GB)                  |
|    - Republication detection (e.g., OurAirports CSV refresh handling)          |
+--------------------------------------------------------------------------------+
                                        |
                                        v
+--------------------------------------------------------------------------------+
| 2. BRONZE LAYER (`bronze.py`)                                                  |
|    - Verbatim Parquet conversion partitioned by source period                  |
|    - Injected row-level lineage (_source_file, _source_row_num, _ingest_run_id)  |
|    - Incremental State Manager (_state.json): skips unmodified periods          |
|    - 100 % raw-to-Bronze record reconciliation audit                           |
+--------------------------------------------------------------------------------+
                                        |
                                        v
+--------------------------------------------------------------------------------+
| 3. SILVER LAYER (`silver.py` & `arrivals.py`)                                  |
|    - Type standardization, timestamp parsing, coordinate boundary validations   |
|    - Quarantine System: isolates malformed records without dropping data       |
|    - Deduplication & OurAirports relational enrichment                         |
|    - Referential integrity enforcement: drops orphan events without flight     |
|    - Arrivals Sub-Engine: 100 NM ring interpolation, 200 NM descent window,     |
|      fused level-off detector, holding stack classifier                        |
+--------------------------------------------------------------------------------+
                                        |
                                        v
+--------------------------------------------------------------------------------+
| 4. GOLD LAYER (`gold.py` & `sql/*.sql`)                                        |
|    - Declarative Spark SQL star-schema models                                  |
|    - Operational tables: hourly/daily movements, runway usage, ground times    |
|    - Arrival efficiency tables: unimpeded references, daily/hourly indicators  |
|    - 30 automated validation checks; atomic publication with _SUCCESS marker   |
+--------------------------------------------------------------------------------+
                                        |
                                        v
+--------------------------------------------------------------------------------+
| 5. MACHINE LEARNING LAYER (`ml.py`)                                            |
|    - Spark MLlib GBTRegressor for arrival delay explanation                    |
|    - Strict temporal train/test split (no data leakage)                        |
|    - Demand-smoothing policy scenarios (capping hourly demand at P90)          |
|    - KMeans unsupervised airport typology (k=3, silhouette 0.55)               |
+--------------------------------------------------------------------------------+
                                        |
                                        v
+--------------------------------------------------------------------------------+
| 6. SUMMARY & QUALITY GATES (`quality.py`)                                      |
|    - 7 blocking quality gates guaranteeing zero-defect promotion               |
|    - JSON run artifacts written to reports/<run_id>/                           |
+--------------------------------------------------------------------------------+
```

### 2.2 Layer-by-Layer Engineering Rationale

#### Ingestion & Source Reliability (`ingestion.py`)
Source files are retrieved from the EUROCONTROL public repository using HTTP streaming. To prevent partial downloads and corrupted inputs:
- Every file's SHA-256 digest, byte count, and HTTP `Last-Modified` header are recorded in a persistent `manifest.csv`.
- The system checks available disk space before downloading (`min_free_gb = 4.0`), avoiding sudden VM disk exhaustion.
- If a source file is updated upstream with identical naming, the ingestion layer detects the updated SHA-256 hash and flags it for reprocessing (as observed during run `prod-4` with the OurAirports master file).

#### Bronze Layer & Incremental State Management (`bronze.py`)
Bronze tables store source data verbatim in Apache Parquet format. Full data provenance and lineage are maintained by appending system metadata columns to every record: `_source_file`, `_source_row_num`, `_ingest_run_id`, and `_bronze_at_utc`.

To optimize execution runtime, Bronze incorporates an **Incremental State Manager** (`bronze/_state.json`):
- The pipeline hashes source files and tracks already-materialized partitions.
- If a source period is unchanged, Bronze partition building is bypassed.
- **Benchmark Impact**: In run `prod-4` (initial cold build), Bronze table generation required **525.7 seconds**. In final run `prod-8`, because source periods were already processed and validated, the Bronze verification completed in just **0.4 seconds** (a 1,300x acceleration).

#### Silver Layer & Quarantine Engine (`silver.py` & `arrivals.py`)
In real-world data engineering, corrupt or anomalous data must never be silently discarded or allowed to crash downstream analytical jobs. The Silver layer implements a **Quarantine Pattern**:
- Erroneous rows are routed into dedicated quarantine tables (`quarantine_flights` and `quarantine_flight_events`) alongside an explicit diagnostic column `_quarantine_reason`.
- In `prod-8`, 625 flights were quarantined due to `DURATION_GT_24H` (invalid sensor tracking spanning days), and 9,100 events were isolated as `ORPHAN_EVENT` (radar events referencing nonexistent flight IDs).
- The remaining **3,127,830 valid flights** and **84,000,238 valid events** were cleansed, standardized to UTC, and enriched with geographic coordinates and runway headings from OurAirports.

#### Gold Layer & Quality Validation (`gold.py`)
The Gold layer computes dimensional aggregations using declarative Spark SQL queries located in `src/opdi_pipeline/sql/`. The published analytical tables serve downstream dashboards and machine learning models:
- `airport_hourly_ops` (474,130 rows) and `airport_daily_ops` (43,397 rows)
- `runway_usage` (23,500 rows) and `airport_ground_times` (43,397 rows)
- `arrival_reference` (3,046 rows), `airport_arrival_daily` (41,334 rows), and `airport_arrival_hourly` (381,863 rows)
- `holding_hotspots` (20,438 rows), `ml_feature_importance` (10 rows), `ml_scenarios` (5 rows), and `airport_typology` (119 rows).

Before publication, the Gold layer executes **30 automated validation checks** verifying non-negativity, referential consistency, and schema contract adherence. Publication is atomic: tables are registered, and a `_SUCCESS` marker is placed alongside execution metadata in `run_status`.

### 2.3 Comprehensive Row Accounting (Production Run `prod-8`)

The table below documents the end-to-end data lifecycle across all pipeline stages:

| Stage / Layer | Entity / Table Name | Records In | Records Out | Accounting & Filtering Details |
|---|---|---|---|---|
| **Raw Ingest** | Compressed OPDI files | 16 files | 16 files | 5.46 GB total downloaded and checksummed |
| **Bronze** | `flight_list` | 3,304,717 | 3,304,717 | 100 % reconciliation against source CSV |
| **Bronze** | `flight_events` | 86,670,133 | 86,670,133 | 100 % reconciliation against source CSV |
| **Bronze** | `measurements` | 118,779,222 | 118,779,222 | 100 % reconciliation against source CSV |
| **Bronze** | `airports` / `runways` | 86,350 / 48,412 | 86,350 / 48,412 | OurAirports reference tables |
| **Silver** | `flights` | 3,304,717 | **3,127,830** | -175,452 out-of-scope; -810 duplicates; -625 quarantined |
| **Silver** | `flight_events` | 86,670,133 | **84,000,238** | -2,660,795 out-of-scope; -9,100 orphan events quarantined |
| **Silver** | `arrivals` | 3,127,830 | **1,866,643** | Flights with known European destination aerodrome |
| **Silver** | `descent_level_offs` | 84,000,238 | **1,919,728** | Fused level segments ($\ge 30\text{ s}$, $\ge 1,000\text{ ft}$ AGL) |
| **Gold** | European Movements | 3,127,830 | **3,636,308** | 1,767,608 departures + 1,868,700 arrivals at 981 airports |

---

## 3. Performance Engineering & Incident Resolution

### 3.1 Hardware Environment & Production Run Timings
The production pipeline executes on a self-hosted Ubuntu 22.04 LTS virtual machine:
- **Compute**: 4 vCPUs, 7.8 GB physical RAM, 38 GB SSD storage.
- **Spark Runtime**: Apache Spark 3.5.0 in local mode, configured with `spark.driver.memory = 2g`, Adaptive Query Execution (`spark.sql.adaptive.enabled = true`), and UTC timezone.

Total production deployment duration on the full 58-day scope:
- **Baseline Run (`prod-4`)**: 18 min 44 s (before arrival efficiency and ML layers were introduced).
- **Final Production Run (`prod-8`)**: **14 min 43 s** (including arrival indicators, level-off extraction, ML training, scenario simulation, and typology clustering).

```
+--------------------------------------------------------------------------------+
|                        STAGE EXECUTION TIMES (prod-8)                          |
+--------------------------------------------------------------------------------+
| Ingestion:           36.0 s   |  Gold Aggregations:     1 min 8 s              |
| Bronze (incremental): 19.0 s  |  Machine Learning Layer: 1 min 2 s             |
| Silver Layer:   10 min 35 s   |  Summary & Quality:      2.8 s                 |
+--------------------------------------------------------------------------------+
| Total Production Pipeline Duration: 14 min 43 s                                |
+--------------------------------------------------------------------------------+
```

Individual step profiling identified the primary computational bottlenecks in `prod-8`:
1. Writing `silver/flight_events`: **263.9 s** (formatting and filtering 84 million records).
2. Arrival efficiency computation: **215.7 s** (descent level-offs: 153.3 s, arrival transit indicators: 55.8 s).
3. Writing `quarantine/flight_events`: **104.7 s** (known non-critical inefficiency: full rule re-evaluation over 86M rows to isolate 9,100 records).
4. GBT Machine Learning model training: **33.3 s**.
5. Gold movement SQL aggregations: **28.5 s**.

### 3.2 The `prod-5` Out-of-Memory Incident & Resolution

#### The Problem
During deployment run `prod-5`, the pipeline crashed in the Silver arrivals module with an unrecoverable JVM error:
```text
java.lang.OutOfMemoryError: GC overhead limit exceeded
    at org.apache.spark.memory.TaskMemoryManager.allocatePage(...)
```
The job had been running for 13 minutes and exhausted the 2 GB driver heap during intermediate shuffles.

#### Root Cause Analysis
Spark operates with **lazy evaluation**. In the initial implementation of `arrivals.py`, the arrival logic accepted *unmaterialized Spark DataFrames* directly from the preceding Silver cleaning step. 

When computing horizontal 100 NM ring crossings, descent level-off aggregations, and measurement joins, Spark's Catalyst query optimizer re-evaluated the entire upstream lineage tree multiple times across all **86.6 million events**. Furthermore, the code attempted an unpartitioned join against the entire **118.8 million measurements** table, forcing massive data shuffle spills to disk that saturated garbage collection.

```
Initial Flawed Pipeline (prod-5):
[Raw Data] -> [Lazy Silver DF] -> [Recomputed for Ring Crossings]  \
                               -> [Recomputed for Level-Offs]      --> Heavy Shuffle -> GC OOM Crash
                               -> [Joined with 119M Measurements] /

Refactored Architecture (prod-6 & prod-8):
[Raw Data] -> [Persisted Silver Parquet on Disk]
                     |
                     +---> [Direct Read & Spatial Pruning (300 NM)]
                     +---> [Staged Level-Off Extraction & Write]
                     +---> [Single-Key Distance Flown Join]
                     +---> Total Arrival Runtime: 215.7 s (Zero OOM Pressure)
```

#### The Engineering Resolution (PR #12 & PR #13)
The architecture was refactored across four core dimensions:
1. **Decoupled Disk Materialization**: `arrivals.run` was restructured to read strictly from the already-persisted on-disk Silver Parquet files rather than chaining lazy memory transformations.
2. **Early Spatial Pruning**: Events are filtered to a **300 NM bounding circle** around destination aerodromes before computing trajectory metrics, pruning >60 % of unrelated cruise events early.
3. **Targeted Measurement Lookup**: Instead of aggregating the 118.8M measurements table, the pipeline performs a direct join restricted strictly to `type = 'Distance flown (NM)'`.
4. **Staged Level-Off Persisting**: `silver/descent_level_offs` is computed, written to disk, and then read back to assemble final arrival metrics.

Following these changes, memory consumption remained stable well below 1.5 GB heap usage, and run `prod-6` and final run `prod-8` completed without incident.

---

## 4. Arrival Efficiency Study: Methodology & Results

### 4.1 Indicator Definitions

```
                     100 NM Entry Point (t_entry)
                            \
                             \  Descent Phase
                              \ (Continuous vs Level-offs)
                               \
                                v
                           [HOLDING STACK]  (Orbit >= 4 min)
                                |
                                v
                     Runway Threshold Touchdown (t_touchdown)
```

#### Indicator A — Horizontal Additional Arrival Transit Time
Adapted from the EUROCONTROL ASMA (Arrival Sequencing and Metering Area) methodology:
- **100 NM Ring Crossing ($t_{\text{entry}}$)**: The arrival area is bounded by a **100 nautical mile ($185.2\text{ km}$)** circle centered on the aerodrome reference point. The entry timestamp and coordinates are computed via linear interpolation between the last radar point outside the ring and the first point inside.
- **Entry Sector**: Categorized into 8 cardinal octants of 45° (`N, NE, E, SE, S, SW, W, NW`) based on the bearing from the aerodrome to the entry point.
- **Touchdown ($t_{\text{touchdown}}$)**: The earliest `entry-runway` event recorded at the destination airport with altitude $\le \text{aerodrome elevation} + 1,500\text{ ft}$.
- **Unimpeded Reference Baseline ($T_{\text{ref}}$)**: To establish an unconstrained baseline without relying on unavailable airline schedules, the pipeline calculates the **empirical 20th percentile** of actual transit times ($t_{\text{touchdown}} - t_{\text{entry}}$) for homogeneous operational cohorts defined by `(destination, runway, aircraft_class, entry_sector)`. A hierarchical fallback cascade ensures robust statistical samples:
  $$\text{Cohort} \ge 20 \longrightarrow (\text{airport, class, sector}) \ge 20 \longrightarrow (\text{airport, class}) \ge 20 \longrightarrow \text{airport-wide}$$
- **Additional Arrival Transit Time ($\Delta t_{\text{arr}}$)**:
  $$\Delta t_{\text{arr}} = (t_{\text{touchdown}} - t_{\text{entry}}) - T_{\text{ref}}$$
  *(Filtered to plausible flight intervals: $5\text{ min} \le \text{transit} \le 120\text{ min}$)*.

#### Indicator B — Vertical Descent Profile & Holding Stacks
- **200 NM Descent Window**: Analyzes trajectory events within **200 NM ($370.4\text{ km}$)** of the destination, starting from the last point at cruising altitude down to runway touchdown.
- **Level Segment Fusion**: Raw OPDI data chops continuous level flights into short micro-segments (~10 seconds apart). Adjacent chunks (`level-start` to `level-end`) separated by $\le 60\text{ s}$ with altitude differences $\le 200\text{ ft}$ are fused into single contiguous segments. Segments $< 30\text{ s}$ or $< 1,000\text{ ft}$ above airport elevation (ground movements) are filtered out. Level flight is segmented into four altitude bands: `< FL70`, `FL70-100`, `FL100-200`, and `$\ge$ FL200`.
- **Holding Pattern Detection**: A level segment is classified as an airborne holding stack if:
  $$\text{Duration} \ge 4.0\text{ min} \quad \text{AND} \quad \frac{\text{Distance Flown}}{\text{Great-Circle Displacement}} \ge 1.5$$
- **Environmental & Fuel Impact Hypothesis**: Grounded in the *EUROCONTROL 2018 ECAC-wide Continuous Descent Operations (CDO) study* (which measured an average excess burn of 46 kg fuel per flight for 217 s of level flight below top-of-descent):
  $$\text{Fuel Burn Penalty} = 12.7\text{ kg jet fuel per minute of level flight} \quad (\text{CO}_2 = 3.16 \times \text{Fuel})$$
  *(Note: This coefficient represents an ECAC fleet-average order of magnitude, not an individual per-airframe dynamic aerodynamic model)*.

### 4.2 Data Quality & Radar Observability Across Focus Airports

The completeness of arrival indicators depends on sensor coverage in OPDI v0.0.2:

| Airport Code | Airport Name | Total Arrivals | 100 NM Ring Entry % | Runway Entry % | Measured Indicator A % (Flight Count) |
|---|---|---|---|---|---|
| **`LFPG`** | Paris Charles-de-Gaulle | 29,504 | 98.1 % | 26.4 % | **26.2 %** (7,725) |
| **`LFBO`** | Toulouse-Blagnac | 6,265 | 86.3 % | 83.4 % | **77.7 %** (4,866) |
| **`ENZV`** | Stavanger Sola | 4,264 | 47.7 % | 13.9 % | **1.1 %** (48) |
| **`EGLL`** | London-Heathrow | 38,004 | 97.1 % | 14.2 % | **13.8 %** (5,263) |
| **`EDDF`** | Frankfurt Main | 37,768 | 97.1 % | 82.8 % | **81.8 %** (30,900) |

> [!IMPORTANT]
> **Observability Takeaways**:
> - **Frankfurt (`EDDF`)** and **Toulouse (`LFBO`)** exhibit high runway-entry detection rates (>80 %), providing reliable sample sizes for Indicator A.
> - **London-Heathrow (`EGLL`)** and **Paris-CDG (`LFPG`)** suffer from low runway-sensor reporting in OPDI v0.0.2 (13.8 % and 26.2 % coverage, respectively). However, because their arrival volumes are large, absolute sample sizes remain robust (5,263 and 7,725 fully measured flights).
> - **Stavanger (`ENZV`)** has virtually no runway sensor events recorded (only 48 flights / 1.1 % coverage), rendering Indicator A statistically unmeasurable. However, **Indicator B (descent level-offs and holding patterns) is universally observable across all airports (100 % coverage)** because it relies on airborne radar events.

### 4.3 Empirical Findings across Focus Airports

All indicators below are flight-weighted across the full 58-day observation window during complete traffic hours:

| Airport | Additional Arrival Time (A) | Level-Off Min / Arrival (B) | Arrivals with Holding % | Total Holding Time (58 days) | Fuel Penalty Hypothesis |
|---|---|---|---|---|---|
| **Toulouse (`LFBO`)** | **1.9 min** | **1.6 min** | **0.7 %** | 415 min | 2.2 tonnes / day |
| **Frankfurt (`EDDF`)** | **2.7 min** | **4.1 min** | **2.9 %** | 6,356 min | 34 tonnes / day |
| **Paris-CDG (`LFPG`)** | **3.1 min** | **4.6 min** | **2.9 %** | 5,812 min | 30 tonnes / day |
| **London-Heathrow (`EGLL`)** | **4.9 min** *(on 14% measured)* | **4.4 min** | **12.7 %** | **32,802 min** | **37 tonnes / day** |
| **Stavanger (`ENZV`)** | *[Unmeasurable - 48 flights]* | **0.2 min** | **0.2 %** | 214 min | 0.2 tonnes / day |

```
                              HOLDING PATTERN RATE (% OF ARRIVALS)
EGLL (Heathrow)  | [==============================] 12.7 %
LFPG (Paris-CDG) | [======] 2.9 %
EDDF (Frankfurt) | [======] 2.9 %
LFBO (Toulouse)  | [=] 0.7 %
ENZV (Stavanger) | [] 0.2 %

                              LEVEL-OFF MINUTES PER ARRIVAL
LFPG (Paris-CDG) | [======================] 4.6 min
EGLL (Heathrow)  | [=====================] 4.4 min
EDDF (Frankfurt) | [====================] 4.1 min
LFBO (Toulouse)  | [========] 1.6 min
ENZV (Stavanger) | [=] 0.2 min
```

#### Detailed Operational Insights:

##### 1. London-Heathrow (`EGLL`): The Runway Throughput Bottleneck
Heathrow stands out as the most congested arrival environment in European airspace:
- **Dominance in Airborne Holding**: Arriving flights spend **32,802 minutes** in holding patterns over the 58 days. **12.7 % of all arrivals enter a holding pattern** (over four times the rate of Paris or Frankfurt).
- **Terminal Airspace Level-Offs**: Arriving aircraft spend an average of **2.27 minutes** in level flight specifically between `FL70` and `FL100` (intermediate approach capture), reflecting stacking above the terminal area.
- **The Saturation Curve**: Hourly arrival throughput analysis reveals a steep non-linear saturation curve:
  - $< 20\text{ arrivals/h} \implies \text{additional arrival time} \approx 0.1\text{ to }0.2\text{ min}$
  - $20\text{--}29\text{ arrivals/h} \implies 2.4\text{ min}$
  - $30\text{--}39\text{ arrivals/h} \implies 4.5\text{ min}$
  - $40\text{--}49\text{ arrivals/h} \implies \mathbf{6.8\text{ min}}$
- **Diurnal Queuing**: Morning arrival waves (07:00–08:00 UTC) generate peaks of **8 to 10 minutes** of additional arrival time.
- **Spatial Holding Stacks**: Spatial clustering of holding events (`02_holding_hotspots_egll_lfpg.png`) reveals four distinct clusters arranged symmetrically around London in each quadrant, matching the four standard Heathrow arrival holding stacks.

##### 2. Paris-CDG (`LFPG`) & Frankfurt (`EDDF`): The Vertical Step-Down Inefficiency
Paris-CDG and Frankfurt present a fundamentally different operational profile:
- **Low Holding Rates**: Both airports record holding rates of only **2.9 %**, indicating that terminal runway queues rarely back up into airborne holding patterns.
- **High Vertical Step-Down Inefficiency**: Despite minimal holding, arrivals accumulate **4.6 minutes (CDG)** and **4.1 minutes (FRA)** of horizontal level flight during descent.
- **Altitude Distribution**:
  - At Paris-CDG, level-offs are split between low-altitude vectoring (**1.92 min below FL70**) and high-altitude cruise transitions (**1.06 min at FL100-200**, **1.35 min above FL200**).
  - At Frankfurt, step-downs occur primarily below FL70 (**1.36 min**) and above FL200 (**1.46 min**).
- **Environmental Cost**: Under the EUROCONTROL 12.7 kg/min hypothesis, intermediate level segments account for an estimated **30 tonnes of fuel/day at CDG** (~11,000 tonnes of fuel / 34,760 tonnes of $\text{CO}_2$ per year) and **34 tonnes/day at Frankfurt**. Both hubs could achieve substantial environmental savings by transitioning to Continuous Descent Operations (CDO).

##### 3. Toulouse-Blagnac (`LFBO`) & Stavanger (`ENZV`): Fluid Operations
- **Toulouse-Blagnac**: Operates well within terminal capacity. Average additional arrival time is low (**1.9 min**), holding is rare (**0.7 %**), and level flight during descent averages only **1.6 minutes** (resulting in an estimated 2.2 tonnes of fuel loss/day).
- **Stavanger Sola**: Displays near-zero vertical descent penalties (**0.2 min** of level flight, **0.2 %** holding rate, 0.2 tonnes fuel/day). Aircraft descending into Stavanger follow unobstructed continuous descent glides from cruising level down to the runway threshold.

---

## 5. Machine Learning, Feature Importance & Policy Scenarios

### 5.1 GBT Explanatory Delay Model
To quantify the drivers of additional arrival transit time, a machine learning pipeline was implemented using Spark MLlib (`ml.py`). A Gradient-Boosted Trees regressor (`GBTRegressor`, 20 iterations, max depth 5) was trained on individual measured arrivals during complete hours:
- **Feature Set**: `dest_icao`, `entry_sector`, `arrival_runway`, `aircraft_class`, `operator` (top 30 airlines, remainder grouped into `OTHER`), `arrival_hour`, `day_of_week`, `hourly_demand`, `previous_hour_demand`, and `load_factor` (hourly demand divided by the aerodrome's 90th-percentile hourly demand).
- *(Note: `level_off_count` is excluded as a predictor because vertical level segments are part of the physical delay mechanism being explained)*.
- **Strict Temporal Partitioning**: To prevent temporal data leakage, records arriving before **2026-07-14** formed the training dataset (**534,683 arrivals**), while the final 14 days formed the unseen test evaluation set (**200,937 arrivals**).
- **Naive Baseline Benchmark**: A historical reference model computing the historical mean additional delay grouped by `(dest_icao, arrival_hour)`.

#### Model Evaluation & Quality Gate:

$$\begin{aligned}
\text{GBT Model:} \quad & \text{MAE} = \mathbf{2.74\text{ min}}, \quad \text{RMSE} = 4.44\text{ min}, \quad R^2 = 0.135 \\
\text{Naive Baseline:} \quad & \text{MAE} = 2.84\text{ min}, \quad \text{RMSE} = 4.64\text{ min}, \quad R^2 = 0.053
\end{aligned}$$

The GBT model demonstrated a lower Mean Absolute Error than the historical baseline on the unseen test period, successfully passing the automated quality gate for model publication.

At the focal airport level:
- **`LFPG` (Paris-CDG)**: Model MAE 2.65 min vs Baseline 2.66 min
- **`EDDF` (Frankfurt)**: Model MAE 2.53 min vs Baseline 2.56 min
- **`LFBO` (Toulouse)**: Model MAE 1.87 min vs Baseline 1.86 min
- **`EGLL` (Heathrow)**: Model MAE 4.28 min vs Baseline 4.10 min

### 5.2 Feature Importance Analysis
The GBT model assigns feature importances (`gold/ml_feature_importance`) across the input variables:

| Rank | Explanatory Feature | Relative Importance % | Operational Interpretation |
|---|---|---|---|
| **1** | **Destination Aerodrome (`dest_icao`)** | **55.0 %** | Physical layout, runway configuration, and terminal airspace structure dominate arrival transit times. |
| **2** | **Aircraft Wake Class (`aircraft_class`)** | **15.0 %** | Wake vortex separation constraints dictate required spacing behind preceding traffic. |
| **3** | **Arrival Runway (`arrival_runway`)** | **8.0 %** | Specific approach path geometry and runway dependencies. |
| **4** | **Hour of Day (`arrival_hour`)** | **7.0 %** | Diurnal arrival waves and terminal peak traffic periods. |
| **5** | **Airport Load Factor** | **5.0 %** | Proximity of current demand to the airport's 90th-percentile capacity. |
| **6** | **Entry Sector (`entry_sector`)** | **4.0 %** | Route orientation relative to active runway alignment. |
| **7** | **Airline Operator (`operator`)** | **3.0 %** | Fleet composition and operational hub scheduling. |
| **8** | **Hourly Demand (`hourly_demand`)** | **1.0 %** | Raw volume count of concurrent arrivals. |
| **9** | **Previous Hour Demand** | **0.4 %** | Residual queue spillover from preceding hours. |
| **10** | **Day of Week** | **0.1 %** | Weekly traffic periodicity. |

**Key Takeaway**: Over half (55.0 %) of predictable arrival delay is governed purely by the destination aerodrome itself. Delays are structural rather than airline-specific.

### 5.3 Counterfactual Demand-Smoothing Simulation (`gold/ml_scenarios`)
Using the trained GBT model, we simulated a tactical air traffic flow management intervention: capping peak hourly arrival demand at each airport's **90th percentile** and redistributing excess arrivals into adjacent off-peak hours on the same day:

| Airport | Hourly Demand Cap | Rescheduled Flights / Day | Predicted Baseline Delay | Predicted Delay After Cap | Net Daily Savings |
|---|---|---|---|---|---|
| **Frankfurt (`EDDF`)** | 51 arr/h | 7 flights / day | 5,842 min/day | 5,835 min/day | **−7.0 min / day** |
| **Toulouse (`LFBO`)** | 8 arr/h | 2 flights / day | 1,024 min/day | 1,017 min/day | **−6.8 min / day** |
| **Paris-CDG (`LFPG`)** | 35 arr/h | 19 flights / day | 4,218 min/day | 4,216 min/day | **−1.8 min / day** |
| **Heathrow (`EGLL`)** | 45 arr/h | 12 flights / day | 6,104 min/day | 6,105 min/day | **+0.5 min / day** |
| **Stavanger (`ENZV`)** | — | 0 flights / day | — | — | **0.0 min / day** |

> [!NOTE]
> **Analytical Reflection on the Heathrow Simulation**:
> Why does the model predict zero benefit for Heathrow under demand capping (+0.5 min/day), despite the descriptive data showing extreme saturation above 40 arr/h?
> 
> Because the global GBT model was trained over 600+ European airports, and only 13.8 % of Heathrow arrivals had measurable runway timestamps in OPDI, the global trees averaged Heathrow's severe non-linear queuing across the broader, fluid European network. This demonstrates an important principle in data science: **a global regression model can dilute extreme local queuing effects**. The descriptive saturation curve (Figure `04_saturation_curves`) remains the authoritative evidence for Heathrow.

### 5.4 Unsupervised Airport Typology (`gold/airport_typology`)
To classify European aerodromes without manual labeling, a `StandardScaler` and `KMeans` clustering pipeline was executed on **119 representative European airports** ($\ge 100$ measured arrivals and $\ge 30 \%$ runway coverage). Evaluating $k \in [3, 6]$, the optimal silhouette score occurred at **$k = 3$** (silhouette = **0.55**):

```
+------------------------------------------------------------------------------------------------------+
|                                 UNSUPERVISED AIRPORT CLUSTERING (k=3)                                |
+------------------------------------------------------------------------------------------------------+
| Cluster 1: "Large Airports, Fluid Arrivals" (23 airports)                                            |
|   - Characteristics: High daily volume (>300 arr/day), low additional transit times, minimal holding. |
|   - Representative Airports: Frankfurt (EDDF), Amsterdam (EHAM), Madrid (LEMD), Munich (EDDM).       |
|                                                                                                      |
| Cluster 2: "Medium Airports, Fluid Operations" (77 airports)                                         |
|   - Characteristics: Moderate traffic (30-150 arr/day), highly predictable descent profiles.          |
|   - Representative Airports: Toulouse-Blagnac (LFBO).                                                |
|                                                                                                      |
| Cluster 3: "Small Airports, High Additional Time & Procedural Delays" (19 airports)                  |
|   - Characteristics: Constrained regional aerodromes subject to procedural vectoring or terrain.     |
+------------------------------------------------------------------------------------------------------+
```
*(London-Heathrow and Paris-CDG were excluded from this unsupervised clustering due to the 30 % runway sensor coverage threshold)*.

---

## 6. Operational Recommendations

Based strictly on the verified findings of run `prod-8`:

### 1. London-Heathrow (`EGLL`): Implement Extended Arrival Management (E-AMAN)
- **Problem**: Severe runway throughput bottleneck causing widespread low-altitude holding (12.7 % of flights, 32,802 holding minutes) and non-linear delays exceeding 6.8 minutes during peak hours.
- **Intervention**: Prioritize **Extended Arrival Management (E-AMAN)** up to 350 NM away. By instructing en-route aircraft to reduce cruise speed by Mach 0.02–0.04 hundreds of miles before reaching the UK, delays can be absorbed efficiently at high altitude rather than burning fuel in low-altitude terminal holding stacks (`FL70-100`).

### 2. Paris-CDG (`LFPG`) & Frankfurt (`EDDF`): Implement Continuous Descent Operations (CDO)
- **Problem**: Minimal holding queues, but persistent intermediate level-offs during descent (4.6 min at CDG, 4.1 min at FRA), wasting an estimated 30 to 34 tonnes of kerosene daily per hub.
- **Intervention**: Restructure terminal arrival airspace around **Point-Merge Arrival Sequencing** and modernized Arrival Management (AMAN) tools. Allowing aircraft to fly continuous, idle-thrust descent profiles without horizontal step-downs represents potential savings of **~11,000 tonnes of fuel / ~34,760 tonnes of $\text{CO}_2$ per year at Paris-CDG alone**.

### 3. Toulouse-Blagnac (`LFBO`) & Stavanger (`ENZV`): Focus on Ground Operations
- **Problem**: In-flight arrival operations are already highly fluid and efficient (0.2–1.6 min of level flight, holding rate $\le 0.7 \%$).
- **Intervention**: Tactical investments in airborne arrival sequencing would yield negligible returns. Data engineering and optimization efforts should target **taxi-out times and ground turnaround efficiency** rather than airborne trajectory management.

---

## 7. Methodological Limitations

In compliance with academic standards, the following methodological boundaries are acknowledged:
1. **Absence of Airline Timetable Schedules**: OPDI v0.0.2 contains no published schedule data. Delays reflect additional transit time relative to an empirical 20th-percentile baseline, not passenger schedule punctuality.
2. **Omission of Meteorological Factors**: OPDI does not capture wind vectors, visibility, or convective storms. A portion of measured additional transit time reflects headwind components or procedural storm spacing rather than air traffic congestion.
3. **Sensor Coverage Disparities**: While runway detection exceeds 80 % at Frankfurt and Toulouse, it drops to 13.8 % at Heathrow and 1.1 % at Stavanger. While airborne metrics (Indicator B) are 100 % observed, Indicator A carries selection constraints at poorly covered airports.
4. **Empirical Baseline Reference**: A 20th-percentile reference cannot detect delays that affect an entire cohort of flights uniformly across a busy hour.
5. **Fleet-Average Fuel Proxy**: The 12.7 kg/min fuel consumption factor is an ECAC fleet-average benchmark from EUROCONTROL, not a dynamic airframe-specific performance calculation (e.g., Eurocontrol BADA).
6. **Network Source Outage**: A 14-hour European radar data gap occurred on June 9, 2026 (10:00–23:59 UTC). The pipeline's automated regularity gate successfully flagged and excluded 15 incomplete hours from baseline metrics and model training.

---

## 8. Academic Integrity & AI-Use Disclosure

In accordance with course evaluation policies for DAT535 at the University of Stavanger:
- **Thomas Canisares--Morère** utilized **Claude (Anthropic)** as an interactive pair-programming assistant for codebase refactoring, modular package architecture, CI/CD workflow automation, Bronze incremental state logic, Spark SQL queries, synthetic test suite generation, JVM heap optimization during the `prod-5` incident, and analytical framework review.
- **Mathis Nancey** utilized **Google Antigravity (Gemini)** for prototyping arrival indicator calculations, ML feature assembly and pipeline tuning, notebook visualization routines, and drafting documentation.
- **Quality Control Protocol**: All code was independently reviewed, unit-tested (46 tests), executed in CI, and validated on the VM. In early iterations, AI-generated drafts included unverified holding stack identifiers and hallucinated figures; the team instituted a strict policy ensuring **every number in this report is programmatically verified against the production artifacts (`prod-8`)**.

---

## 9. References & Verification Artifacts

- **EUROCONTROL Performance Review Unit (PRU)**: *Methodology for Additional ASMA Time Calculation*, Performance Review Commission.
- **EUROCONTROL Continuous Descent Operations (CDO) Task Force (2018)**: *ECAC-Wide CDO Analysis*, 2017 Traffic Benchmark (12.7 kg/min level flight hypothesis).
- **ICAO Environmental Protection**: *Doc 9988 — Guidance on the Development of States' Action Plans on CO2 Emissions Reduction Activities*.
- **Repository Verification Artifacts**:
  - Codebase: `mathis6938/group08` (Branch `main`).
  - Production Run ID: `prod-8` (Summary: `~/opdi-data/pipeline_summary.json`).
  - Executed Showcase Notebook: [`projet_opdi/notebooks/05_arrival_efficiency.ipynb`](file:///home/mathis/Documents/cours/dataengineering/group08/projet_opdi/notebooks/05_arrival_efficiency.ipynb).
  - High-Resolution Figures:
    - `01_europe_additional_time_and_typology.png`
    - `02_holding_hotspots_egll_lfpg.png`
    - `03_hour_day_heatmaps.png`
    - `04_saturation_curves.png`
    - `05_real_descent_profiles.png`
    - `06_focus_airports.png`
    - `07_model_and_scenario.png`
