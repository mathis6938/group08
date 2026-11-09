# DAT535 Final Project Report — Group 08
## Flight Arrival Efficiency & Machine Learning on EUROCONTROL OPDI Data
**Course:** DAT535 — Data Engineering | **Institution:** University of Stavanger (UiS)  
**Authors:** Mathis & Thomas (Group 08) | **Date:** Autumn 2026  
**Repository:** `mathis6938/group08` | **Production Run:** `prod-8` (2026-11-09)  

---

## Executive Summary

This project delivers an end-to-end, industrial-grade data engineering pipeline and an operational study investigating flight arrival efficiency across European airspace. Built with **PySpark 3.5** following a six-layer Medallion architecture (Ingestion, Bronze, Silver, Gold, ML, and Summary), the system processes **58 days** of real air traffic data from **EUROCONTROL OPDI v0.0.2** and **OurAirports** (June 1 – July 28, 2026), covering **3,304,717 flights**, **86,670,133 flight events**, and **118,779,222 physical trajectory measurements** (5.46 GB raw).

Beyond descriptive flight tracking, the project addresses a concrete operational question inspired by EUROCONTROL Performance Review Unit (PRU) methodologies:
> *« How much time and fuel do flights lose before landing at Paris-CDG (LFPG), Toulouse-Blagnac (LFBO) and Stavanger (ENZV) — with London-Heathrow (EGLL) and Frankfurt (EDDF) as international benchmarks — when, why, and what could be improved? »*

### Key Verified Findings (Production Run `prod-8`):
1. **London-Heathrow (EGLL)** is heavily constrained by runway throughput saturation: arriving aircraft spend on average **4.9 min** of additional arrival time (on the 14 % measured arrivals) and **12.7 %** of flights enter a holding pattern (totalling **32,802 holding minutes** over 58 days). Additional arrival time exhibits a steep non-linear saturation curve, climbing from 0.1–0.2 min at low demand (< 20 arrivals/h) to **6.8 min** when hourly arrivals reach 40–49/h.
2. **Paris-CDG (LFPG)** and **Frankfurt (EDDF)** experience limited holding (2.9 % of flights each), but suffer from substantial vertical inefficiencies, accumulating **4.6 min** and **4.1 min** of level flight during descent per arrival, respectively. Under the EUROCONTROL 12.7 kg/min level flight hypothesis, this equates to estimated losses of **30 tonnes of fuel per day at CDG** and **34 tonnes/day at Frankfurt**.
3. **Toulouse-Blagnac (LFBO)** displays highly fluid operations with **1.9 min** of additional time, **1.6 min** of level flight, and only **0.7 %** holding (2.2 tonnes of fuel/day).
4. **Stavanger (ENZV)** benefits from excellent vertical descent efficiency (**0.2 min** of level flight, **0.2 %** holding, 0.2 tonnes of fuel/day), though horizontal transit time is not statistically measurable due to low destination runway coverage (1.1 % / 48 flights).
5. **Machine Learning Layer**: A Spark MLlib `GBTRegressor` explains arrival delay with a test MAE of **2.74 min** (R² = 0.135), beating the naive baseline (MAE 2.84 min, R² = 0.053). A peak-demand smoothing policy scenario (capping hourly demand at the 90th percentile) predicts potential savings of **7.0 min/day at EDDF**, **6.8 min/day at LFBO**, and **1.8 min/day at LFPG**. An unsupervised `KMeans` clustering ($k=3$, silhouette 0.55) on 119 representative European airports isolates large fluid hubs, medium fluid airports, and delay-prone regional facilities.

---

## 1. Introduction & Operational Problem

Air traffic management in Europe faces twin pressures: accommodating rising traffic density while minimizing fuel burn and carbon emissions. The arrival phase—from cruising altitude down to touchdown—is critically sensitive to airspace bottlenecks, controller intervention, and terminal manoeuvring constraints.

Traditional flight delay indicators rely on scheduled airline timetables (comparing actual vs scheduled on-block times). However, scheduled buffers frequently mask in-flight holding and airspace congestion. Crucially, the EUROCONTROL OPDI dataset does **not** contain scheduled times. Consequently, this study adopts the performance framework of the EUROCONTROL Performance Review Unit (PRU):
- **No commercial schedule delay**: We quantify **additional transit time** relative to an empirical, unconstrained ("unimpeded") reference flight under nominal conditions.
- **Statistical associations, not causal assertions**: Because atmospheric wind, convective weather, and tactical air traffic control sector staffing are unobserved in OPDI, all findings represent empirical associations and operational factors.
- **Scope of focal airports**:
  - *Primary focus*: **Paris-CDG (`LFPG`)** (major continental dual-pair hub), **Toulouse-Blagnac (`LFBO`)** (medium French mixed industrial/commercial airport), and **Stavanger (`ENZV`)** (regional Norwegian coastal airport, home of UiS).
  - *International benchmarks*: **London-Heathrow (`EGLL`)** (severely slot-constrained dual-runway hub) and **Frankfurt (`EDDF`)** (high-capacity German hub).

---

## 2. Architecture & Data Engineering

The pipeline is implemented as a modular Python package (`projet_opdi/src/opdi_pipeline/`) driven by a central CLI (`run_pipeline.py`) and configured via TOML specifications (`config/dev.toml` for the 10-day development window and `config/prod.toml` for the full 58-day scope).

```
                      +---------------------------------------+
                      |       EUROCONTROL OPDI v0.0.2         |
                      | 16 raw files (5.46 GB, SHA-256 check) |
                      +---------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| 1/6 INGESTION: HTTP download with retry, MD5/SHA-256 manifests, min_free_gb check  |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| 2/6 BRONZE: Raw Parquet with row-level lineage (_source_file, _source_row_num,    |
|             _ingest_run_id, _bronze_at_utc). Incremental partition builds.        |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| 3/6 SILVER: Standardisation, quality rules, quarantine tables, deduplication,     |
|             OurAirports join, referential integrity.                              |
|             Arrivals Sub-pipeline: 100 NM ring entry, 200 NM descent window,      |
|             fused level-offs, holding detection, fuel/CO2 conversions.            |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| 4/6 GOLD: Star-schema Spark SQL models (hourly/daily ops, runway usage, ground    |
|           times, 100 NM arrival indicators, unimpeded references, hotspots).      |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| 5/6 ML: GBTRegressor arrival model (strict temporal split, baseline comparison),  |
|         demand smoothing policy scenarios, KMeans airport typology.               |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
| 6/6 SUMMARY: 7 blocking quality gates, schema contracts, JSON run reports.        |
+-----------------------------------------------------------------------------------+
```

### 2.1 Layer Breakdown & Engineering Decisions

1. **Ingestion Layer (`ingestion.py`)**:
   - Manages HTTP downloads with exponential backoff retries and SHA-256 checksum verification against a persistent manifest.
   - Enforces an automated disk space quota (`min_free_gb = 4.0`), preventing VM crashes during large downloads.
   - Detects republished source files (demonstrated in run `prod-4` when OurAirports reference files were republished and automatically re-ingested).

2. **Bronze Layer (`bronze.py`)**:
   - Preserves source data verbatim in Apache Parquet format.
   - Enforces full row-level lineage via metadata injection (`_source_file`, `_source_row_num`, `_ingest_run_id`, `_bronze_at_utc`).
   - Implements an **incremental state manager** (`bronze/_state.json`): previously ingested, unchanged source periods are skipped during pipeline reruns. In `prod-4` (initial cold build), Bronze table generation required **525.7 s**; in `prod-8`, because source files were already processed, Bronze verification completed in **0.4 s**.
   - Guarantees 100 % source-to-Bronze reconciliation across all 16/16 files.

3. **Silver Layer (`silver.py` & `arrivals.py`)**:
   - Cleanses timestamps, casts coordinates, and enforces valid coordinate bounds.
   - **Quarantine Engine**: Erroneous data is never dropped silently. In `prod-8`, 625 flights were routed to `quarantine_flights` (due to `DURATION_GT_24H`) and 9,100 events were routed to `quarantine_flight_events` (due to `ORPHAN_EVENT` without matching flight headers).
   - Generates the core arrival tables: `silver/arrivals` (1,866,643 rows) and `silver/descent_level_offs` (1,919,728 rows).

4. **Gold Layer (`gold.py` & `sql/*.sql`)**:
   - Computes analytical aggregations via declarative Spark SQL queries.
   - Published tables: `airport_hourly_ops` (474,130 rows), `airport_daily_ops` (43,397 rows), `runway_usage` (23,500 rows), `airport_ground_times` (43,397 rows), `arrival_reference` (3,046 rows), `airport_arrival_daily` (41,334 rows), `airport_arrival_hourly` (381,863 rows), and `holding_hotspots` (20,438 rows).
   - Execution is guarded by 30 validation checks verifying non-negativity, referential consistency, and schema contract compliance.

5. **Machine Learning Layer (`ml.py`)**:
   - Trains a `GBTRegressor` pipeline on complete-hour measured arrivals, evaluates feature importances, simulates demand-capping policies, and clusters European airports via `KMeans`.

6. **Summary & Quality Gates (`quality.py`)**:
   - Evaluates **7 strict quality gates**:
     1. Bronze raw files match schema contracts.
     2. Raw-to-Bronze record reconciliation is exact.
     3. Silver row accounting is consistent.
     4. Silver referential integrity has no orphan events.
     5. Silver tables match schema contracts.
     6. Gold validation checks pass 100 %.
     7. Gold publication atomically places the `_SUCCESS` marker and records execution history.
   - Writes consolidated machine-readable JSON reports into `reports/<run_id>/`.

### 2.2 Data Quality & Source Anomalies
- **Missing Identifiers in OPDI v0.0.2**: Origin airport (`adep`) is missing for **43.5 %** of flights; destination (`ades`) is missing for **40.3 %**; both airports are known for only **38.5 %**.
- **Coordinate & Event Integrity**: Altitude is valid for **99.95 %** of events. **48.8 %** of movement times originate from explicit airport sensor events (the remainder derived from first/last radar contact). Runway identifiers are available for **43.3 %** of movements, of which **99.2 %** successfully match OurAirports reference runways.
- **Major Network Source Gap**: A complete European data blackout occurred on **June 9, 2026, from 10:00 to 23:59 UTC** (14 consecutive hours with zero movements), alongside an abnormal traffic drop on **June 6, 2026 at 19:00 UTC** (390 movements vs 2,752 European median). The pipeline's automated regularity audit flagged **15 incomplete hours** (< 50 % of median European traffic), systematically excluding them from baseline calculations and model training.

---

## 3. Performance Engineering & Incident Resolution

### 3.1 Hardware Environment & Runtime Benchmarks
The production pipeline runs on an Ubuntu VM configured with **4 vCPUs, 7.8 GB RAM, and 38 GB disk space**, executing PySpark 3.5 in local mode with a **2 GB driver memory**.

Total production run durations (deployment job on full scope, excluding independent pytest runners):
- **Run `prod-4`** (initial baseline build, before arrival efficiency study): **18 min 44 s**.
- **Run `prod-8`** (final complete production pipeline with ML): **14 min 43 s**.
  - *Ingestion*: 36 s
  - *Bronze*: 19 s
  - *Silver*: 10 min 35 s
  - *Gold*: 1 min 8 s
  - *ML*: 1 min 2 s

Slowest individual steps in `prod-8`:
- Writing `silver/flight_events`: **263.9 s**
- Arrival study total: **215.7 s** (descent level-offs: 153.3 s, arrival transit indicators: 55.8 s)
- Writing `quarantine/flight_events`: **104.7 s** (identified bottleneck: recomputes event rules over the full dataset to extract 9,100 quarantined rows; unoptimized but acceptable)
- GBT Model training: **33.3 s**
- Gold movement aggregations: **28.5 s**

### 3.2 The `prod-5` Out-of-Memory Incident & Resolution
During production deployment run `prod-5`, the pipeline failed in the Silver arrival step with:
`java.lang.OutOfMemoryError: GC overhead limit exceeded` after 13 minutes of execution.

**Root Cause Analysis**:
The arrival logic in `arrivals.py` received *lazy Spark DataFrames* directly from preceding transformation steps. When computing horizontal ring crossings, vertical level-offs, and measurement joins, Spark's query planner repeatedly evaluated the entire Silver transformation chain from scratch over **86.6 million events** and attempted heavy aggregations across all **118.8 million measurements**, exhausting the 2 GB driver heap during shuffle spill.

**Engineering Fix (PR #12 & PR #13)**:
1. *Decoupled Materialization*: `arrivals.run` was restructured to read directly from the already-written, on-disk Parquet tables of the Silver layer.
2. *Spatial Pruning & Disk Caching*: Events are filtered early to a 300 NM bounding circle around destination aerodromes and cached on disk.
3. *Direct Measurement Joining*: Eliminated full-table measurement aggregations by performing a targeted join strictly on `Distance flown (NM)`.
4. *Staged Level-Off Writing*: `silver/descent_level_offs` is persisted to disk first, then read back to construct `silver/arrivals`.

Following this architectural refactoring, run `prod-6` and final run `prod-8` completed smoothly without memory pressure, reducing the arrival calculation to **215.7 s**.

---

## 4. Arrival Efficiency Study: Methodology & Results

### 4.1 Indicator Definitions

#### Indicator A — Additional Arrival Transit Time (Horizontal Efficiency)
Inspired by the EUROCONTROL ASMA indicator (which monitors transit within 40 NM), our study utilizes a **100 NM ($185.2\text{ km}$)** radius circle around the destination aerodrome:
- **Ring Entry Time ($t_{\text{entry}}$)**: Determined by identifying the last trajectory segment transitioning from outside to inside the 100 NM boundary and performing linear time and coordinate interpolation.
- **Entry Sector**: Measured as the bearing from the aerodrome to the interpolated entry point, categorized into 8 cardinal octants of 45° (`N, NE, E, SE, S, SW, W, NW`).
- **Touchdown ($t_{\text{touchdown}}$)**: The first `entry-runway` event at the destination with altitude $\le \text{elevation} + 1,500\text{ ft}$.
- **Unimpeded Reference Transit Time ($T_{\text{ref}}$)**: The empirical **20th percentile** of actual transit times ($t_{\text{touchdown}} - t_{\text{entry}}$) computed across homogeneous operational groups: `(airport, runway, aircraft_class, entry_sector)`. A hierarchical fallback cascade ensures robust sample sizes:
  $$\text{Group} \ge 20 \longrightarrow (\text{airport, class, sector}) \ge 20 \longrightarrow (\text{airport, class}) \ge 20 \longrightarrow \text{airport}$$
- **Additional Arrival Time**:
  $$\Delta t_{\text{arr}} = (t_{\text{touchdown}} - t_{\text{entry}}) - T_{\text{ref}} \quad (\text{filtered to plausible window: } 5 \le \text{transit} \le 120\text{ min})$$

#### Indicator B — Vertical Descent Profile (CDO) & Holding Stacks
- **Descent Window**: Trajectory events within **200 NM ($370.4\text{ km}$)** of the destination, spanning from the last event at the highest cruise level down to runway touchdown.
- **Level-Off Segmentation & Fusion**: Adjacent level chunks (`level-start` to `level-end`) separated by $\le 60\text{ s}$ with altitude delta $\le 200\text{ ft}$ are fused into contiguous level segments. Segments $< 30\text{ s}$ or $< 1,000\text{ ft}$ above airport elevation (ground movements) are discarded. Altitude bands: `<FL70`, `FL70-100`, `FL100-200`, and `≥FL200`.
- **Holding Pattern Detection**: A level segment is classified as a holding stack if:
  $$\text{Duration} \ge 4.0\text{ min} \quad \text{AND} \quad \frac{\text{Distance Flown}}{\text{Great-Circle Displacement}} \ge 1.5$$
- **Environmental Impact Hypothesis**: Grounded in the *EUROCONTROL 2018 ECAC-wide CDO study* (46 kg excess fuel per flight for 217 s of level flight below top-of-descent), level flight carries an estimated penalty of:
  $$\text{Fuel Penalty} = 12.7\text{ kg fuel / minute of level flight} \quad \text{and} \quad \text{CO}_2 = 3.16 \times \text{Fuel}$$

### 4.2 Coverage & Observability Constraints
Across all European arrivals in `prod-8` (1,866,643 flights), **74.7 %** exhibit a valid 100 NM ring entry, **45.0 %** have an observable runway-entry event, and **39.4 %** yield a fully measured, plausible Indicator A transit time. Coverage varies dramatically across the focal airports:

| Airport | Total Arrivals | Ring Entry % | Runway Entry % | Measured Indicator A % (Count) |
|---|---|---|---|---|
| **Paris-CDG (`LFPG`)** | 29,504 | 98.1 % | 26.4 % | **26.2 %** (7,725) |
| **Toulouse-Blagnac (`LFBO`)** | 6,265 | 86.3 % | 83.4 % | **77.7 %** (4,866) |
| **Stavanger (`ENZV`)** | 4,264 | 47.7 % | 13.9 % | **1.1 %** (48) |
| **London-Heathrow (`EGLL`)** | 38,004 | 97.1 % | 14.2 % | **13.8 %** (5,263) |
| **Frankfurt (`EDDF`)** | 37,768 | 97.1 % | 82.8 % | **81.8 %** (30,900) |

*Observation*: While Frankfurt and Toulouse achieve high runway detection (> 80 %), Paris-CDG (26.2 %) and London-Heathrow (13.8 %) have low runway-entry sensor reporting in OPDI v0.0.2. At Stavanger, runway events are virtually unrecorded (1.1 % / 48 flights), making Indicator A statistically unmeasurable there. Conversely, **Indicator B (in-flight level-offs and holdings) is universally observed across 100 % of flights**.

### 4.3 Empirical Findings across Focal Airports

All metrics below are flight-weighted across the 58-day period during complete European traffic hours:

| Airport | Mean Additional Time (A) | Level-Off Min / Arrival (B) | Arrivals with Holding % | Fuel Loss Hypothesis |
|---|---|---|---|---|
| **Toulouse (`LFBO`)** | **1.9 min** | **1.6 min** | **0.7 %** | 2.2 tonnes / day |
| **Frankfurt (`EDDF`)** | **2.7 min** | **4.1 min** | **2.9 %** | 34 tonnes / day |
| **Paris-CDG (`LFPG`)** | **3.1 min** | **4.6 min** | **2.9 %** | 30 tonnes / day |
| **London-Heathrow (`EGLL`)** | **4.9 min** (on 14 % measured) | **4.4 min** | **12.7 %** | 37 tonnes / day |
| **Stavanger (`ENZV`)** | *[Not measurable - 48 flights]* | **0.2 min** | **0.2 %** | 0.2 tonnes / day |

#### Detailed Operational Insights:
1. **Vertical Level-Off Distribution by Altitude Band**:
   - At **Heathrow**, level-offs are heavily concentrated in the low terminal airspace: **2.27 min** spent between `FL70-100` (intermediate approach capture).
   - At **Paris-CDG**, level-offs occur both at low altitude (**1.92 min** below `FL70`) and high altitude (**1.06 min** at `FL100-200`, **1.35 min** above `FL200`).
   - At **Frankfurt**, step-downs are split between **1.36 min** below `FL70` and **1.46 min** above `FL200`.
   - At **Toulouse**, level flight is minimal (**0.72 min** below `FL70`, **0.64 min** above `FL200`).
   - At **Stavanger**, flights descend continuously with near-zero level-offs (**0.2 min** total).

2. **Holding Stack Dynamics**:
   - A total of **47,417 holding patterns** were detected across European airspace over the 58 days.
   - **London-Heathrow dominates all European holding**: **32,802 holding minutes** recorded over the period (12.7 % of all arrivals enter a holding stack).
   - In comparison, Frankfurt recorded **6,356 holding minutes**, Paris-CDG **5,812 holding minutes**, Toulouse **415 holding minutes**, and Stavanger **214 holding minutes**.
   - Spatial hotspot mapping (`02_holding_hotspots_egll_lfpg`) shows four distinct holding clusters arranged symmetrically around Heathrow (one in each quadrant, matching the four standard London holding stacks), whereas Paris-CDG holding is concentrated primarily in the eastern arrival sectors.

3. **Airfield Throughput Saturation**:
   - Heathrow displays a pronounced non-linear saturation curve (`04_saturation_curves`):
     - Demand $< 20\text{ arrivals/h} \implies \text{additional time} \approx 0.1\text{--}0.2\text{ min}$
     - Demand $20\text{--}29\text{ arrivals/h} \implies 2.4\text{ min}$
     - Demand $30\text{--}39\text{ arrivals/h} \implies 4.5\text{ min}$
     - Demand $40\text{--}49\text{ arrivals/h} \implies 6.8\text{ min}$
   - In contrast, Frankfurt remains virtually flat at **2.5–2.9 min** across the entire range from 20 to 59 arrivals/h, reflecting high runway system capacity. Paris-CDG fluctuates between **2.4–4.3 min** without a clear demand-driven trend, and Toulouse remains stable at **1.8–2.3 min**.

4. **Temporal Congestion Patterns**:
   - Heathrow experiences severe morning peak delays at **07:00–08:00 UTC** reaching **8–10 min** of additional time.
   - Paris-CDG exhibits a morning arrival peak at **06:00 UTC** (**4.9 min**).

5. **Pan-European Context**:
   - Among European airports with $\ge 10,000$ arrivals and $\ge 50 \%$ coverage (evaluated in run `prod-6`), the highest average additional times were observed at **London-Gatwick (4.3 min)**, **Lisbon (3.9 min)**, and **Milan-Malpensa (3.7 min)**, while the lowest were recorded at **Stockholm (2.0 min)** and **Budapest (1.9 min)**.

---

## 5. Machine Learning & Policy Scenarios

### 5.1 GBTRegressor Explanatory Delay Model
A Spark MLlib `GBTRegressor` (20 iterations, maximum depth 5) was trained to explain the additional arrival time of each measured flight during complete hours.
- **Feature Set**: `dest_icao`, `entry_sector`, `arrival_runway`, `aircraft_class`, `operator` (top 30, remainder categorized as `OTHER`), `arrival_hour`, `day_of_week`, `hourly_demand`, `previous_hour_demand`, and `load_factor` (hourly demand relative to the aerodrome's 90th-percentile demand).
  *(Note: `level_off_count` is intentionally excluded as an explanatory feature because vertical level-offs constitute part of the physical delay mechanism).*
- **Strict Temporal Partitioning**: Flights arriving before **2026-07-14** formed the training set (**534,683 arrivals**); flights from the final 14 days formed the test set (**200,937 arrivals**). Random splitting was strictly avoided to prevent temporal data leakage.
- **Naive Baseline Benchmark**: A historical lookup table computing the mean additional time by `(dest_icao, arrival_hour)` on the training set.

#### Model Performance vs Quality Gate:
$$\begin{aligned}
\text{GBT Model:} \quad & \text{MAE} = 2.74\text{ min}, \quad \text{RMSE} = 4.44\text{ min}, \quad R^2 = 0.135 \\
\text{Naive Baseline:} \quad & \text{MAE} = 2.84\text{ min}, \quad \text{RMSE} = 4.64\text{ min}, \quad R^2 = 0.053
\end{aligned}$$
Because the GBT model achieved a lower MAE than the baseline on the unseen test period, the model passed its quality gate and was persisted to `models/prod-8/arrival_gbt`.

At the individual focal airport level, the global model matches or slightly underperforms the simple historical mean:
- **LFPG**: Model MAE 2.65 min vs Baseline 2.66 min
- **EDDF**: Model MAE 2.53 min vs Baseline 2.56 min
- **LFBO**: Model MAE 1.87 min vs Baseline 1.86 min
- **EGLL**: Model MAE 4.28 min vs Baseline 4.10 min

### 5.2 Feature Importance
The GBT model ranks variable importances (`gold/ml_feature_importance`) as follows:
1. **Destination Aerodrome (`dest_icao`)**: **55.0 %**
2. **Aircraft Class (`aircraft_class`)**: **15.0 %**
3. **Arrival Runway (`arrival_runway`)**: **8.0 %**
4. **Hour of Day (`arrival_hour`)**: **7.0 %**
5. **Airport Load Factor**: **5.0 %**
6. **Entry Sector (`entry_sector`)**: **4.0 %**
7. **Airline Operator (`operator`)**: **3.0 %**
8. **Hourly Demand (`hourly_demand`)**: **1.0 %**
9. **Previous Hour Demand**: **0.4 %**
10. **Day of Week**: **0.1 %**

*Interpretation*: Terminal infrastructure layout and aircraft wake turbulence categories are the primary structural determinants of arrival transit time.

### 5.3 Demand-Smoothing Policy Simulation (`gold/ml_scenarios`)
A counterfactual simulation capped arrival demand at each aerodrome's 90th percentile during the test period, smoothing excess flights into adjacent off-peak hours on the same day:
- **Frankfurt (`EDDF`)**: Cap = 51 arrivals/h (7 flights rescheduled/day) $\implies$ **Predicted savings: 7.0 min/day**.
- **Toulouse (`LFBO`)**: Cap = 8 arrivals/h $\implies$ **Predicted savings: 6.8 min/day**.
- **Paris-CDG (`LFPG`)**: Cap = 35 arrivals/h (19 flights rescheduled/day) $\implies$ **Predicted savings: 1.8 min/day**.
- **London-Heathrow (`EGLL`)**: Cap = 45 arrivals/h $\implies$ **Predicted savings: -0.5 min/day**.
- **Stavanger (`ENZV`)**: No demand exceeding threshold $\implies$ **0 min/day**.

*Critical Analytical Insight*: The global ML model fails to capture the pronounced demand-saturation effect at Heathrow that is clearly visible in the empirical data (Figure `04_saturation_curves`). Because a single pan-European model was trained across 600+ aerodromes, and only 13.8 % of Heathrow arrivals had measured runway times, the model diluted Heathrow's specific queuing dynamics. **The descriptive empirical evidence remains the authoritative source for Heathrow's saturation behavior.**

### 5.4 Unsupervised Airport Typology (`gold/airport_typology`)
A `StandardScaler` and `KMeans` pipeline was executed on **119 representative European aerodromes** (filtering for $\ge 100$ measured arrivals and $\ge 30 \%$ runway coverage). Evaluating $k \in [3, 6]$, the optimal clustering occurred at **$k = 3$** with a silhouette score of **0.55**:
1. **Cluster 1 — "Large airports, fluid arrivals" (23 airports)**: High traffic volume, moderate delay, efficient runway throughput (e.g., Frankfurt `EDDF`, Amsterdam `EHAM`, Madrid `LEMD`, Munich `EDDM`, London-Gatwick `EGKK`).
2. **Cluster 2 — "Medium airports, fluid arrivals" (77 airports)**: Moderate regional traffic, negligible holding, low additional time (e.g., Toulouse `LFBO`).
3. **Cluster 3 — "Small airports, high additional time & frequent holdings" (19 airports)**: Constrained regional aerodromes subject to procedural delays or geographic approach constraints.

*(Note: London-Heathrow and Paris-CDG were excluded from the unsupervised clustering due to the 30 % coverage filter).*

---

## 6. Operational Recommendations

Based strictly on the verified findings of run `prod-8`:

1. **London-Heathrow (`EGLL`)**:
   - Operational bottleneck is strictly driven by **hourly runway saturation**. Holding is widespread (12.7 % of flights, 32,802 minutes).
   - *Recommendation*: Prioritize **extended Arrival Management (E-AMAN)** and upstream cross-border flow management to absorb delay via linear en-route speed reduction rather than low-altitude terminal holding stacks (`FL70-100`). Particular focus should be placed on smoothing the 07:00–08:00 UTC morning arrival wave.

2. **Paris-CDG (`LFPG`) & Frankfurt (`EDDF`)**:
   - Both hubs suffer primarily from **vertical descent step-downs** rather than airborne holding stacks (each recording ~2.9 % holding, but accumulating 4.6 and 4.1 minutes of level flight per arrival).
   - *Recommendation*: Implement **Continuous Descent Operations (CDO)** with Point-Merge arrival sequencing. Under the EUROCONTROL 12.7 kg/min hypothesis, eliminating intermediate level-offs represents potential savings of **~30 tonnes of fuel/day at CDG** and **~34 tonnes of fuel/day at Frankfurt**.

3. **Toulouse-Blagnac (`LFBO`) & Stavanger (`ENZV`)**:
   - Both aerodromes operate in unconstrained airspace regimes (1.6 min and 0.2 min of level flight, holding rate $\le 0.7 \%$).
   - *Recommendation*: Arrival phase efficiency is already near optimal; operational focus should target taxi-out and ground turnaround times rather than airborne sequencing.

---

## 7. Methodological Limitations

In compliance with academic data engineering standards, the following constraints must be noted:
1. **Absence of Airline Schedule Data**: OPDI v0.0.2 contains no published timetables. Delay can only be expressed as additional transit relative to an unconstrained 20th-percentile baseline, not as airline schedule delay.
2. **Omission of Meteorological Factors**: Wind fields, visibility, cloud ceiling, and convective weather are absent from OPDI. A portion of additional transit time inevitably reflects headwind components or procedural weather spacing rather than structural air traffic congestion.
3. **Partial and Uneven Runway Coverage**: While runway coverage exceeds 80 % at Frankfurt and Toulouse, it drops to 14.2 % at Heathrow and 13.9 % at Stavanger. If flights captured by airport sensors differ systematically from uncaptured flights, selection bias may exist.
4. **Empirical Reference Baseline**: A 20th-percentile reference cannot detect structural delays that affect an entire cohort of flights uniformly across a peak hour.
5. **Fuel Burn Approximation**: The 12.7 kg/min coefficient is a fleet-average statistical proxy derived from EUROCONTROL ECAC studies, not a dynamic aircraft performance model (such as BADA).
6. **Network Source Outage**: The 14-hour data hole on June 9, 2026, required automated exclusion to prevent distortion of daily figures.

---

## 8. AI-Use Disclosure

In accordance with course evaluation policies for DAT535:
- **Thomas** utilized **Claude (Anthropic)** as an interactive pair-programming assistant throughout the project lifecycle: refactoring exploratory notebooks into the modular `opdi_pipeline` package, designing GitHub Actions CI/CD workflows, engineering incremental Bronze state management, drafting Spark SQL models, creating synthetic pipeline tests, diagnosing and resolving the `prod-5` JVM heap overflow, and structuring the benchmark framework. All code was independently reviewed, executed in CI, and validated on the VM.
- **Mathis** utilized **Google Antigravity (Gemini)** for developing initial prototypes of the arrival indicator rules, designing GBT feature assemblies, implementing notebook visualization routines, and compiling technical drafts.
- **Key Methodological Lesson**: An early AI-generated draft of analytical conclusions hallucinated holding stack identifiers and unverified delay numbers. Consequently, the team established a strict verification protocol: **all quantitative figures in this report are programmatically audited and verified against the production run artifacts (`prod-8`)**.

---

## 9. References & Project Artifacts

- **EUROCONTROL Performance Review Unit (PRU)**: *Methodology for Additional ASMA Time Calculation*, EUROCONTROL Standard Inputs for Cost-Benefit Analyses.
- **EUROCONTROL Continuous Descent Operations (CDO) Study (2018)**: *ECAC-Wide CDO Task Force Report*, 2017 Traffic Benchmark (12.7 kg/min level flight hypothesis).
- **ICAO Environmental Report**: Standard aviation kerosene emission factor ($3.16\text{ kg CO}_2\text{ / kg fuel}$).
- **Repository Artifacts**:
  - Codebase: `mathis6938/group08` (Branch `main`, commit `0e47027`).
  - Production Artifact: `opdi-prod-run-6` / `prod-8` (logs, layer reports, and published Gold tables).
  - Executed Showcase Notebook: [`projet_opdi/notebooks/05_arrival_efficiency.ipynb`](file:///home/mathis/Documents/cours/dataengineering/group08/projet_opdi/notebooks/05_arrival_efficiency.ipynb).
  - Generated High-Resolution Figures:
    - `01_europe_additional_time_and_typology.png`
    - `02_holding_hotspots_egll_lfpg.png`
    - `03_hour_day_heatmaps.png`
    - `04_saturation_curves.png`
    - `05_real_descent_profiles.png`
    - `06_focus_airports.png`
    - `07_model_and_scenario.png`
