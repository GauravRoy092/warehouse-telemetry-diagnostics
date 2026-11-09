# Automated Warehouse Fleet Telemetry & SLA Breach Diagnostics

> An end-to-end analytical engineering project diagnosing the root causes of dispatch SLA failure across 40 automated fulfillment centers operating Autonomous Mobile Robots (AMRs). Built with containerized PostgreSQL 16, custom window function sequence models, and Metabase executive reporting.

## 📌 Executive Summary

Modern warehouse automation platforms rely on fleets of Autonomous Mobile Robots (AMRs) to move inventory pods to pick-and-pack stations under stringent customer Service Level Agreements (SLAs). In this scenario—modeled after fulfillment networks in major Indian logistics corridors like Bhiwandi, Gurugram, and Bengaluru—order dispatch delays were generating **₹2.12 Crore** in monthly contractual breach penalties.

Ground operations teams initially attributed delays to generic hardware fatigue. This analytics project investigates the actual multi-source failure chains across **500,000+ IoT telemetry events** and **150,000 fulfillment orders**.

### Key Empirical Findings

1. **Spatial Penalty Concentration:** Just **4 out of 40 sites (10% of nodes)** generated **₹2.01 Cr (\~94%)** of total penalty exposure. Across all 4 bottleneck facilities, failures isolated strictly to **ZONE_C**, where legacy Wi-Fi hardware caused signal drops below $-76\text{ dBm}$, forcing AMRs into persistent `NAV_STACK_TIMEOUT` halts.

2. **Predictive Failure Signature:** Using SQL window functions (`LAG`), we identified that two consecutive `BARCODE_SCAN_RETRY` events within a short rolling window serve as a deterministic pre-failure indicator for a catastrophic `MOTOR_CONTROLLER_FAULT`, providing a **16.9-minute lead time** for proactive rerouting.

3. **Firmware Rollout Regression:** Fleet-wide deployment of **Firmware v3.9** doubled total downtime hours from **2,893 hrs to 5,575 hrs**, driven by a runaway thermal throttling loop that spiked operating temperatures to $78.5^\circ\text{C}$ and generated **76,509 thermal warnings** (compared to 0 in v3.8).

## 📊 Executive Dashboard Preview

The analytical findings are synthesized into an operational monitoring cockpit built in Metabase:

```
+-------------------------------------------------------------------------------------------------------------+
|                                  Executive Fleet Telemetry & SLA Operations                                 |
+-----------------------------------+-------------------------------------+-----------------------------------+
|             16.9 Mins             |         4 / 40 Sites (68% Cost)     |             ₹2.12 Cr              |
|   KPI: Early Warning Lead Time    |     KPI: Critical Bottleneck Sites  |    KPI: Total SLA Penalty Cost    |
+-----------------------------------+-------------------------------------+-----------------------------------+
|                                                                                                             |
|  [CARD 1] SLA Penalty Exposure by Site & Zone (Stacked Bar)                                                 |
|  * Highlights extreme financial leakage in ZONE_C across SITE_04, SITE_11, SITE_19, and SITE_27             |
|                                                                                                             |
+-------------------------------------------------------+-----------------------------------------------------+
|  [CARD 2] Predictive Pre-Failure Sequences (Row Chart)|  [CARD 3] Firmware Stability Audit (Data Table)     |
|  * BARCODE_SCAN_RETRY -> BARCODE_SCAN_RETRY -> FAULT  |  * v3.8 vs v3.9 Battery, Temperature & Throttle Warn|
+-------------------------------------------------------+-----------------------------------------------------+
```

*(Place your exported dashboard capture at `assets/dashboard_executive_view.png`)*

## 🏗️ Architecture & Pipeline Flow

The project eliminates Kaggle-style sanitized CSV workflows in favor of a containerized operational telemetry pipeline:

```
┌─────────────────────────────────┐
│     Synthetic Engine (Python)   │
│  - 40 Nodes (Geographic Mix)    │
│  - 500k Telemetry IoT Records   │
│  - Injected Realistic Anomalies │
└────────────────┬────────────────┘
                 │ High-throughput stream / CSV
                 ▼
┌─────────────────────────────────┐
│     PostgreSQL 16 (Docker)      │
│  - Normalized Star/Snowflake    │
│  - Composite Indexes (B-Tree)   │
│  - Analytical CTEs & Windows    │
└────────────────┬────────────────┘
                 │ Native connection (Port 5432)
                 ▼
┌─────────────────────────────────┐
│       Metabase BI (Docker)      │
│  - Real-time SQL Visualizer     │
│  - Executive KPI Cards          │
│  - Spatial & Firmware Audits    │
└─────────────────────────────────┘
```

## 🗄️ Relational Data Model (DDL)

The schema is built for high write throughput and analytical scans.

```sql
-- Database creation
CREATE DATABASE warehouse_telemetry;
\c warehouse_telemetry;

-- 1. Site Master Table
CREATE TABLE site_metadata (
    site_id VARCHAR(10) PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    region VARCHAR(20) NOT NULL,
    total_bots_deployed INT NOT NULL,
    network_vendor VARCHAR(30) NOT NULL,
    go_live_date DATE NOT NULL
);

-- 2. High-Frequency IoT Robot Telemetry Stream
CREATE TABLE robot_telemetry_logs (
    log_id BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    site_id VARCHAR(10) NOT NULL REFERENCES site_metadata(site_id),
    bot_id VARCHAR(20) NOT NULL,
    zone_id VARCHAR(10) NOT NULL,
    firmware_version VARCHAR(10) NOT NULL,
    battery_pct NUMERIC(5,2),
    wifi_rssi_dbm INT,
    cpu_temp_c NUMERIC(5,2),
    error_code VARCHAR(40) NOT NULL,
    recovery_time_sec INT DEFAULT 0
);

-- 3. Fulfillment Order Execution & Penalties
CREATE TABLE fulfillment_orders (
    order_id VARCHAR(25) PRIMARY KEY,
    site_id VARCHAR(10) NOT NULL REFERENCES site_metadata(site_id),
    assigned_bot_id VARCHAR(20) NOT NULL,
    zone_id VARCHAR(10) NOT NULL,
    order_timestamp TIMESTAMP NOT NULL,
    promised_sla_mins INT NOT NULL,
    actual_completion_mins NUMERIC(6,2) NOT NULL,
    sla_breached BOOLEAN NOT NULL,
    penalty_cost_inr NUMERIC(10,2) DEFAULT 0.00
);

-- Targeted Composite Indexes for Analytical Queries
CREATE INDEX idx_telemetry_site_zone ON robot_telemetry_logs(site_id, zone_id);
CREATE INDEX idx_telemetry_bot_time ON robot_telemetry_logs(bot_id, event_timestamp);
CREATE INDEX idx_telemetry_error ON robot_telemetry_logs(error_code);
CREATE INDEX idx_orders_site_zone ON fulfillment_orders(site_id, zone_id);
CREATE INDEX idx_orders_sla ON fulfillment_orders(sla_breached);
```

## 🔬 In-Depth Diagnostic SQL Queries

### Analysis 1: Cross-Domain Correlation (Wi-Fi Degradation vs. SLA Loss)

Demonstrates conditional aggregation (`FILTER`), multi-table relational joins, and sub-query CTEs to isolate financial leakage to physical warehouse zones.

```sql
WITH zone_telemetry AS (
    SELECT 
        site_id,
        zone_id,
        ROUND(AVG(wifi_rssi_dbm)::NUMERIC, 1) AS avg_wifi_dbm,
        COUNT(*) FILTER (WHERE error_code = 'NAV_STACK_TIMEOUT') AS nav_timeout_count,
        ROUND((SUM(recovery_time_sec) / 3600.0)::NUMERIC, 1) AS total_downtime_hours
    FROM robot_telemetry_logs
    GROUP BY site_id, zone_id
),
zone_sla_impact AS (
    SELECT 
        site_id,
        zone_id,
        COUNT(order_id) AS total_orders,
        COUNT(order_id) FILTER (WHERE sla_breached = TRUE) AS breached_orders,
        SUM(penalty_cost_inr) AS penalty_inr
    FROM fulfillment_orders
    GROUP BY site_id, zone_id
)
SELECT 
    s.site_id,
    s.city,
    s.network_vendor,
    -- Pivoting zone penalties for clean metric breakdowns
    COALESCE(SUM(o.penalty_inr) FILTER (WHERE o.zone_id = 'ZONE_A'), 0) AS penalty_zone_a,
    COALESCE(SUM(o.penalty_inr) FILTER (WHERE o.zone_id = 'ZONE_B'), 0) AS penalty_zone_b,
    COALESCE(SUM(o.penalty_inr) FILTER (WHERE o.zone_id = 'ZONE_C'), 0) AS penalty_zone_c,
    COALESCE(SUM(o.penalty_inr) FILTER (WHERE o.zone_id = 'ZONE_D'), 0) AS penalty_zone_d,
    SUM(o.penalty_inr) AS total_penalty_inr
FROM site_metadata s
LEFT JOIN zone_sla_impact o ON s.site_id = o.site_id
LEFT JOIN zone_telemetry t ON s.site_id = t.site_id AND o.zone_id = t.zone_id
GROUP BY s.site_id, s.city, s.network_vendor
ORDER BY total_penalty_inr DESC;
```

### Analysis 2: Pre-Failure Event Sequence Detection (`LAG` Window Function)

Reconstructs chronological telemetry timelines per physical bot to identify lead indicators before full motor shutdowns occur.

```sql
WITH error_sequences AS (
    SELECT 
        bot_id,
        site_id,
        event_timestamp AS crash_time,
        error_code AS current_error,
        recovery_time_sec,
        LAG(error_code, 1) OVER (PARTITION BY bot_id ORDER BY event_timestamp) AS prev_error_1,
        LAG(event_timestamp, 1) OVER (PARTITION BY bot_id ORDER BY event_timestamp) AS prev_time_1,
        LAG(error_code, 2) OVER (PARTITION BY bot_id ORDER BY event_timestamp) AS prev_error_2,
        LAG(event_timestamp, 2) OVER (PARTITION BY bot_id ORDER BY event_timestamp) AS prev_time_2
    FROM robot_telemetry_logs
    WHERE error_code <> 'NONE'
)
SELECT 
    prev_error_2 || ' -> ' || prev_error_1 || ' -> ' || current_error AS failure_chain,
    COUNT(*) AS total_occurrences,
    ROUND(AVG(EXTRACT(EPOCH FROM (crash_time - prev_time_2)) / 60.0)::NUMERIC, 1) AS avg_lead_time_mins,
    ROUND((SUM(recovery_time_sec) / 3600.0)::NUMERIC, 1) AS wasted_floor_hours
FROM error_sequences
WHERE current_error = 'MOTOR_CONTROLLER_FAULT'
  AND EXTRACT(EPOCH FROM (crash_time - prev_time_2)) <= 1800
GROUP BY failure_chain
ORDER BY total_occurrences DESC
LIMIT 5;
```

### Analysis 3: Firmware Cohort Reliability & Thermal Audit

Assesses hardware fleet degradation between production versions `v3.8` and `v3.9`.

```sql
SELECT 
    firmware_version,
    ROUND(AVG(battery_pct)::NUMERIC, 1) AS avg_battery_pct,
    ROUND(AVG(cpu_temp_c)::NUMERIC, 1) AS avg_cpu_temp_c,
    COUNT(*) FILTER (WHERE error_code = 'THERMAL_THROTTLE_WARN') AS thermal_warnings,
    ROUND((SUM(recovery_time_sec) / 3600.0)::NUMERIC, 1) AS total_downtime_hours
FROM robot_telemetry_logs
GROUP BY firmware_version;
```

## 📈 Engineering & Business Recommendations

| Finding | Root Cause | Engineering Solution | Business/Financial Impact | 
| ----- | ----- | ----- | ----- | 
| **Zone C Bottleneck** | Legacy AP packet dropping below $-76\text{ dBm}$ | Replace legacy AP hardware in `SITE_04`, `SITE_11`, `SITE_19`, and `SITE_27` with mesh antennas. | Recovers up to **₹1.9 Cr / quarter** in SLA breach fines. | 
| **Motor Stalls** | Optical alignment issues trigger repeated scan failures, heating the drive unit. | Deploy dispatch rule: 2 consecutive `BARCODE_SCAN_RETRY` events immediately trigger autonomous return to charging dock for re-zeroing. | Prevents complete bot stall, saving **\~1,200 floor downtime hours**. | 
| **Firmware Regression** | `v3.9` aggressive path-planning loops overtax robot CPU without thermal pauses. | Immediate rollback to `v3.8` on all affected nodes; patch path-planning scheduler before next release. | Restores fleet battery efficiency by **+13%** and prevents hardware thermal damage. | 

## 🚀 Quickstart: Reproducing Locally

### Prerequisites

* Docker & Docker Compose
* Python 3.10+ (or Google Colab)
* DBeaver or pgAdmin 4

### 1. Launch Services

Spin up both PostgreSQL 16 and Metabase containers:

```bash
# PostgreSQL Container
docker run -d \
  --name pg_warehouse \
  -p 5432:5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=warehouse_telemetry \
  postgres:16

# Metabase Container
docker run -d \
  --name bi_metabase \
  -p 3000:3000 \
  metabase/metabase
```

### 2. Initialize Schema

Connect your SQL client (DBeaver/pgAdmin) to `127.0.0.1:5432` (`app_db` or `warehouse_telemetry`) and execute the DDL definitions from [`sql/schema.sql`](./sql/schema.sql).

### 3. Generate & Ingest Dataset

Run the data generator locally or inside Google Colab:

```bash
pip install pandas numpy sqlalchemy psycopg2-binary
python scripts/generate_telemetry_data.py
```

### 4. Configure Metabase Dashboard

1. Access Metabase at `http://localhost:3000`.
2. Connect to the database using:
   * **Host:** `host.docker.internal` *(Mac/Windows Docker networking)*
   * **Port:** `5432`
   * **Database:** `warehouse_telemetry`
   * **User/Password:** `postgres / postgres`
3. Execute the diagnostic queries in [`sql/diagnostics.sql`](./sql/diagnostics.sql) and assemble your visual cards.