# Worklog

This file documents the daily progress and steps taken to complete the Real-time Attribution Dashboard assignment.

---

### Day 0: Setup & Planning
- Read the assignment document carefully.
- Noted deliverables (architecture, dbt models, streaming demo, dashboard, docs).
- Installed Python and dbt locally.
- Installed Google Cloud SDK and authenticated to GCP.
- Created new GitHub repo `attribution_project`.

---

### Day 1: Environment & dbt Models
- Initialized dbt project (`dbt init attribution_project`).
- Created `profiles.yml` with BigQuery connection.
- Added **staging model** (`stg_events.sql`) for GA4 dataset.
- Defined schema tests (`not_null`, `unique`) in `schema.yml`.
- Ran `dbt run` to build first models.
- Committed and pushed changes to GitHub.

---

### Day 2: Intermediate & Mart Models
- Created **intermediate model** (`int_user_journey.sql`) to order user events.
- Built **mart models** (`mart_first_click.sql`, `mart_last_click.sql`) for attribution logic.
- Added schema.yml tests for marts.
- Verified lineage using `dbt docs generate`.
- Updated GitHub with commits.

---

### Day 3: Streaming Demo & Architecture
- Wrote Python script to generate synthetic events (`generate_events_csv.py`).
- Added streaming script (`stream_events.py`) to batch insert into BigQuery `streaming_events` table.
- Due to BigQuery Sandbox limitation, simulated near-real-time using small CSV batches instead of true streaming inserts.
- Created and added **architecture diagram** (`architecture.png`) to repo.
- Committed and pushed updates.

---

### Day 4: Dashboard & Documentation
- Connected BigQuery marts + streaming table to Looker Studio.
- Built dashboard with:
  - First vs Last totals
  - 14-day time series trend
  - Channel breakdown
  - Live streaming events panel
- Wrote `README.md` including setup instructions, runbook, and limitations.
- Finalized GitHub repo structure.

---

### Next Steps (Final Deliverables)
- Record 5–8 min demo video showing:
  - dbt run + test
  - Streaming script execution
  - BigQuery tables update
  - Dashboard refresh
- Submit repo link, dashboard link, and video to CustomerLabs team.
