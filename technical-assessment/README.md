# Analytical Engineer Technical Assessment

This repository contains my submission for the **Analytics Engineer position assessment**.  

The work is organized according to the assignment requirements and divided into three main parts (1–3):  

- **Part 1:**
   - **Task 1 (Notebook)** -> Data quality checks + cleaning based on **5 business rules**.  
   - **Task 2 (Python Script)**- > Expansion of cleaned data to **1000 rows per table** (`players`, `affiliates`, `transactions`).  
- **Part 2:**
   - **dbt Models** -> Transformation layer in dbt with three staging and three marts models answering the business questions.  
- **Part 3:**
   - **Task 1 (Bonus - Airflow)** -> Airflow orchestration of dbt models. Includes a DAG considering the implementation of dbt models in BigQuery.
   - **Task 2 (Bonus - GC Composer)** -> Pipeline implementation in BigQuery + Google Cloud Composer. Includes research notes.

---


## Repository Structure
```
├── airflow/
│   └── dags/
│       └── ancient_gaming_pipeline.py
│
├── data/
│   ├── affiliates_cleaned_from_notebook.csv
│   ├── affiliates_expanded.csv
│   ├── exceptions_from_notebook.csv
│   ├── players_cleaned_from_notebook.csv
│   ├── players_expanded.csv
│   ├── Sample_data_-Technical_Interview-affiliates.csv
│   ├── Sample_data-Technical_Interview-players.csv
│   ├── Sample_data-Technical_Interview-_transactions.csv
│   ├── transactions_cleaned_from_notebook.csv
│   └── transactions_expanded.csv
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── staging_tests.yml
│       │   ├── stg_affiliates.sql
│       │   ├── stg_players.sql
│       │   └── stg_transactions.sql
│       │
│       └── marts/
│           ├── marts_tests.yml
│           ├── country_deposit_analysis.sql
│           ├── player_daily_transactions.sql
│           └── player_top_three_deposits.sql
│
├── notebooks/
│   ├── eda.ipynb
│   └── part_1_task_1__preprocess_data.ipynb
│
├── scripts/
│   └── part_1_task_2__expand_clean_data.py
│
└── README.md
```

---


## Part 1 - Task 1 — Data Cleaning

First, EDA done in **`notebooks/eda.ipynb`**. Then, cleaning implemented in **`notebooks/part_1_task_1__preprocess_data.ipynb`**.  

The raw data was checked against 5 business rules:
1. **KYC rule**: Players cannot transact before KYC approval.  
   - Non-KYC transactions dropped.  
   - Transactions before `created_at` dropped.  
2. **Affiliate code usage**: Multiple players may share codes, but if `redeemed_at` is set → must be 1:1.  
3. **Redeemed affiliate must be linked**: Enforced via cleaning + exceptions.  
4. **Affiliate optionality**: Null `affiliate_id` allowed.  
5. **IDs must be unique**: Deduplication ensured.  

Violations were logged in an **exceptions file** (**`data/exceptions_from_notebook.csv`**) for auditability.  

---


## Part 1 - Task 2 — Data Expansion

Implemented in **`scripts/part_1_task_2__expand_clean_data.py`**.  

Key points:
- Expanded all three tables to **1000 rows**.  
- Business rules respected:
  - Transactions only for KYC-approved players.  
  - Transaction timestamps after `player.created_at`.  
  - ~20% of affiliates remain unredeemed.  
- Faker was used to generate realistic countries, affiliates, timestamps, and transaction amounts.  
- All timestamps normalized to **tz-naive** to avoid inconsistencies.  

---


## Part 2 — dbt Data Models

Implemented in **`dbt/models/staging`** and **`dbt/models/marts`**:

Staging models:
1. **`stg_affiliates.sql`** - Selects and renames columns from the raw `affiliates` table for clarity (e.g., `id` -> `affiliate_id`).
2. **`stg_players.sql`** - Selects and renames columns from the raw `players` table.  
3. **`stg_transactions.sql`** - Selects and renames columns from the raw `transactions` table.  
   - Encloses reserved keywords like `"type"` and `"timestamp"` in double quotes to prevent SQL errors.

Marts models:
1. **`player_daily_transactions.sql`** - Creates one row per player, per day, summarizing their transaction activity.  
   - Calculates total `deposit_amount` and `withdrawal_amount`.  
   - **Applies a key business rule:** Withdrawals are reported as positive values.  
   - Adds a `total_transactions` column to count all daily activities for a player.
2. **`country_deposit_analysis.sql`** - Calculates the `total_deposit_amount` and `number_of_deposits` for each country.  
   - **Applies business rule filters:** Includes only transactions from players who are `is_kyc_approved` and whose affiliate origin is `'Discord'`.  
3. **`player_top_three_deposits.sql`** - Creates one row per player, pivoted to show their three largest deposit amounts in the columns `first_largest_deposit`, `second_largest_deposit`, and `third_largest_deposit`.  
   - Uses the `ROW_NUMBER()` window function to assign a unique rank to each deposit, correctly handling ties to ensure the three columns are always populated.

Configuration & Testing:
1. **`dbt_project.yml`** - The main configuration file for the dbt project.  
   - Defines the project name, model paths, and materialization strategies (e.g., views for staging, tables for marts).
2. **`profiles.yml`** - The connection configuration file, kept separate from the project for security.  
   - Specifies the credentials and settings required for dbt to connect to the BigQuery data warehouse.
3. **`staging_tests.yml`** and **`marts_tests.yml`** (in `staging/` and `marts/`) - The files where data quality tests are defined.  
   - **Enforces data integrity:** Contains tests like `unique`, `not_null`, `accepted_values`, and `relationships` (foreign keys) to ensure the data is reliable and accurate.

---


## Part 3 / Bonus  - Task 1 — Airflow Orchestration

My Notes:
- I have **never used dbt together with Airflow in production**.  
- My usual practice is orchestrating **Python scripts** directly in Airflow.  
- I researched how to integrate dbt and Airflow for this assessment, and found [**Astronomer Cosmos**](https://github.com/astronomer/astronomer-cosmos).  
Cosmos can **dynamically generate Airflow tasks from dbt models**, respecting inter-model dependencies. This seems like a promising approach, but I am **not sure it’s the best or most standard way** to orchestrate dbt with Airflow.

Implemented in the **`airflow/`** directory and configured to run on **Google Cloud Composer**:

Orchestration setup:
1. **`ancient_gaming_pipeline.py`** - The main Airflow DAG that defines and orchestrates the entire dbt pipeline.  
   - **Schedule:** Configured to run daily (`@daily`) to meet the business requirement for a daily refresh.  
   - **Cosmos Integration:** Uses the `DbtTaskGroup` from the `astronomer-cosmos` library to automatically parse the dbt project and create an Airflow task for each model and test, perfectly mirroring the dbt dependency graph.

2. **`profiles.yml`** (in `dbt/`) - The dbt connection profile, specifically configured for the production Airflow environment.  
   - **BigQuery Connection:** Defines how dbt connects to the Google BigQuery data warehouse.  
   - **Authentication:** Uses the `oauth` method to securely authenticate via the Cloud Composer environment's underlying service account, which is a best practice.  
   - **Dynamic Configuration:** Leverages Airflow environment variables (`{{ env_var(...) }}`) to specify the GCP project and dataset, which avoids hardcoding sensitive information and makes the pipeline configurable.

---


## Part 3 / Bonus  - Task 2 — Pipeline Implementation (BigQuery & Google Cloud Composer)
 
Although I have **never used Google Cloud Composer**, and unfortunately, I could not correctly finish this task within the deadline. However, I did research the steps and how I would try to address this requirement if I had more time.  

The instructions for this part were to **implement the pipeline in BigQuery and orchestrate it with Airflow on Google Cloud Composer**.  

Here’s how I would approach it:

### Phase 1: Google Cloud Setup
- Create a new GCP project (e.g. `ancient-gaming-pipeline`).  
- Enable the required APIs: **BigQuery API**, **Cloud Composer API**, **Cloud Storage API**.  
- Create a GCS bucket (e.g. `ancient-gaming-raw-data`) to store the CSVs.  

### Phase 2: Load Raw Data into BigQuery
- Upload the `players.csv`, `affiliates.csv`, and `transactions.csv` to the GCS bucket.  
- Create a BigQuery dataset (`ancient_gaming_raw`).  
- Create tables from the uploaded CSVs, letting BigQuery **auto-detect schemas**.  

### Phase 3: Set Up Cloud Composer
- Create a Composer 2 environment (e.g. `ancient-gaming-orchestrator`).  
- Install the required PyPI dependency: `apache-airflow-providers-astronomer-cosmos`.  
- Configure the Composer service account with roles: **BigQuery User** and **BigQuery Data Editor**.  
- Add an Airflow variable (`DBT_DATASET = dbt_ancient_gaming`) for the target dataset.  

### Phase 4: Deploy and Test Run
- Upload both the `airflow/` folder (with the DAG) and the `dbt/` project folder into Composer’s DAGs bucket.  
- In the Airflow UI, trigger the DAG (`dbt_ancient_gaming_pipeline`).  
- Airflow (via Astronomer Cosmos) would dynamically generate tasks for each dbt model, respecting dependencies.  
- After a successful run, check BigQuery for the `dbt_ancient_gaming` dataset containing the three final dbt models. 

### Additional Considerations

- I thought about error handling and retries in Airflow, including configuring an email account to send alerts for warnings or task failures so the team is immediately notified of issues.
- I considered modularizing dbt models so each step could be independently tested and maintained.
- I would implement logging and monitoring, leveraging Composer’s built-in Airflow UI and BigQuery query history.
- To ensure data quality, I would create Singular data tests using SQL to validate key business rules (e.g., verifying transaction totals).
- While I have not implemented and executed this pipeline yet, I focused on understanding the integration points between GCS, BigQuery, Airflow, and dbt, which would allow me to quickly implement the pipeline in practice.


---


Author: *Augusto Cardoso Agostini*
