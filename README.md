# **F1 ETL Pipeline — Airflow + Docker**

This project builds a **fully automated ETL pipeline** for Formula 1 race data (from https://openf1.org/) using:

* Docker
* Apache Airflow
* SQLite
* OpenF1 API
* Python ETL modules
* Tyre-strategy feature engineering
* Streamlit dashboard

The Airflow DAG automatically triggers a **full load** on first run or an **incremental update** on subsequent runs, computes tyre-strategy features, and runs your processing/dashboard script.

---

# 🚀 **1. Project Overview**

The system automates:

### **ETL**

* Pulls meetings, sessions, results, laps, pit stops, stints, weather, and driver/team identities
* Normalizes and stores them as a relational SQLite database (`f1_data.db`)

### **Feature Engineering**

* Detects tyre-change events
* Computes stint sequences, tyre histories, and delta performance indicators

### **Orchestration (Airflow)**

* Chooses **full ETL** if no DB exists
* Chooses **incremental update** otherwise
* Runs tyre-change processing
* Runs `app.py` (analytics or Streamlit dashboard bootstrap)

---

# 📦 **2. Repository Structure**

```
├── dags/
│   └── f1_pipeline_dag.py          # Airflow DAG
├── scripts/
│   ├── load_f1_functional.py       # Initial ETL
│   ├── update_f1_data.py           # Incremental ETL
│   ├── create_tyre_changes.py      # Feature engineering
│   └── app.py                      # Final processing / Streamlit
├── data/
│   └── f1_data.db                  # Output DB
├── docker-compose.yaml
├── Dockerfile
└── README.md
```

---

# 🐳 **3. How to Run Everything**

Everything below is the core workflow you follow every time you want to run ETL + feature engineering + dashboard.

---

## **Step 1 — Install Docker Desktop**

[https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)

---

## **Step 2 — Build and Start the Airflow Environment**

In the project root:

```bash
docker-compose down --volumes --remove-orphans
docker-compose build --no-cache
docker-compose up
```

This:

* Builds an Airflow image
* Installs dependencies
* Mounts `dags/`, `scripts/`, `data/`
* Starts the Airflow webserver and scheduler

---

## **Step 3 — Open Airflow UI**

Browser:

```
http://localhost:8080
```

Login:

* **user:** admin
* **password:** admin

---

## **Step 4 — Trigger the ETL Pipeline**

In Airflow UI:

1. Find **f1_etl_pipeline**
2. Toggle it **ON**
3. Click **Trigger DAG**

### What happens:

* **First run** → full ETL creates `f1_data.db`
* **Later runs** → incremental ETL updates only new sessions
* Computes tyre-change features
* Executes your dashboard/script (`app.py`)

---

## **Step 5 — View the Streamlit Dashboard (when applicable)**

When your `run_app` step in the airflow is running, to view streamlit dashboard, you can open:


```
http://0.0.0.0:8501
```

This is where the dashboard or analytics app is displayed.


# 🔀 **4. DAG Flow (How It Works Internally)**

```
choose_etl_mode  (BranchPythonOperator)
        |
        ├── run_initial_etl      # if DB missing
        └── run_update_etl       # if DB exists
                |
                ▼
      compute_tyre_changes
                |
                ▼
             run_app
```

Branching uses:

```
TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
```

This ensures both full load and incremental paths rejoin cleanly.

---

# 📁 **5. Where Data Lives**

The SQLite DB stays on local machine, NOT inside the container:

Host:

```
./data/f1_data.db
```

Inside the container (mounted):

```
/opt/airflow/data/f1_data.db
```

Even when rebuilding Docker containers, the database persists.

---

# ⚙️ **6. Helpful Commands**

### See active containers

```bash
docker ps
```

### Stop everything

```bash
docker-compose down
```

### Rebuild everything

```bash
docker-compose up -d --build
```

### Shell into Airflow container

```bash
docker exec -it airflow bash
```

