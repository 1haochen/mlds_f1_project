# mlds_f1_project

Here is a **clean, simple, beginner-friendly README.md** for your entire Docker + Airflow + F1 ETL pipeline.
It tells the user:

* What this project does
* How to install
* How to run
* How to update
* How the DAG works
* What each script does

It’s short, clear, and usable by anyone.

---

# ✅ **README.md (Simple & Clean)**

You can copy-paste this into your repo.

---

# **F1 ETL Pipeline — Airflow + Docker**

This project builds a **fully automated ETL pipeline** for Formula 1 race data using:

* Docker
* Apache Airflow
* SQLite
* OpenF1 API
* Python ETL scripts
* Tyre-change feature engineering
* (Optional) Streamlit dashboard

The Airflow DAG automatically decides whether to run a **full load** or an **incremental update**, computes tyre strategies, and runs your processing script.

---

## 🚀 **1. Project Overview**

This pipeline automatically:

### **ETL**

* Loads all F1 data (meetings, sessions, laps, stints, pit stops, weather, etc.)
* Maps team and driver identity history
* Builds a clean relational database (`f1_data.db`)

### **Feature Engineering**

* Computes tyre strategy events
* Calculates tyre change laps
* Produces driver-level tyre sequences and performance effects

### **Orchestration**

An Airflow DAG decides:

* **If no database exists → full initial ETL**
* **If the database already exists → incremental update**

After ETL, Airflow:

1. Computes tyre change features
2. Runs `app.py` (you can plug analytics or dashboard prep here)

---

## 📦 **2. What’s Inside**

```
├── dags/
│   └── f1_pipeline_dag.py          # Main Airflow pipeline
├── scripts/
│   ├── load_f1_functional.py       # Full initial ETL
│   ├── update_f1_data.py           # Incremental ETL
│   ├── create_tyre_changes.py      # Feature engineering
│   └── app.py                      # Final processing
├── data/
│   └── f1_data.db                  # Created after ETL
├── docker-compose.yaml
├── Dockerfile
└── README.md
```

---

## 🐳 **3. How to Run Everything**

### **Step 1 — Install Docker Desktop**

Mac / Windows / Linux
[https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)

---

### **Step 2 — Build and Start Airflow**

In the project directory:

```bash
docker-compose down --volumes --remove-orphans
docker-compose up -d --build
```

This will:

* Build your custom Airflow image
* Install Python dependencies
* Mount your scripts/data/dags
* Start Airflow webserver + scheduler

---

### **Step 3 — Open Airflow UI**

Visit in browser:

```
http://localhost:8080
```

Default login:

* **user:** airflow
* **password:** airflow

---

## ▶ **4. Running the Pipeline**

In Airflow UI:

1. Find DAG **f1_etl_pipeline**
2. Toggle it **ON**
3. Click **Trigger DAG**

Airflow will automatically choose:

### **First run → full ETL**

Creates `data/f1_data.db`.

### **Later runs → incremental update**

Uses your `update_f1_data.py`.

---

## 🔀 **5. How the DAG Works**

```
choose_etl_mode (BranchPythonOperator)
      |
      ├── run_initial_etl   (if DB missing)
      └── run_update_etl    (if DB exists)
                |
                ▼
    compute_tyre_changes
                |
                ▼
            run_app
```

Branch re-joining works using:

```
TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
```

---

## ⚙️ **6. Common Commands**

### View running Docker containers

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

### Enter Airflow container

```bash
docker exec -it airflow bash
```

---

## 📁 **7. Where Data Lives**

Your SQLite database is stored on the host:

```
./data/f1_data.db
```

It is mounted inside the container at:

```
/opt/airflow/data/f1_data.db
```

So even if you stop/rebuild the container, **your data stays safe**.

---

## 📊 **8. Adding a Streamlit Dashboard (Optional)**

If your `app.py` launches Streamlit:

```
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
```

Expose the port in `docker-compose.yaml`:

```yaml
ports:
  - "8501:8501"
```

Then open:

```
http://localhost:8501
```

---

## 🎉 **9. You’re Done!**

You now have:

* A reproducible Docker environment
* A fully automated Airflow ETL pipeline
* A tyre strategy feature engineering system
* A persistent SQLite database
* A clean DAG structure

If you want, I can also generate:

* A pretty architecture diagram
* A CLI tool for running ETL manually
* Unit tests for ETL functions
* A Streamlit dashboard UI

Just tell me!
