
# Database Operations Modules

This repository provides two Python modules for performing robust, logged, and flexible database operations on **SQLite** and **PostgreSQL** databases. These classes are designed for streamlined data engineering workflows, especially where Pandas integration is essential.

---

## 📁 Modules Overview

### 1. `sqlite_operations.py`

Provides the `SQLiteDB` class for interacting with SQLite databases.

#### ✅ Key Features

* Connect to a SQLite database file.
* Read table records into a Pandas DataFrame.
* Insert single or batch records from DataFrames.
* Join module metadata to enrich serial number data.
* Retrieve the latest measurement date.
* Centralized logging of errors and events.

#### 📌 Example

```python
from sqlite_operations import SQLiteDB

db = SQLiteDB("/path/to/database.db")
df = db.read_records("module-metadata")
db.create_sqlite_record("module-metadata", ["column1", "column2"], ["value1", "value2"])
```

---

### 2. `postgres_operations.py`

Provides the `PostgresDB` class for interacting with PostgreSQL databases.

#### ✅ Key Features

* Connect to PostgreSQL with credentials.
* Query tables into Pandas DataFrames.
* Insert single records using parameterized SQL queries.
* Execute arbitrary SQL commands.
* Built-in error handling and cursor management.

#### 📌 Example

```python
from postgres_operations import PostgresDB

db = PostgresDB(
    host="localhost",
    dbname="your_db",
    user="your_user",
    password="your_password"
)

results = db.read_records_from_postgres("SELECT * FROM module_metadata;")
db.create_postgres_records_from_dataframe("module_metadata", ["module_id", "make"], ["123", "ABC Solar"])
```

---

## 📦 Requirements

* Python 3.7+
* pandas
* sqlite3 (standard library)
* psycopg2

Install required packages:

```bash
pip install pandas psycopg2
```

---

## 🗂️ Logging

The SQLite class automatically generates a log file (named based on the DB path) that tracks operations and exceptions. PostgreSQL operations print informative errors and could easily be extended with Python’s `logging` module.

---

## 🚀 Future Enhancements

* Automated Raw Data Upload to NSF ACCESS
* RDF Enrichment Via Comments and Metadata Table
* Workflow Integrations and Notebook Support
* Integration with Airflow and other orchestration tools

---

## 🧑‍💻 Author

**Brent Thompson**
University of Central Florida – Data Enabled Photovoltaics Research Group

---

## 📃 License

MIT License – Feel free to use and adapt this code for academic, commercial, or personal use.
