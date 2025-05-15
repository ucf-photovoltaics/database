
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

### 3. `nsf_operations.py` 

Provides the `NSF_DB` class for interacting with interacting with the bucket in NSF ACCESS.

#### ✅ Key Features
* Credential Handling: Loads access credentials from JSON securely
* S3 Client Config: Custom S3-compatible client with private endpoint support
* Upload Support: Uploads raw datafiles using pd.Series.
* Presigned URLS: Generates temporary secure links for private object access (Future Support)
* Download Support: Download required objects from buckets
* Transfer Support: Placeholder for future implementation of secure transfers using globus  ideally between buckets

#### 📌 Example

```python

from nsf_operations import NSF_DB

        """
        Initialize NSF_DB connection using credentials from key_file.
        key_file should be a JSON file with:
            {
                "access_key": "YOUR_ACCESS_KEY",
                "secret_key": "YOUR_SECRET_KEY",
                "endpoint_url": "https://YOUR_OSN_ENDPOINT"
            }
        Currently just based on Amazon S3 services.
        """
nsf_db = NSF_DB(
    key_file: "/path/to/your/key_file.json"
)

# Upload file
df = pd.Series(['file1.txt','file2.txt'])
nsf_db.upload_files(df, bucket_name="bucket_name") # Change to match the bucket name to upload files

# Download Files
file_dict = nsf_db.download_files(bucket_name="bucket_name", file_keys=df) 

# Download all Files
file_dict  = nsf_db.download_files(bucket_name="bucket_name")
```


---

## 📦 Requirements

* Python 3.7+
* pandas
* sqlite3 (standard library)
* psycopg2
* boto3
* botocore

Install required packages:

```bash
pip install pandas psycopg2 boto3 botocore
```

---

## 🗂️ Logging

The SQLite class automatically generates a log file (named based on the DB path) that tracks operations and exceptions. PostgreSQL operations print informative errors and could easily be extended with Python’s `logging` module.

---

## 🚀 Future Enhancements

* Automated Raw Data Upload to NSF ACCESS (Future Considerations: Parallel Processing (Upload/Download), Progress Bar)
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
