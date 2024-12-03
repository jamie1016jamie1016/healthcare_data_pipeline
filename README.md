# **CMS Healthcare Data Pipeline**

This project demonstrates an end-to-end data pipeline using **Apache Airflow** and **Docker**. The pipeline automates the process of downloading, extracting, transforming, loading, and saving healthcare-related data from the public CMS (Centers for Medicare & Medicaid Services) website.

The data is sourced from CMS, a government website providing healthcare-related datasets for public use. These datasets help researchers, data scientists, and healthcare professionals analyze trends, costs, and performance in the healthcare system.

---

## **Project Goals**
1. **Automate Data ETL (Extract, Transform, Load):**
   - Extract data from a remote HTTP endpoint.
   - Transform it into a structured format with user-defined delimiters.
   - Save the transformed data into `.csv` format.
   - Load the data into a PostgreSQL database for querying and analysis.

2. **Output:**
   - A clean, transformed `.csv` file saved to the local directory.
   - Data ingested into a PostgreSQL database for further analysis.

---

## **Pipeline Workflow**
### **Data Source**
The pipeline pulls data from the CMS website ([CMS Outpatient Data](https://data.cms.gov)), which includes **publicly available, anonymized healthcare information**.

### **Steps in the ETL Pipeline**
1. **Extract**:
   - Download the data (in `.zip` format) using the `HttpOperator` in Airflow.
2. **Transform**:
   - Extract `.csv` files from the ZIP archive.
   - Apply transformations such as converting delimiters, cleaning data, and ensuring data quality.
3. **Load**:
   - Save the transformed `.csv` files locally.
   - Insert the data into a PostgreSQL database for querying.

### **Automation**
The pipeline is fully automated via Airflow tasks defined in the DAG (`outpatient_dag.py`), orchestrated with Docker Compose to manage services.

---

## **Repository Structure**
```
cms_health_data_pipeline/
├── dags/
│   ├── outpatient_dag.py       # Airflow DAG definition for the ETL pipeline
├── logs/                       # Directory for Airflow logs
├── scripts/
│   ├── csv_to_db_stage.py      # Script for loading CSV data into PostgreSQL
├── docker-compose.yml          # Docker Compose file to deploy Airflow and Postgres
├── README.md                   # Project documentation
```

---

## **Technologies Used**
- **Docker**: Containerized services for running Airflow and PostgreSQL.
- **Apache Airflow**: Workflow automation and task orchestration.
- **Python**: Data manipulation using libraries like `pandas`.
- **PostgreSQL**: Database for storing Airflow metadata and ingested data.
- **CMS Dataset**: Public dataset hosted by [CMS](https://data.cms.gov).

---

## **Pipeline Enhancements**
1. **Database Integration**:
   - A Python script (`csv_to_db_stage.py`) loads the processed `.csv` data into a PostgreSQL database.
   - Automates data ingestion, allowing users to query the dataset directly.

2. **Dynamic Connection Setup**:
   - The DAG includes functionality to programmatically create an HTTP connection in Airflow, eliminating the need for manual configuration.

3. **Error Handling**:
   - Added logging and retry mechanisms to ensure pipeline reliability.

4. **Modular Design**:
   - The pipeline is modular, enabling easy adaptation to new datasets or transformation requirements.

---

## **Setup Instructions**

### **1. Clone the Repository**
```bash
git clone https://github.com/your-username/cms_health_data_pipeline.git
cd cms_health_data_pipeline
```

### **2. Configure Docker**
Ensure that Docker is installed on your machine. Verify the installation by running:
```bash
docker --version
```

### **3. Start Docker Services**
Ensure to change my path to your path in docker-compose.yml file 
Run the following command to start the containers:
```bash
docker-compose up
```

This will:
- Spin up **PostgreSQL** as the Airflow metadata database.
- Initialize **Apache Airflow** with a webserver and scheduler.
- Mount your `dags`, `logs`, and `scripts` folders for development and monitoring.

> The Airflow webserver will be accessible at **http://localhost:8080**.  
> Login credentials: `admin/admin`.
> Trigger the DAG (`outpatient_dag.py`) manually as this process only need to run once.

---

## **DAG Workflow**
The DAG (`outpatient_dag.py`) defines the ETL pipeline with the following tasks:

1. **Download the ZIP File**:
   - Uses `HttpOperator` to download a `.zip` file from the CMS website.
   - The file is base64 encoded and stored in XCom for subsequent tasks.

2. **Process and Save CSV**:
   - Extracts the CSV file from the ZIP.
   - Applies transformations (e.g., converting the delimiter to `|`).
   - Saves the processed file as `processed_filename.csv` in the `dags` directory.

3. **Insert Data into Database**:
   - Runs a Python script (`csv_to_db_stage.py`) to load the processed CSV data into a PostgreSQL table.

---

## **Output**
### **1. Transformed CSV Files**
- Saved in the `dags/` directory for easy access.

### **2. PostgreSQL Database**
- Data is loaded into the `staging_table` for querying.

---

## **Benefits of This Pipeline**
1. **Reusability**:
   - Easily adapt to other datasets with minimal changes.

2. **Scalability**:
   - Orchestrate complex workflows with Airflow.

3. **Automation**:
   - Eliminates manual steps, reducing human error.

4. **Data Integration**:
   - Processes data and loads it into a database, ready for advanced analytics.

---

## **References**
- CMS Outpatient Data: [https://data.cms.gov](https://data.cms.gov)
- Airflow Documentation: [https://airflow.apache.org](https://airflow.apache.org)
- Docker Documentation: [https://docs.docker.com](https://docs.docker.com)

---

This pipeline provides a scalable, reusable, and automated solution for ETL workflows using healthcare datasets, demonstrating the power of Apache Airflow and Docker in modern data engineering.
