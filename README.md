
# README

## **CMS Healthcare Data Pipeline**

This project demonstrates an end-to-end data pipeline using **Apache Airflow** and **Docker**. The pipeline automates the process of downloading, extracting, transforming, and saving healthcare-related data from the public CMS (Centers for Medicare & Medicaid Services) website.

The data is sourced from CMS, a government website providing healthcare-related datasets for public use. These datasets help researchers, data scientists, and healthcare professionals analyze trends, costs, and performance in the healthcare system.

---

## **Project Goals**
1. **Automate Data ETL (Extract, Transform, Load):**
   - Extract data from a remote HTTP endpoint.
   - Transform it into a structured format with user-defined delimiters.
   - Save the transformed data into `.csv` format.

2. **Output:**
   - A clean, transformed `.csv` file saved to the local directory for further analysis.

---

## **Pipeline Workflow**
1. **Data Source**:
   - The pipeline pulls data from the CMS website ([CMS Outpatient Data](https://data.cms.gov)).
   - The data includes **publicly available, anonymized healthcare information**.

2. **Steps in the ETL Pipeline**:
   - **Extract**: Download the data (in `.zip` format) using the `HttpOperator` in Airflow.
   - **Transform**: Extract `.csv` files from the ZIP archive, reformat them using a custom delimiter (`|`), and save the processed file.
   - **Load**: The processed `.csv` files are ready for database ingestion or local analysis.

3. **Automation**:
   - The pipeline is fully automated via Airflow tasks defined in `outpatient_dag.py`.
   - Airflow orchestrates the workflow using Docker Compose to manage the services.

---

## **Repository Structure**
```
cms_health_data_pipeline/
├── dags/
│   └── outpatient_dag.py       # Airflow DAG definition for the data pipeline
├── logs/                       # Directory for Airflow logs
├── docker-compose.yml          # Docker Compose file to deploy Airflow and Postgres
├── README.md                   # Project documentation
```

---

## **Technologies Used**
- **Docker**: Containerized services for running Airflow and PostgreSQL.
- **Apache Airflow**: Used for workflow automation and task orchestration.
- **Python**: Data manipulation using libraries like `pandas`.
- **PostgreSQL**: Airflow metadata database.
- **CMS Dataset**: Public dataset hosted by [CMS](https://data.cms.gov).

---

## **Setup Instructions**

### **1. Clone the Repository**
```bash
git clone https://github.com/your-username/cms_health_data_pipeline.git
cd cms_health_data_pipeline
```

### **2. Configure Docker**
Ensure that Docker is installed on your machine. To verify, run:
```bash
docker --version
```

### **3. Start Docker Services**
Run the following command to start the containers:
```bash
docker-compose up
```

This will:
- Spin up **PostgreSQL** as the Airflow metadata database.
- Initialize **Apache Airflow** with a webserver and scheduler.
- Mount your `dags` and `logs` folders for development and monitoring.

> The Airflow webserver will be accessible at **http://localhost:8080**.  
> Login credentials: `admin/admin`.

---

### **4. DAG Overview**
The DAG, `outpatient_dag.py`, defines the ETL pipeline:
- **Task 1: Download the ZIP File**
   - Uses `HttpOperator` to download a `.zip` file from the CMS website.
   - The file is base64 encoded and stored in XCom for the next task.

- **Task 2: Process and Save CSV**
   - Extracts the CSV file from the ZIP.
   - Applies transformations (e.g., converting the delimiter to `|`).
   - Saves the processed file as `processed_filename.csv` in the `dags` directory.

---

### **5. Programmatically Create Airflow Connection**
The DAG includes a function to create an HTTP connection dynamically, eliminating the need to manually configure it in the Airflow UI.

---

### **6. Output**
Once the pipeline completes successfully, you will find the processed CSV files in the `dags` directory.

---

## **Benefits of This Pipeline**
- **Reusability**: Easily adapt to other datasets with minimal changes.
- **Scalability**: Orchestrate complex workflows with Airflow.
- **Automation**: Eliminates manual downloading and processing.

---

## **Key Points**
- The CMS dataset is **publicly available** and adheres to privacy regulations.
- This pipeline is a template for working with similar real-world ETL problems.
- The output can be loaded into a data warehouse for further exploration.

---

## **References**
- CMS Outpatient Data: [https://data.cms.gov](https://data.cms.gov)
- Airflow Documentation: [https://airflow.apache.org](https://airflow.apache.org)
- Docker Documentation: [https://docs.docker.com](https://docs.docker.com)
