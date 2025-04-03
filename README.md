# E-Commerce ETL Pipeline with Apache Airflow

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline for an e-commerce dataset using Apache Airflow. The pipeline extracts data, processes it, and stores the results in a PostgreSQL database. The processed data is then used for business KPI analysis and visualization.

## Features
- **Data Extraction:** Loads e-commerce data from source files.
- **Data Transformation:** Cleans and processes the data using Python scripts.
- **Data Loading:** Stores the transformed data in a PostgreSQL database.
- **Workflow Orchestration:** Manages ETL tasks using Apache Airflow.
- **KPI Analysis:** Generates insights like average order value and return rates.
- **Dockerized Environment:** Runs in a fully containerized setup.

---
## Project Structure
```
ECommerceETLPipeline/
│-- airflow_config/
│-- airflow_dags/
│   │-- etl_pipeline_dag.py
│   │-- kpi_analysis_dag.py
│-- airflow_logs/
│-- data/
│   │-- avg_order_value.png
│   │-- order_price_boxplot.png
│   │-- order_price_distribution.png
│-- .gitignore
│-- docker-compose.yml
│-- Dockerfile
│-- README.md
│-- testMySQL/
```

## Setup Instructions

### Prerequisites
- Docker & Docker Compose
- Python (for local development)
- PostgreSQL (if running outside of Docker)

### 1. Clone the Repository
```sh
git clone <repository-url>
cd ECommerceETLPipeline
```

### 2. Initialize Apache Airflow
```sh
docker-compose up -d
```

### 3. Reset Admin Password in Airflow
```sh
docker exec -it <container_id> airflow users reset-password -u admin -p password
```

### 4. Initialize the Database
```sh
docker exec -it <container_id> airflow db init
```

### 5. Run Airflow Worker in Interactive Mode
```sh
docker exec -it <container_id> bash
```

### 6. Install Dependencies
```sh
docker exec -it <container_id> pip install kagglehub
```

### 7. Access PostgreSQL Database
```sh
docker exec -it postgres_db psql -U airflow -d ecommerce_db
```
- Check tables:
```sh
\dt
```
- Reset PostgreSQL user password:
```sql
ALTER USER airflow WITH PASSWORD 'airflow';
```
- Restart PostgreSQL container:
```sh
docker restart postgres_db
```

### 8. Copy Airflow DAGs from Container
```sh
docker cp <container_id>:/opt/airflow/airflow_dags ./airflow_dags_output
```

### 9. Virtual Environment (For Local Setup)
```sh
python -m venv venv
venv\Scripts\Activate
```

### 10. Fetch Airflow Docker YAML (Optional)
```sh
wget "https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml" -OutFile "docker-compose.yaml"
```

---
## Debugging Commands

### Check Running Containers
```sh
docker ps
```

### Restart a Container
```sh
docker restart <container_id>
```

### View Airflow Logs
```sh
docker logs <container_id>
```

### Enter PostgreSQL Shell
```sh
docker exec -it postgres_db psql -U airflow -d ecommerce_db
```

### Adjust WSL Configuration (For Windows Users)
1. Navigate to `C:\Users\YourUsername\`
2. Create a file named `.wslconfig`
3. Add the following configuration:
```ini
[wsl2]
memory=6GB  # Set at least 4GB for Airflow
processors=4  # Set number of CPU cores
swap=4GB  # Set swap memory (optional)
```
4. Restart WSL:
```sh
wsl --shutdown
wsl --status
```

---
## Reference Links
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [ELT Video Guide](https://www.youtube.com/watch?v=OW5OgsLpDCQ)
- [Airflow Basics](https://www.youtube.com/watch?v=5peQThvQmQk)
- [Install Apache Airflow using Docker](https://www.youtube.com/watch?v=Fl64Y0p7rls)

---
## Contributors
- Achsah

---
## License
This project is licensed under the MIT License.

---

🎯 **Ready to get started? Run `docker-compose up -d` and explore the DAGs in Airflow UI!** 🚀

