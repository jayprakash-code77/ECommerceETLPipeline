from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
import logging
import matplotlib.pyplot as plt
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

# Database connection
DB_URI = "postgresql://airflow:airflow@postgres:5432/ecommerce_db"

# Define output path for visualizations
OUTPUT_FOLDER = "/opt/airflow/data"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def compute_kpis():
    """
    Computes business KPIs from the PostgreSQL database.
    - Calculates Average Order Value (AOV)
    - Stores KPI results in the business_kpis table
    """
    engine = create_engine(DB_URI)
    logging.info("Connected to PostgreSQL for KPI computation.")

    # Fetch order data from the database
    query = "SELECT * FROM order_items"
    df = pd.read_sql(query, engine)

    # Log available columns
    logging.info(f"Available columns in order_items table: {list(df.columns)}")

    if df.empty:
        logging.error("Order Items table is empty. Skipping KPI computation.")
        return

    if 'price' not in df.columns:
        logging.error("'price' column missing in order_items table. Aborting KPI computation.")
        return

    # Convert price column to numeric, handling errors
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Compute Average Order Value (AOV)
    avg_order_value = df.groupby("order_id")["price"].sum().mean()
    logging.info(f"Average Order Value: {avg_order_value}")

    # Store KPI in the database
    kpi_df = pd.DataFrame({'metric': ['AOV'], 'value': [avg_order_value]})
    kpi_df.to_sql("business_kpis", engine, if_exists="replace", index=False)
    logging.info("KPI computation completed and stored in business_kpis table.")

def generate_visuals():
    """
    Generates and saves visualizations for business KPIs:
    - Histogram of order prices
    - Box plot of order prices
    - Bar chart of average order value per order
    """
    engine = create_engine(DB_URI)
    logging.info("Connected to PostgreSQL for visualization.")

    # Fetch order data from the database
    query = "SELECT * FROM order_items"
    df = pd.read_sql(query, engine)

    if df.empty:
        logging.error("Order Items table is empty. Skipping visualization.")
        return

    if 'price' not in df.columns:
        logging.error("'price' column missing in DataFrame. Skipping visualization.")
        return

    # Histogram of Order Prices
    plt.figure(figsize=(8, 5))
    plt.hist(df['price'], bins=20, color='blue', alpha=0.7, edgecolor='black')
    plt.xlabel("Price")
    plt.ylabel("Frequency")
    plt.title("Order Price Distribution")
    plt.grid()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "order_price_distribution.png"))
    plt.close()
    logging.info("Histogram saved: data/order_price_distribution.png")

    # Box Plot of Order Prices
    plt.figure(figsize=(6, 5))
    plt.boxplot(df['price'], vert=False, patch_artist=True, boxprops=dict(facecolor="skyblue"))
    plt.xlabel("Price")
    plt.title("Box Plot of Order Prices")
    plt.grid()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "order_price_boxplot.png"))
    plt.close()
    logging.info("Box plot saved: data/order_price_boxplot.png")

    # Bar Chart of Average Order Value per Order
    avg_order_value_per_order = df.groupby("order_id")["price"].sum()
    plt.figure(figsize=(10, 5))
    avg_order_value_per_order.plot(kind="bar", color='green', alpha=0.6)
    plt.xlabel("Order ID")
    plt.ylabel("Total Order Value")
    plt.title("Average Order Value per Order")
    plt.xticks([], [])  # Remove order_id labels for clarity
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig(os.path.join(OUTPUT_FOLDER, "avg_order_value.png"))
    plt.close()
    logging.info("Bar chart saved: data/avg_order_value.png")

    logging.info("All visualizations generated successfully.")

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define DAG
dag = DAG(
    dag_id='kpi_analysis_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Task to compute business KPIs
compute_kpi_task = PythonOperator(
    task_id="compute_kpis",
    python_callable=compute_kpis,
    dag=dag
)

# Task to generate KPI visualizations
visualization_task = PythonOperator(
    task_id="generate_visuals",
    python_callable=generate_visuals,
    dag=dag
)

# Define task dependencies
compute_kpi_task >> visualization_task
