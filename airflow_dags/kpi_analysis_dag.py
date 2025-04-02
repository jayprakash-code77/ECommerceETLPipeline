from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import logging
from io import StringIO

# Logging setup
logging.basicConfig(level=logging.INFO)

# Database Connection
DB_URI = "postgresql://airflow:airflow@postgres:5432/ecommerce_db"
KPI_TABLE = "business_kpis"

def compute_kpis():
    """Computes business KPIs and stores them in the database."""
    engine = create_engine(DB_URI)

    logging.info("Loading data from PostgreSQL...")
    df_orders = pd.read_sql("SELECT order_id, customer_id, order_status, order_purchase_timestamp FROM orders", engine)
    df_order_items = pd.read_sql("SELECT order_id, price FROM order_items", engine)
    df_customers = pd.read_sql("SELECT customer_id, customer_state FROM customers", engine)

    df_orders = df_orders.merge(df_customers, on="customer_id", how="left")

    # ✅ Convert order_purchase_timestamp to datetime
    df_orders["order_purchase_timestamp"] = pd.to_datetime(df_orders["order_purchase_timestamp"], errors='coerce')

    # ✅ Extract month for AOV calculation
    df_orders["month"] = df_orders["order_purchase_timestamp"].dt.to_period("M").astype(str)

    logging.info(f"First few rows after adding 'month' column:\n{df_orders[['order_purchase_timestamp', 'month']].head()}")

    # 🔹 Merge order items with orders
    df_revenue = df_orders.merge(df_order_items, on="order_id", how="left")

    if "price" not in df_revenue.columns:
        logging.error("❌ 'price' column missing after merging. Check order_items table.")
        return  

    # ✅ **Average Order Value (AOV) Calculation**
    if "month" in df_orders.columns and "price" in df_revenue.columns:
        avg_order_value = df_revenue.groupby("month")["price"].sum() / df_orders.groupby("month")["order_id"].nunique()
        avg_order_value = avg_order_value.fillna(0).reset_index()
    else:
        logging.error("❌ Missing 'month' column before AOV calculation.")
        return

    logging.info("✅ KPIs computed successfully.")


def generate_visualizations():
    """Generates and saves visualizations for business KPIs."""
    engine = create_engine(DB_URI)
    kpi_data = pd.read_sql(f"SELECT * FROM {KPI_TABLE}", engine)
    
    def safe_read_json(data):
        return pd.read_json(StringIO(data)) if isinstance(data, str) else pd.DataFrame()
    
    # ✅ **Sales by State (Pie Chart)**
    sales_data = safe_read_json(kpi_data[kpi_data["metric"] == "Total Sales by State"]["value"].values[0])
    if not sales_data.empty:
        plt.figure(figsize=(10, 6))
        plt.pie(sales_data["total_orders"], labels=sales_data["state"], autopct="%1.1f%%", startangle=140)
        plt.title("Total Sales by State")
        plt.savefig("/opt/airflow/dags/sales_by_state.png")

    # ✅ **Product Return Rate (Bar Chart)**
    return_rate = float(kpi_data[kpi_data["metric"] == "Product Return Rate"]["value"].values[0])
    plt.figure(figsize=(6, 4))
    plt.bar(["Return Rate"], [return_rate], color="red")
    plt.ylabel("Return Rate (%)")
    plt.title("Product Return Rate")
    plt.savefig("/opt/airflow/dags/return_rate.png")

    # ✅ **Average Order Value Over Time (Line Chart)**
    avg_order_value = safe_read_json(kpi_data[kpi_data["metric"] == "Average Order Value Over Time"]["value"].values[0])
    if not avg_order_value.empty and "price" in avg_order_value.columns:
        plt.figure(figsize=(10, 5))
        plt.plot(avg_order_value["month"], avg_order_value["price"], marker="o", linestyle="-", color="blue")
        plt.xlabel("Month")
        plt.ylabel("AOV ($)")
        plt.xticks(rotation=45)
        plt.title("Average Order Value Over Time")
        plt.grid()
        plt.savefig("/opt/airflow/dags/avg_order_value.png")
    else:
        logging.error("❌ 'price' column missing in avg_order_value DataFrame. Skipping visualization.")

    logging.info("✅ Visualizations generated successfully.")

# Define DAG
dag = DAG(
    'kpi_analysis_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
    catchup=False
)

# Define tasks
compute_kpi_task = PythonOperator(
    task_id="compute_kpis",
    python_callable=compute_kpis,
    dag=dag
)

generate_visuals_task = PythonOperator(
    task_id="generate_visualizations",
    python_callable=generate_visualizations,
    dag=dag
)

# Set dependencies
compute_kpi_task >> generate_visuals_task
