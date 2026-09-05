from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


# =========================================================
# File Paths
# =========================================================

FILE_PATH = "/opt/airflow/data/sales.csv"
EXTRACTED_FILE = "/opt/airflow/data/extracted_sales.csv"
TRANSFORMED_FILE = "/opt/airflow/data/transformed_sales.csv"


# =========================================================
# 1. EXTRACT
# =========================================================

def extract_data():

    print("========== EXTRACT ==========")

    df = pd.read_csv(FILE_PATH)

    print("Data extracted successfully!")
    print("Number of rows:", len(df))
    print("Number of columns:", len(df.columns))

    print("\nFirst 5 rows:")
    print(df.head())

    # Save extracted data
    df.to_csv(
        EXTRACTED_FILE,
        index=False
    )

    print("\nExtract completed successfully!")


# =========================================================
# 2. VALIDATE
# =========================================================

def validate_data():

    print("========== VALIDATION ==========")

    df = pd.read_csv(EXTRACTED_FILE)

    # Check if dataset is empty
    if df.empty:
        raise ValueError(
            "Dataset is empty!"
        )

    print("Dataset is not empty")

    # Required columns
    required_columns = [
        "order_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "order_date"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns: {missing_columns}"
        )

    print("All required columns exist")

    # Check duplicate order IDs
    duplicates = df["order_id"].duplicated().sum()

    if duplicates > 0:
        raise ValueError(
            f"Found {duplicates} duplicate order IDs"
        )

    print("No duplicate orders")

    # Check quantity
    if (df["quantity"] <= 0).any():
        raise ValueError(
            "Invalid quantity detected!"
        )

    print("Quantity values are valid")

    # Check price
    if (df["price"] <= 0).any():
        raise ValueError(
            "Invalid price detected!"
        )

    print("Price values are valid")

    print("\n DATA VALIDATION PASSED!")


# =========================================================
# 3. TRANSFORM
# =========================================================

def transform_data():

    print("========== TRANSFORMATION ==========")

    df = pd.read_csv(EXTRACTED_FILE)

    # Calculate total amount
    df["total_amount"] = (
        df["quantity"] * df["price"]
    )

    # Convert order_date to datetime
    df["order_date"] = pd.to_datetime(
        df["order_date"]
    )

    # Sort data by order date
    df = df.sort_values(
        "order_date"
    )

    print("total_amount created")
    print("order_date converted to datetime")

    print("\nTransformed data:")
    print(df.head())

    # Save transformed data
    df.to_csv(
        TRANSFORMED_FILE,
        index=False
    )

    print("\nTRANSFORMATION COMPLETED!")


# =========================================================
# 4. LOAD
# =========================================================

def load_data():

    print("========== LOAD ==========")

    # Connect to PostgreSQL
    hook = PostgresHook(
        postgres_conn_id="sales_postgres"
    )

    # Read transformed data
    df = pd.read_csv(
        TRANSFORMED_FILE
    )

    # CSV stores dates as strings,
    # so convert them back to datetime
    df["order_date"] = pd.to_datetime(
        df["order_date"]
    )

    # Get PostgreSQL connection
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_clean (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product VARCHAR(100),
            quantity INTEGER,
            price NUMERIC(10,2),
            order_date DATE,
            total_amount NUMERIC(10,2)
        );
    """)

    # Insert data
    for _, row in df.iterrows():

        cursor.execute("""
            INSERT INTO sales_clean (
                order_id,
                customer_id,
                product,
                quantity,
                price,
                order_date,
                total_amount
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (order_id)
            DO NOTHING;
        """, (
            int(row["order_id"]),
            int(row["customer_id"]),
            row["product"],
            int(row["quantity"]),
            float(row["price"]),
            row["order_date"].date(),
            float(row["total_amount"])
        ))

    # Save changes
    conn.commit()

    # Close connection
    cursor.close()
    conn.close()

    print("Data loaded successfully!")
    print("Rows processed:", len(df))

    print("\nLOAD COMPLETED!")


# =========================================================
# 5. DATA QUALITY CHECK
# =========================================================

def quality_check():

    print("========== QUALITY CHECK ==========")

    # Connect to PostgreSQL
    hook = PostgresHook(
        postgres_conn_id="sales_postgres"
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    # -----------------------------------------------------
    # Check 1: Table is not empty
    # -----------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_clean;
    """)

    row_count = cursor.fetchone()[0]

    print("Rows in database:", row_count)

    if row_count == 0:
        raise ValueError(
            "Quality Check Failed: table is empty!"
        )

    print("Table is not empty")

    # -----------------------------------------------------
    # Check 2: NULL order IDs
    # -----------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_clean
        WHERE order_id IS NULL;
    """)

    null_order_ids = cursor.fetchone()[0]

    if null_order_ids > 0:
        raise ValueError(
            f"Quality Check Failed: "
            f"{null_order_ids} NULL order IDs found"
        )

    print("No NULL order IDs")

    # -----------------------------------------------------
    # Check 3: Invalid quantities
    # -----------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_clean
        WHERE quantity <= 0;
    """)

    invalid_quantity = cursor.fetchone()[0]

    if invalid_quantity > 0:
        raise ValueError(
            f"Quality Check Failed: "
            f"{invalid_quantity} invalid quantities found"
        )

    print("All quantities are valid")

    # -----------------------------------------------------
    # Check 4: Invalid prices
    # -----------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_clean
        WHERE price <= 0;
    """)

    invalid_price = cursor.fetchone()[0]

    if invalid_price > 0:
        raise ValueError(
            f"Quality Check Failed: "
            f"{invalid_price} invalid prices found"
        )

    print("All prices are valid")

    # -----------------------------------------------------
    # Check 5: Invalid total amounts
    # -----------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*)
        FROM sales_clean
        WHERE total_amount <= 0;
    """)

    invalid_total = cursor.fetchone()[0]

    if invalid_total > 0:
        raise ValueError(
            f"Quality Check Failed: "
            f"{invalid_total} invalid total amounts found"
        )

    print("All total amounts are valid")

    # Close connection
    cursor.close()
    conn.close()

    print("\nQUALITY CHECK PASSED!")


# =========================================================
# AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2026, 8, 26),
    schedule="@daily",
    catchup=False,
) as dag:

    # Task 1
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    # Task 2
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    # Task 3
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Task 4
    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # Task 5
    quality = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    # Pipeline order
    extract >> validate >> transform >> load >> quality

