from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

TABLE_NAME = 'staging_data'
CONN_ID = 'postgres_qualidade'
CSV_PATH = '/opt/airflow/data/data_quality_dataset.csv'
PARQUET_PATH = '/opt/airflow/data/data_quality_dataset.parquet'
TEMP_CSV_PATH = '/tmp/temp_data_quality.csv'


# -----------------------------
# Helpers
# -----------------------------

def get_hook():
    return PostgresHook(postgres_conn_id=CONN_ID)


def run_query(query):
    return get_hook().get_records(query)


def log_result(check_name, status, message=""):
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO data_quality_logs (check_name, status, execution_date, error_message)
        VALUES (%s, %s, NOW(), %s);
    """, (check_name, status, message))

    conn.commit()


# -----------------------------
# Create tables
# -----------------------------

def create_tables(**context):
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT,
            nome TEXT,
            cpf TEXT,
            email TEXT,
            preco FLOAT,
            data_pedido TIMESTAMP,
            data_entrega TIMESTAMP,
            data_atualizacao TIMESTAMP
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_logs (
            id SERIAL PRIMARY KEY,
            check_name VARCHAR(100),
            status VARCHAR(20),
            execution_date TIMESTAMP,
            error_message TEXT
        );
    """)

    conn.commit()


# -----------------------------
# Load data
# -----------------------------

def load_data(**context):
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")

    try:
        if os.path.exists(CSV_PATH):
            load_path = CSV_PATH
        else:
            import pandas as pd
            df = pd.read_parquet(PARQUET_PATH)
            df.to_csv(TEMP_CSV_PATH, index=False)
            load_path = TEMP_CSV_PATH

        print(f"📂 Carregando arquivo: {load_path}")

        with open(load_path, 'r') as f:
            cursor.copy_expert(
                f"COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER",
                f
            )

        conn.commit()

    except Exception as e:
        log_result("load_data", "FAIL", str(e))


# -----------------------------
# Checks padronizados
# -----------------------------

def check_completeness(**context):
    check_name = 'completeness'
    try:
        result = run_query(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE email IS NULL;")[0][0]

        if result > 0:
            log_result(check_name, 'FAIL', f"{result} emails nulos")
        else:
            log_result(check_name, 'SUCCESS')

    except Exception as e:
        log_result(check_name, 'FAIL', str(e))


def check_uniqueness(**context):
    check_name = 'uniqueness'
    try:
        result = run_query(f"""
            SELECT COUNT(*) FROM (
                SELECT cpf
                FROM {TABLE_NAME}
                GROUP BY cpf
                HAVING COUNT(*) > 1
            ) t;
        """)[0][0]

        if result > 0:
            log_result(check_name, 'FAIL', f"{result} CPFs duplicados")
        else:
            log_result(check_name, 'SUCCESS')

    except Exception as e:
        log_result(check_name, 'FAIL', str(e))


def check_validity(**context):
    check_name = 'validity'
    try:
        result = run_query(f"""
            SELECT COUNT(*)
            FROM {TABLE_NAME}
            WHERE email IS NOT NULL
            AND email NOT LIKE '%@%.%';
        """)[0][0]

        if result > 0:
            log_result(check_name, 'FAIL', f"{result} emails inválidos")
        else:
            log_result(check_name, 'SUCCESS')

    except Exception as e:
        log_result(check_name, 'FAIL', str(e))


def check_consistency(**context):
    check_name = 'consistency'
    try:
        result = run_query(f"""
            SELECT COUNT(*) 
            FROM {TABLE_NAME} 
            WHERE data_entrega < data_pedido;
        """)[0][0]

        if result > 0:
            log_result(check_name, 'FAIL', f"{result} datas inconsistentes")
        else:
            log_result(check_name, 'SUCCESS')

    except Exception as e:
        log_result(check_name, 'FAIL', str(e))


def check_freshness(**context):
    check_name = 'freshness'
    try:
        result = run_query(f"SELECT MAX(data_atualizacao) FROM {TABLE_NAME};")[0][0]

        if result is None:
            log_result(check_name, 'FAIL', "tabela sem dados")
            return

        # 🔥 CORREÇÃO TIMEZONE
        from datetime import datetime
        delta = datetime.now() - result

        if delta.days > 1:
            log_result(check_name, 'FAIL', f"dados desatualizados: {delta.days} dias")
        else:
            log_result(check_name, 'SUCCESS')

    except Exception as e:
        log_result(check_name, 'FAIL', str(e))


def check_accuracy(**context):
    check_name = 'accuracy'
    try:
        result = run_query(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE preco < 0;")[0][0]

        if result > 0:
            log_result(check_name, 'FAIL', f"{result} preços negativos")
        else:
            log_result(check_name, 'SUCCESS')

    except Exception as e:
        log_result(check_name, 'FAIL', str(e))

def clear_logs(**context):
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE data_quality_logs;")
    conn.commit()

# -----------------------------
# DAG
# -----------------------------

with DAG(
    dag_id='data_quality_pipeline',
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=['data-quality']
) as dag:

    t0 = PythonOperator(task_id='create_tables', python_callable=create_tables)

    t_load = PythonOperator(task_id='load_data', python_callable=load_data)

    t1 = PythonOperator(task_id='check_completeness', python_callable=check_completeness)
    t2 = PythonOperator(task_id='check_uniqueness', python_callable=check_uniqueness)
    t3 = PythonOperator(task_id='check_validity', python_callable=check_validity)
    t4 = PythonOperator(task_id='check_consistency', python_callable=check_consistency)
    t5 = PythonOperator(task_id='check_freshness', python_callable=check_freshness)
    t6 = PythonOperator(task_id='check_accuracy', python_callable=check_accuracy)
    t_clear = PythonOperator(task_id='clear_logs',python_callable=clear_logs)

    t0 >> t_load >> t_clear >> t1 >> t2 >> t3 >> t4 >> t5 >> t6