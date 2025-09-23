from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd


# Аргументы по умолчанию: владелец процесса и время отсчёта для задачи
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}


def generate_insert_queries():
    CRM_FILE_PATH = 'sample_files/crm.csv'
    TELEMETRY_FILE_PATH = 'sample_files/telemetry_agg.csv'
    SQL_FILE_PATH = "./dags/sql/insert_queries.sql"

    try:
        execution_date = datetime.now()

        # Загружаем данные из CSV
        crm_df = pd.read_csv(CRM_FILE_PATH)
        telemetry_df = pd.read_csv(TELEMETRY_FILE_PATH)

        with open(SQL_FILE_PATH, 'w', encoding='utf-8') as sql_file:
            # Записываем заголовок
            sql_file.write(
                "-- SQL скрипт для загрузки данных в витрину протезов\n")
            sql_file.write(
                f"-- Сгенерировано: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}\n")
            sql_file.write(
                f"-- Дата выполнения: {execution_date.strftime('%Y-%m-%d')}\n")
            sql_file.write(f"-- Количество записей CRM: {len(crm_df)}\n")
            sql_file.write(
                f"-- Количество записей телеметрии: {len(telemetry_df)}\n\n")

            # Начало транзакции
            sql_file.write("BEGIN;\n\n")

            # SQL для создания временной таблицы (если нужно)
            sql_file.write("-- Создание временной таблицы для новых данных\n")
            sql_file.write("""
CREATE TEMP TABLE temp_reports_data (
    user_id INTEGER,
    user_name VARCHAR(50),
    customer_id INTEGER,
    full_name VARCHAR(100),
    prosthesis_model VARCHAR(50),
    prosthesis_serial VARCHAR(30),
    activation_date DATE,
    last_usage_date DATE,
    days_active INTEGER,
    avg_daily_usage_hours DECIMAL(4,2),
    total_sessions INTEGER,
    avg_session_duration INTEGER,
    movements_count INTEGER,
    grip_usage_count INTEGER,
    battery_avg_duration DECIMAL(4,2),
    error_count INTEGER,
    common_issue VARCHAR(100),
    calibration_status VARCHAR(20),
    satisfaction_score INTEGER,
    therapy_compliance DECIMAL(3,0)
);
\n""")

            # Вставка данных во временную таблицу
            sql_file.write("-- Вставка данных во временную таблицу\n")
            for _, crm_row in crm_df.iterrows():
                user_id = crm_row['user_id']
                telemetry_row = telemetry_df[telemetry_df['user_id'] == user_id]

                if not telemetry_row.empty:
                    telemetry_data = telemetry_row.iloc[0]

                    # Экранирование специальных символов
                    full_name = str(crm_row['full_name']).replace("'", "''")
                    common_issue = str(telemetry_data['common_issue']).replace(
                        "'", "''") if pd.notna(telemetry_data['common_issue']) else ""

                    insert_sql = f"""
INSERT INTO temp_reports_data VALUES (
    {int(user_id)},
    '{crm_row['user_name']}',
    {int(crm_row['customer_id'])},
    '{full_name}',
    '{crm_row['prosthesis_model']}',
    '{crm_row['prosthesis_serial']}',
    '{crm_row['activation_date']}',
    '{telemetry_data['last_usage_date']}',
    {int(telemetry_data['days_active'])},
    {float(telemetry_data['avg_daily_usage_hours'])},
    {int(telemetry_data['total_sessions'])},
    {int(telemetry_data['avg_session_duration'])},
    {int(telemetry_data['movements_count'])},
    {int(telemetry_data['grip_usage_count'])},
    {float(telemetry_data['battery_avg_duration'])},
    {int(telemetry_data['error_count'])},
    '{common_issue}',
    '{telemetry_data['calibration_status']}',
    {int(crm_row['satisfaction_score'])},
    {float(crm_row['therapy_compliance'])}
);\n"""
                    sql_file.write(insert_sql)

            # Основная вставка/обновление в витрину
            sql_file.write(
                "\n-- Вставка/обновление данных в основной витрине\n")
            sql_file.write("""
INSERT INTO reports (
    user_id, user_name, customer_id, full_name, prosthesis_model, prosthesis_serial,
    activation_date, last_usage_date, days_active, avg_daily_usage_hours,
    total_sessions, avg_session_duration, movements_count, grip_usage_count,
    battery_avg_duration, error_count, common_issue, calibration_status,
    satisfaction_score, therapy_compliance
)
SELECT 
    user_id, user_name, customer_id, full_name, prosthesis_model, prosthesis_serial,
    activation_date, last_usage_date, days_active, avg_daily_usage_hours,
    total_sessions, avg_session_duration, movements_count, grip_usage_count,
    battery_avg_duration, error_count, common_issue, calibration_status,
    satisfaction_score, therapy_compliance
FROM temp_reports_data
ON CONFLICT (user_id) DO UPDATE SET
    last_usage_date = EXCLUDED.last_usage_date,
    days_active = EXCLUDED.days_active,
    avg_daily_usage_hours = EXCLUDED.avg_daily_usage_hours,
    total_sessions = EXCLUDED.total_sessions,
    avg_session_duration = EXCLUDED.avg_session_duration,
    movements_count = EXCLUDED.movements_count,
    grip_usage_count = EXCLUDED.grip_usage_count,
    battery_avg_duration = EXCLUDED.battery_avg_duration,
    error_count = EXCLUDED.error_count,
    common_issue = EXCLUDED.common_issue,
    calibration_status = EXCLUDED.calibration_status,
    updated_at = CURRENT_TIMESTAMP;
\n""")

            sql_file.write(
                "-- Вставка ежедневных метрик будет выполнена отдельным скриптом\n")
            sql_file.write("-- См. файл daily_metrics_*.sql\n\n")

            # Очистка временной таблицы
            sql_file.write("-- Очистка временной таблицы\n")
            sql_file.write("DROP TABLE temp_reports_data;\n\n")

            # Завершение транзакции
            sql_file.write("COMMIT;\n\n")

            # Статистика
            sql_file.write(f"-- Статистика обработки:\n")
            sql_file.write(f"-- Обработано пользователей: {len(crm_df)}\n")
            sql_file.write(
                f"-- Дата выполнения: {execution_date.strftime('%Y-%m-%d')}\n")

        print(f"SQL-файл создан: {SQL_FILE_PATH}")
        print(f"Сгенерировано команд для {len(crm_df)} пользователей")

        return f"Создан SQL-файл: {SQL_FILE_PATH}"

    except Exception as e:
        print(f"Ошибка создания SQL-файла: {e}")
        raise


# Определяем DAG
with DAG('csv_to_postgres_dag',
         default_args=default_args,  # аргументы по умолчанию в начале скрипта
         schedule_interval='@once',  # запускаем один раз
         catchup=False) as dag:  # предотвращает повторное выполнение DAG для пропущенных расписаний.

    # Запускаем выполнение оператора PostgresOperator
    clean_reports = PostgresOperator(
        task_id='clean_reports',
        postgres_conn_id='prod_reports_db',
        sql='sql/clean_reports.sql'
    )

    # Опеределяем оператор для вставки данных
    generate_queries = PythonOperator(
        task_id='generate_insert_queries',
        python_callable=generate_insert_queries
    )

    # Запускаем выполнение оператора PostgresOperator
    run_insert_queries = PostgresOperator(
        task_id='run_insert_queries',
        # Название подключения к PostgreSQL в Airflow UI
        postgres_conn_id='prod_reports_db',
        sql='sql/insert_queries.sql'
    )
    clean_reports >> generate_queries >> run_insert_queries
