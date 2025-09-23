CREATE TABLE reports (
    user_id INTEGER PRIMARY KEY, -- Уникальный идентификатор пользователя
    user_name VARCHAR(50) UNIQUE NOT NULL, -- Уникальное имя пользователя
    customer_id INTEGER NOT NULL, -- ID клиента из CRM
    full_name VARCHAR(100) NOT NULL, -- полное имя пользователя из CRM
    prosthesis_model VARCHAR(50) NOT NULL, -- модель протеза из CRM
    prosthesis_serial VARCHAR(30) UNIQUE, -- серийный номер протеза из CRM
    activation_date DATE NOT NULL, -- дата активации протеза из CRM
    last_usage_date DATE, -- дата последнего использования
    days_active INTEGER DEFAULT 0, -- Количество дней с активностью за последние 30 дней
    avg_daily_usage_hours DECIMAL(4,2) DEFAULT 0, -- Среднее время использования в день (часы)
    total_sessions INTEGER DEFAULT 0, -- Общее количество сеансов использования
    avg_session_duration INTEGER DEFAULT 0, -- Средняя длительность сеанса (минуты)
    movements_count INTEGER DEFAULT 0, -- Общее количество зарегистрированных движений
    grip_usage_count INTEGER DEFAULT 0, -- Количество использований захвата
    battery_avg_duration DECIMAL(4,2) DEFAULT 0, -- Среднее время работы от аккумулятора (часы)
    error_count INTEGER DEFAULT 0, -- Количество ошибок за период
    common_issue VARCHAR(100), -- Наиболее частая проблема
    calibration_status VARCHAR(20) DEFAULT 'normal', -- Статус калибровки устройства
    satisfaction_score INTEGER CHECK (satisfaction_score BETWEEN 1 AND 10), -- Оценка удовлетворенности (1-10)
    therapy_compliance DECIMAL(3,0) CHECK (therapy_compliance BETWEEN 0 AND 100), -- Соответствие рекомендованной терапии (%)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Время создания записи
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Время последнего обновления
);

CREATE INDEX idx_user_activity ON reports (last_usage_date, days_active);
CREATE INDEX idx_prosthesis_model ON reports (prosthesis_model);
CREATE INDEX idx_customer_segment ON reports (customer_id, activation_date);
CREATE INDEX idx_usage_intensity ON reports (avg_daily_usage_hours DESC);

CREATE USER airflow_reports_user WITH PASSWORD 'airflow_reports_password';
GRANT ALL PRIVILEGES ON reports TO airflow_reports_user;
