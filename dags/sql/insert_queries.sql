-- SQL скрипт для загрузки данных в витрину протезов
-- Сгенерировано: 2025-09-23 17:50:42
-- Дата выполнения: 2025-09-23
-- Количество записей CRM: 10
-- Количество записей телеметрии: 10

BEGIN;

-- Создание временной таблицы для новых данных

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

-- Вставка данных во временную таблицу

INSERT INTO temp_reports_data VALUES (
    1001,
    'prothetic1',
    501,
    'Иванов Алексей Петрович',
    'BioHand Pro',
    'BHP-2305001',
    '2024-01-15',
    '2024-12-19',
    28,
    6.5,
    142,
    45,
    12500,
    890,
    8.2,
    3,
    'Случайное отключение',
    'optimal',
    9,
    85.0
);

INSERT INTO temp_reports_data VALUES (
    1002,
    'prothetic2',
    511,
    'Смирнова Мария Игоревна',
    'NeuroLimb Basic',
    'NLB-2402003',
    '2024-02-10',
    '2024-12-19',
    30,
    4.2,
    98,
    32,
    8700,
    520,
    7.8,
    1,
    '',
    'normal',
    7,
    92.0
);

INSERT INTO temp_reports_data VALUES (
    1003,
    'prothetic3',
    502,
    'Петров Дмитрий Сергеевич',
    'BioHand Pro',
    'BHP-2403015',
    '2024-03-05',
    '2024-12-18',
    27,
    7.8,
    165,
    52,
    18900,
    1340,
    8.5,
    0,
    '',
    'optimal',
    10,
    78.0
);

INSERT INTO temp_reports_data VALUES (
    1004,
    'prothetic4',
    503,
    'Козлова Анна Викторовна',
    'SmartGrip Advanced',
    'SGA-2312008',
    '2023-12-20',
    '2024-12-19',
    29,
    5.1,
    115,
    38,
    10200,
    680,
    7.5,
    5,
    'Снижение чувствительности',
    'requires_check',
    8,
    95.0
);

INSERT INTO temp_reports_data VALUES (
    1005,
    'prothetic5',
    504,
    'Васильев Игорь Николаевич',
    'NeuroLimb Basic',
    'NLB-2404012',
    '2024-04-18',
    '2024-12-17',
    18,
    2.3,
    47,
    21,
    4200,
    190,
    6.9,
    8,
    'Частые перезагрузки',
    'needs_calibration',
    6,
    65.0
);

INSERT INTO temp_reports_data VALUES (
    1006,
    'prothetic6',
    505,
    'Николаева Елена Дмитриевна',
    'BioHand Pro',
    'BHP-2405022',
    '2024-05-22',
    '2024-12-19',
    30,
    6.9,
    155,
    44,
    14300,
    950,
    8.3,
    2,
    'Временная потеря связи',
    'normal',
    9,
    88.0
);

INSERT INTO temp_reports_data VALUES (
    1007,
    'prothetic7',
    506,
    'Алексеев Павел Олегович',
    'SmartGrip Advanced',
    'SGA-2406007',
    '2024-06-15',
    '2024-12-16',
    25,
    3.8,
    82,
    30,
    6800,
    410,
    7.2,
    4,
    'Сбои в работе сенсоров',
    'normal',
    7,
    74.0
);

INSERT INTO temp_reports_data VALUES (
    1008,
    'prothetic8',
    507,
    'Фёдорова Ольга Васильевна',
    'NeuroLimb Basic',
    'NLB-2407089',
    '2024-07-08',
    '2024-12-19',
    30,
    8.1,
    178,
    55,
    19600,
    1420,
    8.7,
    1,
    '',
    'optimal',
    10,
    91.0
);

INSERT INTO temp_reports_data VALUES (
    1009,
    'prothetic9',
    508,
    'Дмитриев Сергей Александрович',
    'BioHand Pro',
    'BHP-2408104',
    '2024-08-12',
    '2024-12-18',
    26,
    5.5,
    121,
    39,
    11200,
    730,
    7.9,
    3,
    'Задержка отклика',
    'normal',
    8,
    83.0
);

INSERT INTO temp_reports_data VALUES (
    1010,
    'prothetic10',
    509,
    'Морозова Татьяна Игоревна',
    'SmartGrip Advanced',
    'SGA-2409156',
    '2024-09-25',
    '2024-12-19',
    29,
    6.7,
    148,
    46,
    13800,
    890,
    8.4,
    0,
    '',
    'optimal',
    9,
    89.0
);

-- Вставка/обновление данных в основной витрине

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

-- Вставка ежедневных метрик будет выполнена отдельным скриптом
-- См. файл daily_metrics_*.sql

-- Очистка временной таблицы
DROP TABLE temp_reports_data;

COMMIT;

-- Статистика обработки:
-- Обработано пользователей: 10
-- Дата выполнения: 2025-09-23
