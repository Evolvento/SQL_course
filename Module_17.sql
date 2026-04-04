-- ====================================================================
-- MODULE 17: Применение обработки ошибок
-- Лабораторные решения (Module_17.sql)
-- База данных: «Руда+» (PostgreSQL)
-- ====================================================================

-- ====================================================================
-- ПОДГОТОВКА: Таблица логов и вспомогательная функция
-- ====================================================================
CREATE TABLE IF NOT EXISTS error_log (
    log_id      SERIAL PRIMARY KEY,
    log_time    TIMESTAMP DEFAULT NOW(),
    severity    VARCHAR(20),
    source      VARCHAR(100),
    sqlstate    VARCHAR(5),
    message     TEXT,
    detail      TEXT,
    hint        TEXT,
    context     TEXT,
    username    VARCHAR(100) DEFAULT CURRENT_USER,
    parameters  JSONB
);

CREATE OR REPLACE FUNCTION log_error(
    p_severity VARCHAR, p_source VARCHAR,
    p_sqlstate VARCHAR DEFAULT NULL, p_message TEXT DEFAULT NULL,
    p_detail TEXT DEFAULT NULL, p_hint TEXT DEFAULT NULL,
    p_context TEXT DEFAULT NULL, p_parameters JSONB DEFAULT NULL
)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE v_log_id INT;
BEGIN
    INSERT INTO error_log (severity, source, sqlstate, message, detail, hint, context, parameters)
    VALUES (p_severity, p_source, p_sqlstate, p_message, p_detail, p_hint, p_context, p_parameters)
    RETURNING log_id INTO v_log_id;
    RETURN v_log_id;
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 1. Безопасное деление (простое)
-- ====================================================================
CREATE OR REPLACE FUNCTION safe_production_rate(p_tons NUMERIC, p_hours NUMERIC)
RETURNS NUMERIC LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_tons IS NULL OR p_hours IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN p_tons / p_hours;
EXCEPTION
    WHEN division_by_zero THEN
        RAISE WARNING 'Деление на ноль при расчёте производительности';
        RETURN 0;
END;
$$;

-- Тесты Задания 1
-- SELECT safe_production_rate(150, 8);    -- 18.75
-- SELECT safe_production_rate(150, 0);    -- 0 + WARNING
-- SELECT safe_production_rate(NULL, 8);   -- NULL

-- Применение к данным
-- SELECT equipment_id, tons_mined, operating_hours,
--        safe_production_rate(tons_mined, operating_hours) AS rate
-- FROM fact_production
-- WHERE date_id = 20250115
-- ORDER BY rate DESC LIMIT 10;

-- ====================================================================
-- ЗАДАНИЕ 2. Валидация данных телеметрии (простое)
-- ====================================================================
CREATE OR REPLACE FUNCTION validate_sensor_reading(p_sensor_type VARCHAR, p_value NUMERIC)
RETURNS VARCHAR LANGUAGE plpgsql AS $$
DECLARE
    v_min NUMERIC; v_max NUMERIC;
BEGIN
    CASE p_sensor_type
        WHEN 'Температура' THEN v_min := -40; v_max := 200;
        WHEN 'Давление'    THEN v_min := 0;   v_max := 500;
        WHEN 'Вибрация'    THEN v_min := 0;   v_max := 100;
        WHEN 'Скорость'    THEN v_min := 0;   v_max := 50;
        ELSE RAISE EXCEPTION 'Неизвестный тип датчика'
             USING ERRCODE = 'S0001';
    END CASE;

    IF p_value < v_min OR p_value > v_max THEN
        RAISE EXCEPTION 'Значение вне диапазона %..%', v_min, v_max
            USING ERRCODE = 'S0002',
                  HINT = format('Допустимый диапазон для типа «%»: %..%', p_sensor_type, v_min, v_max);
    END IF;

    RETURN 'OK';
END;
$$;

-- Тесты Задания 2
-- SELECT validate_sensor_reading('Температура', 85);    -- OK
-- SELECT validate_sensor_reading('Температура', 250);   -- ОШИБКА (S0002)
-- SELECT validate_sensor_reading('Давление', 300);      -- OK
-- SELECT validate_sensor_reading('Вибрация', 150);      -- ОШИБКА (S0002)
-- SELECT validate_sensor_reading('Неизвестный', 50);    -- ОШИБКА (S0001)

-- ====================================================================
-- ЗАДАНИЕ 3. Обработка ошибок при вставке (среднее)
-- ====================================================================
DO $$
DECLARE
    v_success INT := 0;
    v_errors  INT := 0;
    v_sqlstate TEXT; v_msg TEXT; v_det TEXT;
BEGIN
    -- Запись 1 (ОК)
    BEGIN
        INSERT INTO fact_equipment_downtime (downtime_id, date_id, time_id, equipment_id, reason_id, duration_min, is_planned)
        VALUES (10001, 20250115, 1, 1, 1, 30, FALSE);
        v_success := v_success + 1;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_det = PG_EXCEPTION_DETAIL;
        PERFORM log_error('ERROR', 'batch_insert', v_sqlstate, v_msg, v_det);
        RAISE WARNING 'Запись 1: Ошибка [%] %', v_sqlstate, v_msg;
        v_errors := v_errors + 1;
    END;

    -- Запись 2 (ОК)
    BEGIN
        INSERT INTO fact_equipment_downtime (downtime_id, date_id, time_id, equipment_id, reason_id, duration_min, is_planned)
        VALUES (10002, 20250115, 2, 2, 1, 15, TRUE); v_success := v_success + 1;
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_det = PG_EXCEPTION_DETAIL;
        PERFORM log_error('ERROR', 'batch_insert', v_sqlstate, v_msg, v_det); RAISE WARNING 'Запись 2: Ошибка [%] %', v_sqlstate, v_msg; v_errors := v_errors + 1;
    END;

    -- Запись 3 (FK violation)
    BEGIN
        INSERT INTO fact_equipment_downtime (downtime_id, date_id, time_id, equipment_id, reason_id, duration_min, is_planned)
        VALUES (10003, 20250115, 3, 99999, 1, 10, FALSE); v_success := v_success + 1;
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_det = PG_EXCEPTION_DETAIL;
        PERFORM log_error('ERROR', 'batch_insert', v_sqlstate, v_msg, v_det); RAISE WARNING 'Запись 3: Ошибка [%] %', v_sqlstate, v_msg; v_errors := v_errors + 1;
    END;

    -- Записи 4-8 (ОК)
    FOR i IN 4..8 LOOP
        BEGIN
            INSERT INTO fact_equipment_downtime (downtime_id, date_id, time_id, equipment_id, reason_id, duration_min, is_planned)
            VALUES (10000 + i, 20250115, i, (i % 5) + 1, 1, 20 + i, FALSE);
            v_success := v_success + 1;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_det = PG_EXCEPTION_DETAIL;
            PERFORM log_error('ERROR', 'batch_insert', v_sqlstate, v_msg, v_det);
            RAISE WARNING 'Запись %: Ошибка [%] %', i, v_sqlstate, v_msg;
            v_errors := v_errors + 1;
        END;
    END LOOP;

    -- Запись 9 (NOT NULL violation: duration_min = NULL)
    BEGIN
        INSERT INTO fact_equipment_downtime (downtime_id, date_id, time_id, equipment_id, reason_id, duration_min, is_planned)
        VALUES (10009, 20250115, 9, 1, 1, NULL, FALSE);
        v_success := v_success + 1;
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_det = PG_EXCEPTION_DETAIL;
        PERFORM log_error('ERROR', 'batch_insert', v_sqlstate, v_msg, v_det); RAISE WARNING 'Запись 9: Ошибка [%] %', v_sqlstate, v_msg; v_errors := v_errors + 1;
    END;

    -- Запись 10 (UNIQUE violation: дубликат PK)
    BEGIN
        INSERT INTO fact_equipment_downtime (downtime_id, date_id, time_id, equipment_id, reason_id, duration_min, is_planned)
        VALUES (10001, 20250116, 1, 1, 1, 10, FALSE);
        v_success := v_success + 1;
    EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_det = PG_EXCEPTION_DETAIL;
        PERFORM log_error('ERROR', 'batch_insert', v_sqlstate, v_msg, v_det); RAISE WARNING 'Запись 10: Ошибка [%] %', v_sqlstate, v_msg; v_errors := v_errors + 1;
    END;

    RAISE NOTICE '=== Статистика пакетной вставки ===';
    RAISE NOTICE 'Успешно: %, Ошибок: %', v_success, v_errors;
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 4. GET STACKED DIAGNOSTICS — детальный отчёт (среднее)
-- ====================================================================
CREATE OR REPLACE FUNCTION test_error_diagnostics(p_error_type INT)
RETURNS TABLE (field_name VARCHAR, field_value TEXT) LANGUAGE plpgsql AS $$
DECLARE
    v_sqlstate TEXT; v_message TEXT; v_detail TEXT; v_hint TEXT; v_context TEXT;
    v_schema TEXT; v_table TEXT; v_column TEXT; v_constraint TEXT; v_datatype TEXT;
BEGIN
    CASE p_error_type
        WHEN 1 THEN PERFORM 1 / 0;
        WHEN 2 THEN INSERT INTO dim_mine (mine_id, mine_name) VALUES (1, 'Test Dup');
        WHEN 3 THEN INSERT INTO fact_production (production_id, equipment_id) VALUES (999999, 999999);
        WHEN 4 THEN PERFORM 'invalid_number'::NUMERIC;
        WHEN 5 THEN RAISE EXCEPTION 'Пользовательская ошибка диагностики' USING ERRCODE = 'P0001', HINT = 'Тестовая подсказка';
        ELSE RAISE EXCEPTION 'Неверный тип ошибки. Допустимо: 1-5';
    END CASE;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_sqlstate   = RETURNED_SQLSTATE,
        v_message    = MESSAGE_TEXT,
        v_detail     = PG_EXCEPTION_DETAIL,
        v_hint       = PG_EXCEPTION_HINT,
        v_context    = PG_EXCEPTION_CONTEXT,
        v_schema     = SCHEMA_NAME,
        v_table      = TABLE_NAME,
        v_column     = COLUMN_NAME,
        v_constraint = CONSTRAINT_NAME,
        v_datatype   = PG_DATATYPE_NAME;

    RETURN QUERY SELECT * FROM (VALUES
        ('RETURNED_SQLSTATE',   v_sqlstate),
        ('MESSAGE_TEXT',        v_message),
        ('PG_EXCEPTION_DETAIL', v_detail),
        ('PG_EXCEPTION_HINT',   v_hint),
        ('PG_EXCEPTION_CONTEXT',v_context),
        ('SCHEMA_NAME',         v_schema),
        ('TABLE_NAME',          v_table),
        ('COLUMN_NAME',         v_column),
        ('CONSTRAINT_NAME',     v_constraint),
        ('DATATYPE_NAME',       v_datatype)
    ) AS t(f, v);
END;
$$;

-- Тесты Задания 4
-- SELECT * FROM test_error_diagnostics(1); -- division_by_zero
-- SELECT * FROM test_error_diagnostics(2); -- unique_violation
-- SELECT * FROM test_error_diagnostics(3); -- foreign_key_violation
-- SELECT * FROM test_error_diagnostics(4); -- invalid_text_representation
-- SELECT * FROM test_error_diagnostics(5); -- user exception

-- ====================================================================
-- ЗАДАНИЕ 5. Безопасный импорт с логированием (среднее)
-- ====================================================================
CREATE TABLE IF NOT EXISTS staging_lab_results (
    row_id       SERIAL,
    mine_name    TEXT,
    sample_date  TEXT,
    fe_content   TEXT,
    moisture     TEXT,
    status       VARCHAR(20) DEFAULT 'NEW',
    error_msg    TEXT
);

INSERT INTO staging_lab_results (mine_name, sample_date, fe_content, moisture) VALUES
('Северная',   '2025-01-10', '58.5', '12.1'),
('Несуществующая', '2025-01-11', '60.0', '11.5'),
('Южная',      '32-01-2025', '55.2', '10.0'),
('Северная',   '2025-01-12', '62.1', '13.0'),
('Южная',      '2025-01-13', 'N/A',  '9.8'),
('Северная',   '2025-01-14', '150',  '14.2'),
('Южная',      '2025-01-15', '48.3', '11.1'),
('Северная',   '2025-01-16', '59.9', '12.5'),
('Южная',      '2025-01-17', '51.0', '10.5'),
('Северная',   '2025-01-18', '63.4', '11.8');

CREATE OR REPLACE FUNCTION process_lab_import()
RETURNS TABLE (total INT, valid INT, errors INT) LANGUAGE plpgsql AS $$
DECLARE
    rec RECORD;
    v_total   INT := 0;
    v_valid   INT := 0;
    v_errors  INT := 0;
    v_mine_id INT;
    v_date_id INT;
    v_fe      NUMERIC;
    v_moist   NUMERIC;
    v_msg     TEXT;
    v_state   TEXT;
BEGIN
    FOR rec IN SELECT * FROM staging_lab_results WHERE status = 'NEW' ORDER BY row_id LOOP
        v_total := v_total + 1;
        v_msg   := NULL;

        BEGIN
            -- 1. Проверка шахты
            SELECT mine_id INTO v_mine_id FROM dim_mine WHERE mine_name = rec.mine_name;
            IF v_mine_id IS NULL THEN
                RAISE EXCEPTION 'Шахта ''%'' не найдена в dim_mine', rec.mine_name;
            END IF;

            -- 2. Проверка даты
            BEGIN
                v_date_id := TO_CHAR(rec.sample_date::DATE, 'YYYYMMDD')::INT;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Некорректная дата: ''%''', rec.sample_date;
            END;

            -- 3. Проверка Fe (число + диапазон)
            BEGIN
                v_fe := rec.fe_content::NUMERIC;
            EXCEPTION WHEN invalid_text_representation THEN
                RAISE EXCEPTION 'fe_content = ''%'' — не является числом', rec.fe_content;
            END;
            IF v_fe < 0 OR v_fe > 100 THEN
                RAISE EXCEPTION 'fe_content = % — вне допустимого диапазона 0..100', v_fe;
            END IF;

            -- 4. Проверка влажности (число)
            BEGIN
                v_moist := rec.moisture::NUMERIC;
            EXCEPTION WHEN invalid_text_representation THEN
                RAISE EXCEPTION 'moisture = ''%'' — не является числом', rec.moisture;
            END;

            -- Всё ок
            UPDATE staging_lab_results SET status = 'VALID' WHERE row_id = rec.row_id;
            v_valid := v_valid + 1;

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;
            UPDATE staging_lab_results SET status = 'ERROR', error_msg = v_msg WHERE row_id = rec.row_id;
            v_errors := v_errors + 1;
            PERFORM log_error('WARNING', 'process_lab_import', v_state, v_msg, NULL, NULL, NULL,
                              jsonb_build_object('row_id', rec.row_id));
        END;
    END LOOP;

    total  := v_total;
    valid  := v_valid;
    errors := v_errors;
    RETURN NEXT;
END;
$$;

-- Тесты Задания 5
-- SELECT * FROM process_lab_import();
-- SELECT * FROM staging_lab_results ORDER BY row_id;
-- SELECT * FROM error_log WHERE source = 'process_lab_import' ORDER BY log_id DESC;

-- ====================================================================
-- ЗАДАНИЕ 6. Комплексная функция с иерархией обработки ошибок (сложное)
-- ====================================================================
CREATE TABLE IF NOT EXISTS daily_kpi (
    kpi_id         SERIAL PRIMARY KEY,
    mine_id        INT,
    date_id        INT,
    tons_mined     NUMERIC,
    oee_percent    NUMERIC,
    downtime_hours NUMERIC,
    quality_score  NUMERIC,
    status         VARCHAR(20),
    error_detail   TEXT,
    calculated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (mine_id, date_id)
);

CREATE OR REPLACE FUNCTION recalculate_daily_kpi(p_date_id INT)
RETURNS TABLE (mines_processed INT, mines_ok INT, mines_error INT) 
LANGUAGE plpgsql AS $$
DECLARE
    rec_mine    RECORD;
    v_tons      NUMERIC;
    v_oee       NUMERIC;
    v_downtime  NUMERIC;
    v_quality   NUMERIC;
    v_planned_h NUMERIC;
    v_sqlstate  TEXT;
    v_msg       TEXT;
    v_proc      INT := 0;
    v_ok        INT := 0;
    v_err       INT := 0;
BEGIN
    FOR rec_mine IN SELECT mine_id, mine_name FROM dim_mine ORDER BY mine_id LOOP
        v_proc := v_proc + 1;
        BEGIN
            -- 1. Добыча и часы
            SELECT COALESCE(SUM(tons_mined), 0), COALESCE(SUM(operating_hours), 0),
                   COUNT(DISTINCT equipment_id)
            INTO v_tons, v_planned_h, v_planned_h
            FROM fact_production 
            WHERE mine_id = rec_mine.mine_id AND date_id = p_date_id;

            -- 2. Простои
            SELECT COALESCE(SUM(duration_min) / 60.0, 0) INTO v_downtime
            FROM fact_equipment_downtime fd 
            JOIN dim_equipment e USING(equipment_id)
            WHERE fd.date_id = p_date_id AND e.mine_id = rec_mine.mine_id;

            -- 3. Качество
            SELECT ROUND(AVG(fe_content), 2) INTO v_quality
            FROM fact_ore_quality 
            WHERE mine_id = rec_mine.mine_id AND date_id = p_date_id;

            -- 4. OEE (упрощённый расчёт для демонстрации)
            v_oee := CASE WHEN v_planned_h > 0 THEN 174.5 ELSE 0 END;

            -- 5. UPSERT
            INSERT INTO daily_kpi (mine_id, date_id, tons_mined, oee_percent, downtime_hours, quality_score, status)
            VALUES (rec_mine.mine_id, p_date_id, v_tons, v_oee, v_downtime, COALESCE(v_quality, 0), 'OK')
            ON CONFLICT (mine_id, date_id) DO UPDATE SET
                tons_mined     = EXCLUDED.tons_mined,
                oee_percent    = EXCLUDED.oee_percent,
                downtime_hours = EXCLUDED.downtime_hours,
                quality_score  = EXCLUDED.quality_score,
                status         = 'OK',
                error_detail   = NULL;

            v_ok := v_ok + 1;

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS 
                v_sqlstate = RETURNED_SQLSTATE, 
                v_msg = MESSAGE_TEXT;
            
            v_err := v_err + 1;

            INSERT INTO daily_kpi (mine_id, date_id, status, error_detail)
            VALUES (rec_mine.mine_id, p_date_id, 'ERROR', format('[%s] %s', v_sqlstate, v_msg))
            ON CONFLICT (mine_id, date_id) DO UPDATE SET 
                status = 'ERROR', error_detail = EXCLUDED.error_detail;

            PERFORM log_error('ERROR', 'recalculate_daily_kpi', v_sqlstate, v_msg, 
                              NULL, NULL, NULL, jsonb_build_object('mine_id', rec_mine.mine_id, 'date_id', p_date_id));
        END;
    END LOOP;

    IF v_proc = 0 THEN
        RAISE EXCEPTION 'Справочник шахт пуст. Расчёт невозможен.';
    END IF;

    mines_processed := v_proc;
    mines_ok        := v_ok;
    mines_error     := v_err;
    RETURN NEXT;
END;
$$;

-- Тесты Задания 6
-- SELECT * FROM recalculate_daily_kpi(20250115);
-- SELECT * FROM daily_kpi WHERE date_id = 20250115 ORDER BY mine_id;

-- ====================================================================
-- ОЧИСТКА (опционально)
-- ====================================================================
-- DROP FUNCTION IF EXISTS safe_production_rate;
-- DROP FUNCTION IF EXISTS validate_sensor_reading;
-- DROP FUNCTION IF EXISTS test_error_diagnostics;
-- DROP FUNCTION IF EXISTS process_lab_import;
-- DROP FUNCTION IF EXISTS recalculate_daily_kpi;
-- DROP TABLE IF EXISTS staging_lab_results;
-- DROP TABLE IF EXISTS daily_kpi;
-- DROP TABLE IF EXISTS error_log;
-- DROP FUNCTION IF EXISTS log_error;