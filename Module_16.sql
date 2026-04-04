-- ====================================================================
-- Модуль 16: Программирование при помощи SQL
-- Лабораторные решения (lab_solutions.sql)
-- Предприятие «Руда+» (MES)
-- ====================================================================

-- ====================================================================
-- ЗАДАНИЕ 1. Анонимный блок — статистика по шахтам (простое)
-- ====================================================================
DO $$
DECLARE
    v_mine_count     INT;
    v_total_tons     NUMERIC;
    v_avg_fe         NUMERIC;
    v_downtime_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_mine_count FROM dim_mine;
    
    SELECT COALESCE(SUM(tons_mined), 0) INTO v_total_tons
    FROM fact_production
    WHERE date_id BETWEEN 20250101 AND 20250131;
    
    SELECT ROUND(AVG(fe_content), 1) INTO v_avg_fe
    FROM fact_ore_quality
    WHERE date_id BETWEEN 20250101 AND 20250131;
    
    SELECT COUNT(*) INTO v_downtime_count
    FROM fact_equipment_downtime
    WHERE date_id BETWEEN 20250101 AND 20250131;

    RAISE NOTICE '===== Сводка по предприятию «Руда+» =====';
    RAISE NOTICE 'Количество шахт: %', v_mine_count;
    RAISE NOTICE 'Добыча за январь 2025: % т', ROUND(v_total_tons, 1);
    RAISE NOTICE 'Среднее содержание Fe: % %%', v_avg_fe;
    RAISE NOTICE 'Количество простоев: %', v_downtime_count;
    RAISE NOTICE '===========================================';
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 2. Переменные и классификация — категории оборудования (простое)
-- ====================================================================
DO $$
DECLARE
    rec          RECORD;
    v_age        INT;
    v_category   VARCHAR;
    v_new        INT := 0;
    v_work       INT := 0;
    v_attention  INT := 0;
    v_replace    INT := 0;
BEGIN
    RAISE NOTICE 'equipment_name | type_name                    | age_years | category';
    RAISE NOTICE '---------------+------------------------------+-----------+-------------------';

    FOR rec IN 
        SELECT e.equipment_name, et.type_name, e.commissioning_date
        FROM dim_equipment e
        JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
        ORDER BY e.equipment_name
    LOOP
        -- Вычисление возраста в годах
        v_age := EXTRACT(YEAR FROM AGE(CURRENT_DATE, rec.commissioning_date))::INT;
        
        -- Классификация
        IF v_age < 2 THEN
            v_category := 'Новое'; v_new := v_new + 1;
        ELSIF v_age BETWEEN 2 AND 5 THEN
            v_category := 'Рабочее'; v_work := v_work + 1;
        ELSIF v_age BETWEEN 6 AND 10 THEN
            v_category := 'Требует внимания'; v_attention := v_attention + 1;
        ELSE
            v_category := 'На замену'; v_replace := v_replace + 1;
        END IF;

        RAISE NOTICE '% | % | % | %',
            RPAD(rec.equipment_name, 15),
            RPAD(rec.type_name, 30),
            LPAD(v_age::TEXT, 9),
            v_category;
    END LOOP;

    RAISE NOTICE 'Сводка по категориям:';
    RAISE NOTICE 'Новое: %, Рабочее: %, Требует внимания: %, На замену: %',
        v_new, v_work, v_attention, v_replace;
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 3. Циклы — подневной анализ добычи (простое)
-- ====================================================================
DO $$
DECLARE
    v_day_prod   NUMERIC;
    v_running    NUMERIC := 0;
    v_sum_prev   NUMERIC := 0;
    v_count      INT := 0;
    v_is_record  VARCHAR(10) := '';
    v_best_day   INT := 0;
    v_best_prod  NUMERIC := 0;
BEGIN
    RAISE NOTICE 'day_num | tons   | running_total | is_record';
    RAISE NOTICE '--------+--------+---------------+----------';

    FOR i IN 1..14 LOOP
        SELECT COALESCE(SUM(tons_mined), 0) INTO v_day_prod
        FROM fact_production WHERE date_id = 20250100 + i;

        v_running := v_running + v_day_prod;
        v_count := v_count + 1;
        v_sum_prev := v_sum_prev + v_day_prod;

        -- Проверка на рекорд (текущий день > среднего всех предыдущих)
        IF i = 1 THEN
            v_is_record := '';
        ELSE
            IF v_day_prod > (v_sum_prev - v_day_prod) / (v_count - 1) THEN
                v_is_record := 'РЕКОРД';
            ELSE
                v_is_record := '';
            END IF;
        END IF;

        IF v_day_prod > v_best_prod THEN
            v_best_prod := v_day_prod;
            v_best_day := i;
        END IF;

        RAISE NOTICE '%      | % | % | %',
            LPAD(i::TEXT, 2, '0'),
            LPAD(TO_CHAR(v_day_prod, 'FM9999.9'), 6),
            LPAD(TO_CHAR(v_running, 'FM99999.9'), 7),
            v_is_record;
    END LOOP;

    RAISE NOTICE 'Общий итог за 14 дней: % т. Средняя добыча: % т/день. Лучший день: % января (% т).',
        ROUND(v_running, 1), ROUND(v_running/14, 1), v_best_day, ROUND(v_best_prod, 1);
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 4. WHILE — мониторинг порога простоев (среднее)
-- ====================================================================
DO $$
DECLARE
    v_threshold   NUMERIC := 500;
    v_date_id     INT := 20250101;
    v_daily_hours NUMERIC;
    v_cumulative  NUMERIC := 0;
    v_reached     BOOLEAN := FALSE;
BEGIN
    RAISE NOTICE 'full_date  | downtime_hours | cumulative_hours | status';
    RAISE NOTICE '-----------+----------------+------------------+----------------';

    WHILE v_date_id <= 20250131 LOOP
        SELECT COALESCE(SUM(duration_min)/60.0, 0) INTO v_daily_hours
        FROM fact_equipment_downtime WHERE date_id = v_date_id;

        IF v_daily_hours > 0 THEN
            v_cumulative := v_cumulative + v_daily_hours;
            RAISE NOTICE '% | % | % | %',
                TO_CHAR(TO_DATE(v_date_id::TEXT, 'YYYYMMDD'), 'YYYY-MM-DD'),
                LPAD(TO_CHAR(v_daily_hours, 'FM999.9'), 4),
                LPAD(TO_CHAR(v_cumulative, 'FM9999.9'), 5),
                CASE WHEN v_cumulative >= v_threshold THEN 'Порог достигнут!' ELSE '' END;
        END IF;

        IF v_cumulative >= v_threshold THEN
            v_reached := TRUE;
            RAISE NOTICE 'Порог % ч достигнут на дату %!', v_threshold, TO_DATE(v_date_id::TEXT, 'YYYYMMDD');
            EXIT;
        END IF;

        v_date_id := v_date_id + 1;
        CONTINUE; -- Явный переход к следующей итерации по условию задания
    END LOOP;

    IF NOT v_reached THEN
        RAISE NOTICE 'Порог % ч не был достигнут до конца месяца. Суммарно: % ч.', 
            v_threshold, ROUND(v_cumulative, 1);
    END IF;
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 5. CASE и FOREACH — анализ датчиков (среднее)
-- ====================================================================
DO $$
DECLARE
    v_sensor_types INT[];
    v_type_id      INT;
    v_type_name    VARCHAR;
    v_sensor_count INT;
    v_readings     BIGINT;
    v_avg_per      NUMERIC;
    v_status       VARCHAR;
BEGIN
    RAISE NOTICE 'type_name                      | sensor_count | reading_count | status';
    RAISE NOTICE '-------------------------------+--------------+---------------+-------------------';

    SELECT ARRAY_AGG(DISTINCT sensor_type_id) INTO v_sensor_types FROM dim_sensor_type;

    FOREACH v_type_id IN ARRAY v_sensor_types LOOP
        SELECT type_name INTO v_type_name FROM dim_sensor_type WHERE sensor_type_id = v_type_id;

        SELECT COUNT(*) INTO v_sensor_count FROM dim_sensor WHERE sensor_type_id = v_type_id;
        SELECT COUNT(*) INTO v_readings 
        FROM fact_equipment_telemetry t
        JOIN dim_sensor s ON t.sensor_id = s.sensor_id
        WHERE s.sensor_type_id = v_type_id
          AND t.date_id BETWEEN 20250101 AND 20250131;

        IF v_sensor_count = 0 OR v_readings = 0 THEN
            v_status := 'Нет данных';
        ELSE
            v_avg_per := v_readings::NUMERIC / v_sensor_count;
            CASE
                WHEN v_avg_per > 1000 THEN v_status := 'Активно работает';
                WHEN v_avg_per >= 100 THEN v_status := 'Нормальная работа';
                WHEN v_avg_per >= 1   THEN v_status := 'Редкие показания';
                ELSE v_status := 'Нет данных';
            END CASE;
        END IF;

        RAISE NOTICE '% | % | % | %',
            RPAD(v_type_name, 30), LPAD(v_sensor_count::TEXT, 12),
            LPAD(v_readings::TEXT, 13), v_status;
    END LOOP;
END;
$$;

-- ====================================================================
-- ЗАДАНИЕ 6. Курсор — пакетное формирование отчёта по сменам (среднее)
-- ====================================================================
CREATE TABLE IF NOT EXISTS report_shift_summary (
    report_date    DATE,
    shift_name     VARCHAR(50),
    mine_name      VARCHAR(100),
    total_tons     NUMERIC(12,2),
    equipment_used INT,
    efficiency     NUMERIC(5,1),
    created_at     TIMESTAMP DEFAULT NOW()
);

DO $$
DECLARE
    cur_dates CURSOR FOR
        SELECT date_id FROM dim_date
        WHERE full_date BETWEEN '2025-01-01' AND '2025-01-15'
        ORDER BY date_id;
    v_rows      INT;
    v_total     INT := 0;
BEGIN
    DELETE FROM report_shift_summary WHERE report_date BETWEEN '2025-01-01' AND '2025-01-15';

    FOR rec IN cur_dates LOOP
        INSERT INTO report_shift_summary (report_date, shift_name, mine_name, total_tons, equipment_used, efficiency)
        SELECT
            d.full_date,
            s.shift_name,
            m.mine_name,
            COALESCE(SUM(fp.tons_mined), 0),
            COUNT(DISTINCT fp.equipment_id),
            ROUND(
                COALESCE(SUM(fp.operating_hours), 0) / 
                NULLIF(COUNT(DISTINCT fp.equipment_id) * 8.0, 0) * 100
            , 1)
        FROM fact_production fp
        JOIN dim_date d ON fp.date_id = d.date_id
        JOIN dim_shift s ON fp.shift_id = s.shift_id
        JOIN dim_mine m ON fp.mine_id = m.mine_id
        WHERE fp.date_id = rec.date_id
        GROUP BY d.full_date, s.shift_name, m.mine_name;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total := v_total + v_rows;
        RAISE NOTICE 'Дата %: вставлено % записей', rec.date_id, v_rows;
    END LOOP;
    RAISE NOTICE 'Всего вставлено: % записей', v_total;
END;
$$;

-- Проверка результата:
-- SELECT * FROM report_shift_summary ORDER BY report_date, shift_name, mine_name;

-- ====================================================================
-- ЗАДАНИЕ 7. RETURN NEXT — функция генерации отчёта (сложное)
-- ====================================================================
CREATE OR REPLACE FUNCTION get_quality_trend(
    p_year INT, 
    p_mine_id INT DEFAULT NULL
)
RETURNS TABLE (
    month_num      INT,
    month_name     VARCHAR,
    samples_count  BIGINT,
    avg_fe         NUMERIC,
    min_fe         NUMERIC,
    max_fe         NUMERIC,
    running_avg_fe NUMERIC,
    trend          VARCHAR
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_month_names VARCHAR[] := ARRAY[
        'Январь','Февраль','Март','Апрель','Май','Июнь',
        'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'
    ];
    v_date_from  INT; v_date_to INT;
    v_prev_avg   NUMERIC := NULL;
    v_sum_fe     NUMERIC := 0;
    v_cnt_fe     BIGINT  := 0;
BEGIN
    FOR month_num IN 1..12 LOOP
        month_name := v_month_names[month_num];
        v_date_from := p_year * 10000 + month_num * 100 + 1;
        v_date_to   := p_year * 10000 + month_num * 100 + 31;

        SELECT 
            COUNT(*), 
            ROUND(AVG(fe_content), 2), 
            ROUND(MIN(fe_content), 2), 
            ROUND(MAX(fe_content), 2)
        INTO samples_count, avg_fe, min_fe, max_fe
        FROM fact_ore_quality q
        WHERE q.date_id BETWEEN v_date_from AND v_date_to
          AND (p_mine_id IS NULL OR q.mine_id = p_mine_id);

        samples_count := COALESCE(samples_count, 0);
        avg_fe := COALESCE(avg_fe, 0); min_fe := COALESCE(min_fe, 0); max_fe := COALESCE(max_fe, 0);

        -- Взвешенное нарастающее среднее
        v_sum_fe := v_sum_fe + (avg_fe * samples_count);
        v_cnt_fe := v_cnt_fe + samples_count;
        running_avg_fe := CASE WHEN v_cnt_fe > 0 THEN ROUND(v_sum_fe::NUMERIC / v_cnt_fe, 2) ELSE 0 END;

        -- Определение тренда
        IF month_num = 1 OR samples_count = 0 THEN
            trend := 'Стабильно';
        ELSIF avg_fe > v_prev_avg THEN trend := 'Улучшение';
        ELSIF avg_fe < v_prev_avg THEN trend := 'Ухудшение';
        ELSE trend := 'Стабильно';
        END IF;
        v_prev_avg := avg_fe;

        RETURN NEXT;
    END LOOP;
END;
$$;

-- Тесты:
-- SELECT * FROM get_quality_trend(2025);
-- SELECT * FROM get_quality_trend(2025, 1);

-- ====================================================================
-- ЗАДАНИЕ 8. Комплексная валидация данных (сложное)
-- ====================================================================
CREATE OR REPLACE FUNCTION validate_mes_data(
    p_date_from INT,
    p_date_to   INT
)
RETURNS TABLE (
    check_id      INT,
    check_name    VARCHAR,
    severity      VARCHAR,
    affected_rows BIGINT,
    details       TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_cnt BIGINT;
BEGIN
    -- 1. Отрицательная добыча
    check_id := 1; check_name := 'Отрицательная добыча';
    SELECT COUNT(*) INTO v_cnt FROM fact_production WHERE tons_mined < 0 AND date_id BETWEEN p_date_from AND p_date_to;
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ОШИБКА'; details := format('Найдено %s записей с tons_mined < 0', v_cnt); recommendation := 'Проверить датчики и скрипты загрузки';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 2. Добыча > 500 т
    check_id := 2; check_name := 'Аномальная добыча (>500 т)';
    SELECT COUNT(*) INTO v_cnt FROM fact_production WHERE tons_mined > 500 AND date_id BETWEEN p_date_from AND p_date_to;
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ПРЕДУПРЕЖДЕНИЕ'; details := format('Найдено %s записей > 500 т', v_cnt); recommendation := 'Верифицировать данные сменного рапорта';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 3. Нулевые часы при >0 добыче
    check_id := 3; check_name := 'Нулевые часы при добыче';
    SELECT COUNT(*) INTO v_cnt FROM fact_production WHERE operating_hours = 0 AND tons_mined > 0 AND date_id BETWEEN p_date_from AND p_date_to;
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ОШИБКА'; details := format('%s записей: добыча >0 при 0 часах', v_cnt); recommendation := 'Исправить учёт рабочих часов';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 4. Пропущенные рабочие дни
    check_id := 4; check_name := 'Пропущенные рабочие дни';
    SELECT COUNT(*) INTO v_cnt FROM dim_date d WHERE d.date_id BETWEEN p_date_from AND p_date_to AND d.is_weekend = FALSE 
        AND NOT EXISTS (SELECT 1 FROM fact_production fp WHERE fp.date_id = d.date_id);
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ПРЕДУПРЕЖДЕНИЕ'; details := format('%s рабочих дней без данных', v_cnt); recommendation := 'Проверить логи загрузки';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 5. Fe вне 0-100%
    check_id := 5; check_name := 'Некорректное содержание Fe';
    SELECT COUNT(*) INTO v_cnt FROM fact_ore_quality WHERE (fe_content < 0 OR fe_content > 100) AND date_id BETWEEN p_date_from AND p_date_to;
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ОШИБКА'; details := format('%s проб с Fe за пределами 0-100%%', v_cnt); recommendation := 'Калибровка лаборатории или исправление ошибок ввода';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 6. Простои > 24ч
    check_id := 6; check_name := 'Длительные простои (>24ч)';
    SELECT COUNT(*) INTO v_cnt FROM fact_equipment_downtime WHERE duration_min > 1440 AND date_id BETWEEN p_date_from AND p_date_to;
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ПРЕДУПРЕЖДЕНИЕ'; details := format('%s простоев длительностью > 24 часов', v_cnt); recommendation := 'Инициировать расследование причин простоя';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 7. Оборудование без телеметрии
    check_id := 7; check_name := 'Оборудование без телеметрии';
    SELECT COUNT(*) INTO v_cnt FROM dim_equipment e WHERE NOT EXISTS (
        SELECT 1 FROM fact_equipment_telemetry t WHERE t.equipment_id = e.equipment_id AND t.date_id BETWEEN p_date_from AND p_date_to
    );
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ПРЕДУПРЕЖДЕНИЕ'; details := format('%s единиц оборудования без телеметрии', v_cnt); recommendation := 'Проверить датчики и подключение';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;

    -- 8. Дублирование записей
    check_id := 8; check_name := 'Дублирование записей';
    SELECT COUNT(*) INTO v_cnt FROM (
        SELECT 1 FROM fact_production WHERE date_id BETWEEN p_date_from AND p_date_to
        GROUP BY equipment_id, shift_id, date_id HAVING COUNT(*) > 1
    ) sub;
    affected_rows := v_cnt;
    IF v_cnt > 0 THEN severity := 'ОШИБКА'; details := format('%s групп дублирующихся записей', v_cnt); recommendation := 'Настроить уникальный индекс или проверить ETL';
    ELSE severity := 'ИНФО'; details := 'Отклонений нет'; recommendation := NULL; END IF;
    RETURN NEXT;
END;
$$;

-- Тест:
-- SELECT * FROM validate_mes_data(20250101, 20250131) ORDER BY severity DESC, affected_rows DESC;

-- ====================================================================
-- ОЧИСТКА (опционально)
-- ====================================================================
-- DROP FUNCTION IF EXISTS get_quality_trend(INT, INT);
-- DROP FUNCTION IF EXISTS validate_mes_data(INT, INT);
-- DROP TABLE IF EXISTS report_shift_summary;