-- ====================================================================
-- MODULE 4
-- Работа с типами данных PostgreSQL
-- Лабораторные решения (SQL)
-- ====================================================================
-- Если вы работаете в своей схеме, при необходимости раскомментируйте:
SET search_path TO "Zelent", public;

-- ====================================================================
-- ЗАДАНИЕ 1. АНАЛИЗ ДЛИНЫ СТРОКОВЫХ ПОЛЕЙ
-- ====================================================================
SELECT
    e.equipment_name,
    LENGTH(e.equipment_name) AS name_len,
    LENGTH(e.inventory_number) AS inv_len,
    LENGTH(COALESCE(e.model, '')) AS model_len,
    LENGTH(COALESCE(e.manufacturer, '')) AS manuf_len,
    LENGTH(e.equipment_name)
        + LENGTH(e.inventory_number)
        + LENGTH(COALESCE(e.model, ''))
        + LENGTH(COALESCE(e.manufacturer, '')) AS total_text_length
FROM dim_equipment e
ORDER BY total_text_length DESC, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 2. РАЗБОР ИНВЕНТАРНОГО НОМЕРА
-- ====================================================================
SELECT
    e.equipment_name,
    e.inventory_number,
    SPLIT_PART(e.inventory_number, '-', 1) AS prefix,
    SPLIT_PART(e.inventory_number, '-', 2) AS type_code,
    CAST(SPLIT_PART(e.inventory_number, '-', 3) AS INTEGER) AS serial_num,
    CASE SPLIT_PART(e.inventory_number, '-', 2)
        WHEN 'LHD' THEN 'Погрузочно-доставочная машина'
        WHEN 'TRK' THEN 'Шахтный самосвал'
        WHEN 'TRUCK' THEN 'Шахтный самосвал'
        WHEN 'CRT' THEN 'Вагонетка'
        WHEN 'CART' THEN 'Вагонетка'
        WHEN 'SKP' THEN 'Скиповой подъёмник'
        WHEN 'SKIP' THEN 'Скиповой подъёмник'
        ELSE 'Неизвестный тип'
    END AS type_description
FROM dim_equipment e
ORDER BY type_code, serial_num, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 3. ФОРМИРОВАНИЕ КРАТКОГО ИМЕНИ ОПЕРАТОРА
-- ====================================================================
SELECT
    o.last_name,
    o.first_name,
    o.middle_name,
    CONCAT(
        o.last_name, ' ',
        LEFT(o.first_name, 1), '.',
        CASE
            WHEN o.middle_name IS NOT NULL THEN LEFT(o.middle_name, 1) || '.'
            ELSE ''
        END
    ) AS short_name_1,
    CONCAT(
        LEFT(o.first_name, 1), '.',
        CASE
            WHEN o.middle_name IS NOT NULL THEN LEFT(o.middle_name, 1) || '. '
            ELSE ' '
        END,
        o.last_name
    ) AS short_name_2,
    UPPER(o.last_name) AS upper_last,
    LOWER(o.position) AS lower_position
FROM dim_operator o
ORDER BY o.last_name, o.first_name, o.middle_name;

-- ====================================================================
-- ЗАДАНИЕ 4A. ПОИСК ОБОРУДОВАНИЯ С "ПДМ" В НАЗВАНИИ
-- ====================================================================
SELECT
    e.equipment_id,
    e.equipment_name,
    e.inventory_number
FROM dim_equipment e
WHERE e.equipment_name LIKE '%ПДМ%'
ORDER BY e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 4B. ПРОИЗВОДИТЕЛИ НА "S" БЕЗ УЧЕТА РЕГИСТРА
-- ====================================================================
SELECT
    e.equipment_id,
    e.equipment_name,
    e.manufacturer
FROM dim_equipment e
WHERE e.manufacturer ILIKE 's%'
ORDER BY e.manufacturer, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 4C. ШАХТЫ С КАВЫЧКАМИ В НАЗВАНИИ
-- ====================================================================
SELECT
    m.mine_id,
    m.mine_name
FROM dim_mine m
WHERE m.mine_name LIKE '%"%'
ORDER BY m.mine_name;

-- ====================================================================
-- ЗАДАНИЕ 4D. ИНВЕНТАРНЫЕ НОМЕРА С СЕРИЙНОЙ ЧАСТЬЮ 001-010
-- ====================================================================
SELECT
    e.equipment_name,
    e.inventory_number
FROM dim_equipment e
WHERE e.inventory_number ~ '^INV-[A-Z]{3}-(00[1-9]|010)$'
ORDER BY e.inventory_number;

-- ====================================================================
-- ЗАДАНИЕ 5. СПИСОК ОБОРУДОВАНИЯ ПО ШАХТАМ
-- ====================================================================
SELECT
    m.mine_name,
    COUNT(*) AS eq_count,
    STRING_AGG(e.equipment_name, ', ' ORDER BY e.equipment_name) AS equipment_list,
    STRING_AGG(DISTINCT e.manufacturer, ', ' ORDER BY e.manufacturer) AS manufacturers
FROM dim_equipment e
JOIN dim_mine m
    ON m.mine_id = e.mine_id
GROUP BY m.mine_name
ORDER BY m.mine_name;

-- ====================================================================
-- ЗАДАНИЕ 6. ВОЗРАСТ ОБОРУДОВАНИЯ
-- ====================================================================
SELECT
    e.equipment_name,
    e.commissioning_date,
    AGE(CURRENT_DATE, e.commissioning_date) AS age_interval,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.commissioning_date))::INT AS years,
    (CURRENT_DATE - e.commissioning_date) AS days,
    CASE
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.commissioning_date)) < 2
            THEN 'Новое'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.commissioning_date)) <= 4
            THEN 'Рабочее'
        ELSE 'Требует внимания'
    END AS category
FROM dim_equipment e
WHERE e.commissioning_date IS NOT NULL
ORDER BY days DESC, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 7. ФОРМАТИРОВАНИЕ ДАТ ДЛЯ ОТЧЕТОВ
-- ====================================================================
SELECT
    e.equipment_name,
    e.commissioning_date,
    TO_CHAR(e.commissioning_date, 'DD.MM.YYYY') AS russian_fmt,
    dd.day_of_month || ' ' || INITCAP(dd.month_name) || ' ' || dd.year || ' г.' AS full_fmt,
    TO_CHAR(e.commissioning_date, 'YYYY-MM-DD') AS iso_fmt,
    dd.year_quarter,
    dd.day_of_week_name,
    dd.year_month
FROM dim_equipment e
JOIN dim_date dd
    ON dd.full_date = e.commissioning_date
WHERE e.commissioning_date IS NOT NULL
ORDER BY e.commissioning_date, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 8A. АНАЛИЗ ПРОСТОЕВ ПО ДНЯМ НЕДЕЛИ
-- ====================================================================
SELECT
    dd.day_of_week_name AS day_of_week,
    COUNT(*) AS downtime_count,
    ROUND(AVG(fd.duration_min), 2) AS avg_duration
FROM fact_equipment_downtime fd
JOIN dim_date dd
    ON dd.full_date = DATE(fd.start_time)
GROUP BY dd.day_of_week, dd.day_of_week_name
ORDER BY dd.day_of_week;

-- ====================================================================
-- ЗАДАНИЕ 8B. АНАЛИЗ ПРОСТОЕВ ПО ЧАСАМ
-- ====================================================================
SELECT
    EXTRACT(HOUR FROM hour_bucket)::INT AS hour_of_day,
    COUNT(*) AS downtime_count
FROM (
    SELECT DATE_TRUNC('hour', fd.start_time) AS hour_bucket
    FROM fact_equipment_downtime fd
) t
GROUP BY hour_of_day
ORDER BY downtime_count DESC, hour_of_day
LIMIT 10;

-- ====================================================================
-- ЗАДАНИЕ 8C. ПИКОВЫЙ ЧАС
-- ====================================================================
SELECT
    EXTRACT(HOUR FROM DATE_TRUNC('hour', fd.start_time))::INT AS peak_hour,
    COUNT(*) AS downtime_count
FROM fact_equipment_downtime fd
GROUP BY peak_hour
ORDER BY downtime_count DESC, peak_hour
LIMIT 1;

-- ====================================================================
-- ЗАДАНИЕ 9. РАСЧЕТ ГРАФИКА КАЛИБРОВКИ ДАТЧИКОВ
-- ====================================================================
SELECT
    s.sensor_code,
    st.type_name,
    e.equipment_name,
    s.calibration_date,
    (CURRENT_DATE - s.calibration_date) AS days_since,
    (s.calibration_date + INTERVAL '180 days')::DATE AS next_calibration,
    CASE
        WHEN CURRENT_DATE - s.calibration_date > 180 THEN 'Просрочена'
        WHEN CURRENT_DATE - s.calibration_date BETWEEN 150 AND 180 THEN 'Скоро'
        ELSE 'В норме'
    END AS cal_status
FROM dim_sensor s
JOIN dim_sensor_type st
    ON st.sensor_type_id = s.sensor_type_id
JOIN dim_equipment e
    ON e.equipment_id = s.equipment_id
WHERE s.calibration_date IS NOT NULL
ORDER BY
    CASE
        WHEN CURRENT_DATE - s.calibration_date > 180 THEN 1
        WHEN CURRENT_DATE - s.calibration_date BETWEEN 150 AND 180 THEN 2
        ELSE 3
    END,
    s.calibration_date,
    s.sensor_code;

-- ====================================================================
-- ЗАДАНИЕ 10. КОМПЛЕКСНЫЙ ОТЧЕТ: КАРТОЧКА ОБОРУДОВАНИЯ
-- ====================================================================
SELECT
    CONCAT(
        '[', et.type_name, '] ',
        e.equipment_name,
        ' (', COALESCE(e.manufacturer, 'Без производителя'), ' ', COALESCE(e.model, ''), ')',
        ' | Шахта: ', REPLACE(REPLACE(m.mine_name, 'Шахта ', ''), '"', ''),
        ' | Введён: ', TO_CHAR(e.commissioning_date, 'DD.MM.YYYY'),
        ' | Возраст: ',
            EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.commissioning_date))::INT,
        ' лет',
        ' | Статус: ',
            CASE e.status
                WHEN 'active' THEN 'АКТИВЕН'
                WHEN 'maintenance' THEN 'НА ТО'
                WHEN 'decommissioned' THEN 'СПИСАН'
                ELSE UPPER(COALESCE(e.status, 'НЕИЗВЕСТНО'))
            END,
        ' | Видеорег.: ', CASE WHEN e.has_video_recorder THEN 'ДА' ELSE 'НЕТ' END,
        ' | Навигация: ', CASE WHEN e.has_navigation THEN 'ДА' ELSE 'НЕТ' END
    ) AS equipment_card
FROM dim_equipment e
JOIN dim_equipment_type et
    ON et.equipment_type_id = e.equipment_type_id
JOIN dim_mine m
    ON m.mine_id = e.mine_id
ORDER BY et.type_name, e.equipment_name;
