-- ====================================================================
-- MODULE 6
-- Использование встроенных функций
-- Лабораторные решения (SQL)
-- ====================================================================
-- Если вы работаете в своей схеме, при необходимости раскомментируйте:
SET search_path TO Zelent, public;

-- ====================================================================
-- ЗАДАНИЕ 1. ОКРУГЛЕНИЕ РЕЗУЛЬТАТОВ АНАЛИЗОВ
-- ====================================================================
SELECT
    oq.sample_number,
    oq.fe_content,
    oq.sio2_content,
    oq.al2o3_content,
    ROUND(oq.fe_content, 1) AS fe_rounded,
    CEIL(oq.sio2_content) AS sio2_ceil,
    FLOOR(oq.al2o3_content) AS al2o3_floor
FROM fact_ore_quality oq
WHERE oq.date_id = 20240315
ORDER BY oq.fe_content DESC, oq.sample_number;

-- ====================================================================
-- ЗАДАНИЕ 2. ОТКЛОНЕНИЕ ОТ ЦЕЛЕВОГО СОДЕРЖАНИЯ FE
-- ====================================================================
SELECT
    oq.sample_number,
    oq.fe_content,
    ROUND(oq.fe_content - 60.0, 2) AS deviation,
    ROUND(ABS(oq.fe_content - 60.0), 2) AS abs_deviation,
    CASE SIGN(oq.fe_content - 60.0)
        WHEN 1 THEN 'Выше нормы'
        WHEN 0 THEN 'В норме'
        ELSE 'Ниже нормы'
    END AS direction,
    ROUND(POWER(oq.fe_content - 60.0, 2), 2) AS squared_dev
FROM fact_ore_quality oq
WHERE oq.date_id BETWEEN 20240301 AND 20240331
ORDER BY abs_deviation DESC, oq.sample_number
LIMIT 10;

-- ====================================================================
-- ЗАДАНИЕ 3. СТАТИСТИКА ДОБЫЧИ ПО СМЕНАМ
-- ====================================================================
SELECT
    fp.shift_id,
    ds.shift_name,
    COUNT(*) AS record_count,
    ROUND(SUM(fp.tons_mined), 2) AS total_tons,
    ROUND(AVG(fp.tons_mined), 2) AS avg_tons,
    COUNT(DISTINCT fp.operator_id) AS unique_operators
FROM fact_production fp
JOIN dim_shift ds
    ON ds.shift_id = fp.shift_id
WHERE fp.date_id BETWEEN 20240301 AND 20240331
GROUP BY fp.shift_id, ds.shift_name
ORDER BY fp.shift_id;

-- ====================================================================
-- ЗАДАНИЕ 4. СПИСОК ПРИЧИН ПРОСТОЕВ ПО ОБОРУДОВАНИЮ
-- ====================================================================
SELECT
    e.equipment_name,
    STRING_AGG(DISTINCT dr.reason_name, '; ' ORDER BY dr.reason_name) AS reasons,
    ROUND(SUM(fd.duration_min), 2) AS total_min,
    COUNT(*) AS incidents
FROM fact_equipment_downtime fd
JOIN dim_equipment e
    ON e.equipment_id = fd.equipment_id
JOIN dim_downtime_reason dr
    ON dr.reason_id = fd.reason_id
WHERE fd.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_min DESC, incidents DESC, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 5. ПРЕОБРАЗОВАНИЕ DATE_ID И ФОРМАТИРОВАНИЕ ОТЧЕТА
-- ====================================================================
SELECT
    fp.date_id,
    TO_CHAR(TO_DATE(fp.date_id::VARCHAR, 'YYYYMMDD'), 'DD.MM.YYYY') AS formatted_date,
    ROUND(SUM(fp.tons_mined), 2) AS total_tons,
    TO_CHAR(SUM(fp.tons_mined), 'FM999G999G990D00') AS formatted_tons
FROM fact_production fp
WHERE fp.date_id BETWEEN 20240301 AND 20240307
GROUP BY fp.date_id
ORDER BY fp.date_id;

-- ====================================================================
-- ЗАДАНИЕ 6. КЛАССИФИКАЦИЯ ПРОБ И РАСЧЕТ ПРОЦЕНТА КАЧЕСТВА
-- ====================================================================
SELECT
    d.full_date,
    SUM(CASE WHEN oq.fe_content >= 65 THEN 1 ELSE 0 END) AS rich_ore,
    SUM(CASE WHEN oq.fe_content >= 55 AND oq.fe_content < 65 THEN 1 ELSE 0 END) AS medium_ore,
    SUM(CASE WHEN oq.fe_content < 55 THEN 1 ELSE 0 END) AS poor_ore,
    COUNT(*) AS total,
    ROUND(
        100.0
        * SUM(CASE WHEN oq.fe_content >= 60 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS good_pct
FROM fact_ore_quality oq
JOIN dim_date d
    ON d.date_id = oq.date_id
WHERE oq.date_id BETWEEN 20240301 AND 20240331
GROUP BY d.full_date
ORDER BY d.full_date;

-- ====================================================================
-- ЗАДАНИЕ 7. БЕЗОПАСНЫЕ KPI ПО ОПЕРАТОРАМ
-- ====================================================================
SELECT
    o.last_name || ' ' || o.first_name AS operator_name,
    ROUND(SUM(fp.tons_mined), 2) AS total_tons,
    ROUND(COALESCE(SUM(fp.fuel_consumed_l), 0), 2) AS total_fuel,
    ROUND(SUM(fp.tons_mined) / NULLIF(SUM(fp.trips_count), 0), 2) AS tons_per_trip,
    ROUND(COALESCE(SUM(fp.fuel_consumed_l), 0) / NULLIF(SUM(fp.tons_mined), 0), 3) AS fuel_per_ton,
    GREATEST(
        COALESCE(
            ROUND(
                SUM(fp.tons_mined) FILTER (WHERE fp.shift_id = 1)
                / NULLIF(SUM(fp.trips_count) FILTER (WHERE fp.shift_id = 1), 0),
                2
            ),
            0
        ),
        COALESCE(
            ROUND(
                SUM(fp.tons_mined) FILTER (WHERE fp.shift_id = 2)
                / NULLIF(SUM(fp.trips_count) FILTER (WHERE fp.shift_id = 2), 0),
                2
            ),
            0
        )
    ) AS best_shift_tons_per_trip
FROM fact_production fp
JOIN dim_operator o
    ON o.operator_id = fp.operator_id
WHERE fp.date_id BETWEEN 20240301 AND 20240331
GROUP BY o.last_name, o.first_name
ORDER BY tons_per_trip DESC, operator_name;

-- ====================================================================
-- ЗАДАНИЕ 8. АНАЛИЗ ПРОПУСКОВ ДАННЫХ
-- ====================================================================
SELECT
    COUNT(*) AS total_rows,
    COUNT(oq.sio2_content) AS sio2_filled,
    COUNT(*) - COUNT(oq.sio2_content) AS sio2_null,
    ROUND(100.0 * COUNT(oq.sio2_content) / COUNT(*), 1) AS sio2_pct,
    COUNT(oq.al2o3_content) AS al2o3_filled,
    COUNT(*) - COUNT(oq.al2o3_content) AS al2o3_null,
    ROUND(100.0 * COUNT(oq.al2o3_content) / COUNT(*), 1) AS al2o3_pct,
    COUNT(oq.moisture) AS moisture_filled,
    COUNT(*) - COUNT(oq.moisture) AS moisture_null,
    ROUND(100.0 * COUNT(oq.moisture) / COUNT(*), 1) AS moisture_pct,
    COUNT(oq.density) AS density_filled,
    COUNT(*) - COUNT(oq.density) AS density_null,
    ROUND(100.0 * COUNT(oq.density) / COUNT(*), 1) AS density_pct,
    COUNT(oq.sample_weight_kg) AS weight_filled,
    COUNT(*) - COUNT(oq.sample_weight_kg) AS weight_null,
    ROUND(100.0 * COUNT(oq.sample_weight_kg) / COUNT(*), 1) AS weight_pct
FROM fact_ore_quality oq
WHERE oq.date_id BETWEEN 20240301 AND 20240331;

-- ====================================================================
-- ЗАДАНИЕ 9. КОМПЛЕКСНЫЙ KPI-ОТЧЕТ ПО ОБОРУДОВАНИЮ
-- ====================================================================
SELECT
    e.equipment_name,
    et.type_name,
    COUNT(*) AS shift_count,
    ROUND(SUM(fp.tons_mined), 1) AS total_tons,
    ROUND(SUM(fp.operating_hours), 1) AS total_hours,
    ROUND(SUM(fp.tons_mined) / NULLIF(SUM(fp.operating_hours), 0), 2) AS productivity,
    ROUND(SUM(fp.operating_hours) / NULLIF(COUNT(*) * 8.0, 0) * 100, 1) AS utilization,
    ROUND(COALESCE(SUM(fp.fuel_consumed_l), 0) / NULLIF(SUM(fp.tons_mined), 0), 3) AS fuel_per_ton,
    CASE
        WHEN SUM(fp.tons_mined) / NULLIF(SUM(fp.operating_hours), 0) > 20 THEN 'Высокая'
        WHEN SUM(fp.tons_mined) / NULLIF(SUM(fp.operating_hours), 0) > 12 THEN 'Средняя'
        ELSE 'Низкая'
    END AS efficiency_category,
    CASE
        WHEN COUNT(fp.fuel_consumed_l) = COUNT(*) THEN 'Полные'
        ELSE 'Неполные'
    END AS data_status
FROM fact_production fp
JOIN dim_equipment e
    ON e.equipment_id = fp.equipment_id
JOIN dim_equipment_type et
    ON et.equipment_type_id = e.equipment_type_id
WHERE fp.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name, et.type_name
ORDER BY productivity DESC, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 10. КАТЕГОРИЗАЦИЯ ПРОСТОЕВ
-- ====================================================================
WITH categorized AS (
    SELECT
        e.equipment_name,
        dr.reason_name,
        COALESCE(fd.duration_min, 0) AS duration_safe,
        ROUND(COALESCE(fd.duration_min, 0) / 60.0, 1) AS duration_hours,
        CASE
            WHEN COALESCE(fd.duration_min, 0) > 480 THEN 'Критический'
            WHEN COALESCE(fd.duration_min, 0) >= 120 THEN 'Длительный'
            WHEN COALESCE(fd.duration_min, 0) >= 30 THEN 'Средний'
            ELSE 'Короткий'
        END AS category,
        CASE
            WHEN fd.is_planned THEN 'Плановый'
            ELSE 'Внеплановый'
        END AS downtime_status,
        CASE
            WHEN fd.end_time IS NULL THEN 'В процессе'
            ELSE 'Завершён'
        END AS completion_status
    FROM fact_equipment_downtime fd
    JOIN dim_equipment e
        ON e.equipment_id = fd.equipment_id
    JOIN dim_downtime_reason dr
        ON dr.reason_id = fd.reason_id
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
)
SELECT
    c.category,
    COUNT(*) AS cnt,
    ROUND(SUM(c.duration_safe) / 60.0, 1) AS total_hours,
    ROUND(
        100.0 * SUM(c.duration_safe)
        / NULLIF((SELECT SUM(duration_safe) FROM categorized), 0),
        1
    ) AS pct
FROM categorized c
GROUP BY c.category
ORDER BY total_hours DESC, c.category;
