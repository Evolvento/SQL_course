-- ====================================================================
-- MODULE 7
-- Введение в индексы
-- Лабораторные решения (SQL)
-- ====================================================================
SET search_path TO Zelent, public;

-- ====================================================================
-- ЗАДАНИЕ 1. АНАЛИЗ СУЩЕСТВУЮЩИХ ИНДЕКСОВ
-- ====================================================================
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('fact_production', 'fact_equipment_telemetry', 'fact_equipment_downtime', 'fact_ore_quality')
  AND schemaname = 'public'
ORDER BY tablename, indexname;

SELECT
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE relname = 'fact_production'
  AND schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

SELECT
    relname AS table_name,
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) AS total_index_size,
    COUNT(*) AS index_count
FROM pg_stat_user_indexes
WHERE relname IN ('fact_production', 'fact_equipment_telemetry', 'fact_equipment_downtime', 'fact_ore_quality')
  AND schemaname = 'public'
GROUP BY relname
ORDER BY SUM(pg_relation_size(indexrelid)) DESC;

-- ====================================================================
-- ЗАДАНИЕ 2. АНАЛИЗ ПЛАНА ВЫПОЛНЕНИЯ
-- ====================================================================
EXPLAIN
SELECT e.equipment_name, SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

EXPLAIN ANALYZE
SELECT e.equipment_name, SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT e.equipment_name, SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

-- Ожидаемо: используется idx_fact_production_date.

-- ====================================================================
-- ЗАДАНИЕ 3. ОПТИМИЗАЦИЯ ПОИСКА ПО РАСХОДУ ТОПЛИВА
-- ====================================================================
EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name, o.last_name, p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC;

SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE fuel_consumed_l > 80) AS matching_rows,
    ROUND(COUNT(*) FILTER (WHERE fuel_consumed_l > 80)::numeric / COUNT(*) * 100, 2) AS selectivity_pct
FROM fact_production;

DROP INDEX IF EXISTS idx_prod_fuel;
CREATE INDEX idx_prod_fuel ON fact_production (fuel_consumed_l);

EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name, o.last_name, p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC;

SELECT p.date_id, e.equipment_name, o.last_name, p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC
LIMIT 15;

-- ====================================================================
-- ЗАДАНИЕ 4. ЧАСТИЧНЫЙ ИНДЕКС ДЛЯ АВАРИЙНОЙ ТЕЛЕМЕТРИИ
-- ====================================================================
EXPLAIN ANALYZE
SELECT t.telemetry_id, t.date_id, t.equipment_id, t.sensor_id, t.sensor_value
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE;

DROP INDEX IF EXISTS idx_telemetry_alarm_partial;
DROP INDEX IF EXISTS idx_telemetry_alarm_full;
CREATE INDEX idx_telemetry_alarm_partial ON fact_equipment_telemetry (date_id) WHERE is_alarm = TRUE;
CREATE INDEX idx_telemetry_alarm_full ON fact_equipment_telemetry (date_id, is_alarm);

SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname IN ('idx_telemetry_alarm_partial', 'idx_telemetry_alarm_full')
ORDER BY pg_relation_size(indexrelid);

EXPLAIN ANALYZE
SELECT t.telemetry_id, t.date_id, t.equipment_id, t.sensor_id, t.sensor_value
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE;

-- ====================================================================
-- ЗАДАНИЕ 5. КОМПОЗИТНЫЙ ИНДЕКС ДЛЯ ОТЧЕТА ПО ДОБЫЧЕ
-- ====================================================================
EXPLAIN ANALYZE
SELECT date_id, tons_mined, tons_transported, trips_count, operating_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240301 AND 20240331;

DROP INDEX IF EXISTS idx_prod_equip_date;
DROP INDEX IF EXISTS idx_prod_date_equip;
CREATE INDEX idx_prod_equip_date ON fact_production (equipment_id, date_id);
CREATE INDEX idx_prod_date_equip ON fact_production (date_id, equipment_id);

EXPLAIN ANALYZE
SELECT date_id, tons_mined, tons_transported, trips_count, operating_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240301 AND 20240331;

EXPLAIN ANALYZE
SELECT *
FROM fact_production
WHERE date_id = 20240315;

-- ====================================================================
-- ЗАДАНИЕ 6. ИНДЕКС ПО ВЫРАЖЕНИЮ ДЛЯ ПОИСКА ОПЕРАТОРОВ
-- ====================================================================
EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name, middle_name, position, qualification
FROM dim_operator
WHERE LOWER(last_name) = 'петров';

DROP INDEX IF EXISTS idx_operator_lower_lastname;
CREATE INDEX idx_operator_lower_lastname ON dim_operator (LOWER(last_name));

EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name, middle_name, position, qualification
FROM dim_operator
WHERE LOWER(last_name) = 'петров';

EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name
FROM dim_operator
WHERE last_name = 'Петров';

EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name
FROM dim_operator
WHERE UPPER(last_name) = 'ПЕТРОВ';

-- ====================================================================
-- ЗАДАНИЕ 7. ПОКРЫВАЮЩИЙ ИНДЕКС ДЛЯ ДАШБОРДА
-- ====================================================================
EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined
FROM fact_production
WHERE date_id = 20240315;

DROP INDEX IF EXISTS idx_prod_date_cover;
CREATE INDEX idx_prod_date_cover ON fact_production (date_id) INCLUDE (equipment_id, tons_mined);
ANALYZE fact_production;

EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined
FROM fact_production
WHERE date_id = 20240315;

EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined, fuel_consumed_l
FROM fact_production
WHERE date_id = 20240315;

DROP INDEX IF EXISTS idx_prod_date_cover_ext;
CREATE INDEX idx_prod_date_cover_ext ON fact_production (date_id) INCLUDE (equipment_id, tons_mined, fuel_consumed_l);
ANALYZE fact_production;

EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined, fuel_consumed_l
FROM fact_production
WHERE date_id = 20240315;

-- ====================================================================
-- ЗАДАНИЕ 8. BRIN-ИНДЕКС ДЛЯ ТЕЛЕМЕТРИИ
-- ====================================================================
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname = 'idx_fact_telemetry_date';

DROP INDEX IF EXISTS idx_telemetry_date_brin;
CREATE INDEX idx_telemetry_date_brin
ON fact_equipment_telemetry USING brin (date_id)
WITH (pages_per_range = 128);

SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname IN ('idx_fact_telemetry_date', 'idx_telemetry_date_brin')
ORDER BY pg_relation_size(indexrelid) DESC;

SET enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240301 AND 20240331;
RESET enable_bitmapscan;

SET enable_indexscan = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240301 AND 20240331;
RESET enable_indexscan;

SELECT attname, correlation
FROM pg_stats
WHERE tablename = 'fact_equipment_telemetry'
  AND attname = 'date_id';

-- ====================================================================
-- ЗАДАНИЕ 9. АНАЛИЗ ВЛИЯНИЯ ИНДЕКСОВ НА INSERT
-- ====================================================================
BEGIN;

SELECT COUNT(*) AS index_count
FROM pg_indexes
WHERE tablename = 'fact_production'
  AND schemaname = 'public';

EXPLAIN ANALYZE
INSERT INTO fact_production (
    date_id, shift_id, mine_id, shaft_id, equipment_id,
    operator_id, location_id, ore_grade_id,
    tons_mined, tons_transported, trips_count,
    distance_km, fuel_consumed_l, operating_hours
)
VALUES (20240401, 1, 1, 1, 1, 1, 1, 1, 120.50, 115.00, 8, 12.5, 45.2, 7.5);

DROP INDEX IF EXISTS idx_test_1;
DROP INDEX IF EXISTS idx_test_2;
DROP INDEX IF EXISTS idx_test_3;
CREATE INDEX idx_test_1 ON fact_production (tons_mined);
CREATE INDEX idx_test_2 ON fact_production (fuel_consumed_l, operating_hours);
CREATE INDEX idx_test_3 ON fact_production (date_id, shift_id, mine_id);

SELECT COUNT(*) AS index_count
FROM pg_indexes
WHERE tablename = 'fact_production'
  AND schemaname = 'public';

EXPLAIN ANALYZE
INSERT INTO fact_production (
    date_id, shift_id, mine_id, shaft_id, equipment_id,
    operator_id, location_id, ore_grade_id,
    tons_mined, tons_transported, trips_count,
    distance_km, fuel_consumed_l, operating_hours
)
VALUES (20240401, 1, 1, 1, 1, 1, 1, 1, 130.00, 125.00, 9, 14.0, 50.1, 8.0);

ROLLBACK;

-- ====================================================================
-- ЗАДАНИЕ 10. КОМПЛЕКСНАЯ ОПТИМИЗАЦИЯ: КЕЙС «РУДА+»
-- ====================================================================
SELECT m.mine_name, SUM(p.tons_mined) AS total_tons, SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY m.mine_name;

DROP INDEX IF EXISTS idx_prod_date_mine;
CREATE INDEX idx_prod_date_mine ON fact_production (date_id, mine_id);

SELECT g.grade_name, ROUND(AVG(q.fe_content), 2) AS avg_fe,
       ROUND(AVG(q.sio2_content), 2) AS avg_sio2, COUNT(*) AS samples
FROM fact_ore_quality q
JOIN dim_ore_grade g ON q.ore_grade_id = g.ore_grade_id
WHERE q.date_id BETWEEN 20240101 AND 20240331
GROUP BY g.grade_name;

DROP INDEX IF EXISTS idx_quality_date;
CREATE INDEX idx_quality_date ON fact_ore_quality (date_id);

SELECT e.equipment_name, SUM(dt.duration_min) AS total_downtime_min, COUNT(*) AS incidents
FROM fact_equipment_downtime dt
JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
WHERE dt.is_planned = FALSE
  AND dt.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_downtime_min DESC
LIMIT 5;

DROP INDEX IF EXISTS idx_downtime_unplanned;
CREATE INDEX idx_downtime_unplanned
ON fact_equipment_downtime (date_id, equipment_id)
WHERE is_planned = FALSE;

SELECT t.date_id, t.time_id, t.sensor_id, t.sensor_value, t.quality_flag
FROM fact_equipment_telemetry t
WHERE t.equipment_id = 7
  AND t.is_alarm = TRUE
ORDER BY t.date_id DESC, t.time_id DESC
LIMIT 20;

DROP INDEX IF EXISTS idx_telemetry_equip_alarm;
CREATE INDEX idx_telemetry_equip_alarm
ON fact_equipment_telemetry (equipment_id, date_id DESC, time_id DESC)
WHERE is_alarm = TRUE;

SELECT p.date_id, e.equipment_name, p.tons_mined, p.trips_count, p.operating_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.operator_id = 3
  AND p.date_id BETWEEN 20240311 AND 20240317
ORDER BY p.date_id;

DROP INDEX IF EXISTS idx_prod_operator_date;
CREATE INDEX idx_prod_operator_date ON fact_production (operator_id, date_id);

-- ====================================================================
-- ОЧИСТКА ПОСЛЕ ЛАБОРАТОРНОЙ
-- ====================================================================
DROP INDEX IF EXISTS idx_prod_fuel;
DROP INDEX IF EXISTS idx_telemetry_alarm_partial;
DROP INDEX IF EXISTS idx_telemetry_alarm_full;
DROP INDEX IF EXISTS idx_prod_equip_date;
DROP INDEX IF EXISTS idx_prod_date_equip;
DROP INDEX IF EXISTS idx_operator_lower_lastname;
DROP INDEX IF EXISTS idx_prod_date_cover;
DROP INDEX IF EXISTS idx_prod_date_cover_ext;
DROP INDEX IF EXISTS idx_telemetry_date_brin;
DROP INDEX IF EXISTS idx_test_1;
DROP INDEX IF EXISTS idx_test_2;
DROP INDEX IF EXISTS idx_test_3;
DROP INDEX IF EXISTS idx_prod_date_mine;
DROP INDEX IF EXISTS idx_quality_date;
DROP INDEX IF EXISTS idx_downtime_unplanned;
DROP INDEX IF EXISTS idx_telemetry_equip_alarm;
DROP INDEX IF EXISTS idx_prod_operator_date;
