-- ====================================================================
-- MODULE 8
-- Проектирование стратегий оптимизированных индексов
-- Лабораторные решения (SQL)
-- ====================================================================
SET search_path TO "Zelent", public;

-- ====================================================================
-- ЗАДАНИЕ 1. АНАЛИЗ СЕЛЕКТИВНОСТИ
-- ====================================================================
ANALYZE fact_production;

SELECT
    attname AS column_name,
    n_distinct,
    correlation,
    null_frac,
    most_common_vals::text
FROM pg_stats
WHERE tablename = 'fact_production'
  AND schemaname = 'public'
ORDER BY attname;

-- ====================================================================
-- ЗАДАНИЕ 2. КОЭФФИЦИЕНТ ЗАПОЛНЕНИЯ fillfactor
-- ====================================================================
DROP INDEX IF EXISTS idx_prod_date_ff100;
DROP INDEX IF EXISTS idx_prod_date_ff90;
DROP INDEX IF EXISTS idx_prod_date_ff70;
DROP INDEX IF EXISTS idx_prod_date_ff50;

CREATE INDEX idx_prod_date_ff100 ON fact_production (date_id) WITH (fillfactor = 100);
CREATE INDEX idx_prod_date_ff90  ON fact_production (date_id) WITH (fillfactor = 90);
CREATE INDEX idx_prod_date_ff70  ON fact_production (date_id) WITH (fillfactor = 70);
CREATE INDEX idx_prod_date_ff50  ON fact_production (date_id) WITH (fillfactor = 50);

SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
    pg_relation_size(indexname::regclass) AS size_bytes
FROM pg_indexes
WHERE indexname LIKE 'idx_prod_date_ff%'
ORDER BY size_bytes;

DROP INDEX IF EXISTS idx_prod_date_ff100;
DROP INDEX IF EXISTS idx_prod_date_ff90;
DROP INDEX IF EXISTS idx_prod_date_ff70;
DROP INDEX IF EXISTS idx_prod_date_ff50;

-- ====================================================================
-- ЗАДАНИЕ 3. УПРАВЛЕНИЕ СТАТИСТИКОЙ
-- ====================================================================
SELECT attname, attstattarget
FROM pg_attribute
WHERE attrelid = 'fact_production'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;

EXPLAIN ANALYZE
SELECT *
FROM fact_production
WHERE mine_id = 1
  AND shaft_id = 1
  AND date_id BETWEEN 20240101 AND 20240131;

ALTER TABLE fact_production ALTER COLUMN mine_id SET STATISTICS 1000;
ALTER TABLE fact_production ALTER COLUMN shaft_id SET STATISTICS 1000;
ALTER TABLE fact_production ALTER COLUMN date_id SET STATISTICS 1000;
ANALYZE fact_production;

DROP STATISTICS IF EXISTS stat_prod_mine_shaft;
CREATE STATISTICS stat_prod_mine_shaft (dependencies, ndistinct)
ON mine_id, shaft_id FROM fact_production;
ANALYZE fact_production;

EXPLAIN ANALYZE
SELECT *
FROM fact_production
WHERE mine_id = 1
  AND shaft_id = 1
  AND date_id BETWEEN 20240101 AND 20240131;

SELECT
    schemaname,
    statistics_name,
    attnames,
    kinds,
    n_distinct,
    dependencies
FROM pg_stats_ext
WHERE statistics_name = 'stat_prod_mine_shaft';

-- ====================================================================
-- ЗАДАНИЕ 4. ДУБЛИРУЮЩИЕСЯ ИНДЕКСЫ
-- ====================================================================
DROP INDEX IF EXISTS idx_prod_equip_date_v1;
DROP INDEX IF EXISTS idx_prod_equip_date_v2;
DROP INDEX IF EXISTS idx_prod_equip_only;

CREATE INDEX idx_prod_equip_date_v1 ON fact_production (equipment_id, date_id);
CREATE INDEX idx_prod_equip_date_v2 ON fact_production (equipment_id, date_id);
CREATE INDEX idx_prod_equip_only ON fact_production (equipment_id);

SELECT
    a.indexrelid::regclass AS index_1,
    b.indexrelid::regclass AS index_2,
    a.indrelid::regclass AS table_name,
    pg_size_pretty(pg_relation_size(a.indexrelid)) AS index_size
FROM pg_index a
JOIN pg_index b
    ON a.indrelid = b.indrelid
   AND a.indexrelid < b.indexrelid
   AND a.indkey::text = b.indkey::text
WHERE a.indrelid::regclass::text NOT LIKE 'pg_%';

SELECT
    a.indexrelid::regclass AS shorter_index,
    b.indexrelid::regclass AS longer_index,
    a.indrelid::regclass AS table_name,
    pg_size_pretty(pg_relation_size(a.indexrelid)) AS shorter_size,
    pg_size_pretty(pg_relation_size(b.indexrelid)) AS longer_size
FROM pg_index a
JOIN pg_index b
    ON a.indrelid = b.indrelid
   AND a.indexrelid <> b.indexrelid
   AND a.indnkeyatts < b.indnkeyatts
   AND a.indkey::text = (
       SELECT string_agg(x, ' ')
       FROM unnest(string_to_array(b.indkey::text, ' ')) WITH ORDINALITY AS t(x, ord)
       WHERE ord <= a.indnkeyatts
   )
WHERE a.indrelid::regclass::text NOT LIKE 'pg_%';

SELECT pg_size_pretty(SUM(pg_relation_size(b.indexrelid))) AS wasted_space
FROM pg_index a
JOIN pg_index b
    ON a.indrelid = b.indrelid
   AND a.indexrelid < b.indexrelid
   AND a.indkey::text = b.indkey::text
WHERE a.indrelid::regclass::text NOT LIKE 'pg_%';

DROP INDEX IF EXISTS idx_prod_equip_date_v1;
DROP INDEX IF EXISTS idx_prod_equip_date_v2;
DROP INDEX IF EXISTS idx_prod_equip_only;

-- ====================================================================
-- ЗАДАНИЕ 5. МОНИТОРИНГ НЕИСПОЛЬЗУЕМЫХ ИНДЕКСОВ
-- ====================================================================
SELECT
    schemaname || '.' || relname AS table_name,
    indexrelname AS index_name,
    idx_scan,
    idx_tup_read,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    pg_relation_size(indexrelid) AS size_bytes
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

SELECT
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) AS total_wasted_space,
    COUNT(*) AS unused_index_count
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname = 'public';

SELECT
    sui.relname AS table_name,
    sui.indexrelname AS index_name,
    sui.idx_scan,
    pg_size_pretty(pg_relation_size(sui.indexrelid)) AS index_size,
    i.indisunique,
    i.indisprimary
FROM pg_stat_user_indexes sui
JOIN pg_index i ON sui.indexrelid = i.indexrelid
WHERE sui.idx_scan = 0
  AND sui.schemaname = 'public'
  AND i.indisunique = false
  AND i.indisprimary = false
ORDER BY pg_relation_size(sui.indexrelid) DESC;

SELECT stats_reset FROM pg_stat_bgwriter;

-- ====================================================================
-- ЗАДАНИЕ 6. REINDEX И ОБСЛУЖИВАНИЕ
-- ====================================================================
DROP INDEX IF EXISTS idx_prod_bloat_test;
CREATE INDEX idx_prod_bloat_test ON fact_production (equipment_id, date_id);

SELECT pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS initial_size;

UPDATE fact_production SET equipment_id = equipment_id WHERE date_id BETWEEN 20240101 AND 20240115;
UPDATE fact_production SET equipment_id = equipment_id WHERE date_id BETWEEN 20240116 AND 20240131;

SELECT pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS bloated_size;

SELECT
    pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS current_size,
    '104 kB' AS expected_size,
    ROUND((pg_relation_size('idx_prod_bloat_test') - 106496)::numeric / 106496 * 100, 1) AS bloat_pct;

REINDEX INDEX idx_prod_bloat_test;
SELECT pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS size_after_reindex;

UPDATE fact_production SET equipment_id = equipment_id WHERE date_id BETWEEN 20240101 AND 20240115;
UPDATE fact_production SET equipment_id = equipment_id WHERE date_id BETWEEN 20240116 AND 20240131;
REINDEX INDEX idx_prod_bloat_test;

DROP INDEX IF EXISTS idx_prod_bloat_test;

-- ====================================================================
-- ЗАДАНИЕ 7. ПОКРЫВАЮЩИЙ ИНДЕКС ДЛЯ ОТЧЁТА
-- ====================================================================
DROP INDEX IF EXISTS idx_prod_equip_date_covering;

EXPLAIN (ANALYZE, BUFFERS)
SELECT date_id,
       SUM(tons_mined) AS total_tons,
       SUM(trips_count) AS total_trips,
       SUM(operating_hours) AS total_hours
FROM fact_production
WHERE equipment_id = 7
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id;

CREATE INDEX idx_prod_equip_date_covering
ON fact_production (equipment_id, date_id)
INCLUDE (tons_mined, trips_count, operating_hours);

ANALYZE fact_production;

EXPLAIN (ANALYZE, BUFFERS)
SELECT date_id,
       SUM(tons_mined) AS total_tons,
       SUM(trips_count) AS total_trips,
       SUM(operating_hours) AS total_hours
FROM fact_production
WHERE equipment_id = 7
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id;

SELECT date_id,
       SUM(tons_mined) AS total_tons,
       SUM(trips_count) AS total_trips,
       SUM(operating_hours) AS total_hours
FROM fact_production
WHERE equipment_id = 7
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id
LIMIT 15;

DROP INDEX IF EXISTS idx_prod_equip_date_covering;

-- ====================================================================
-- ЗАДАНИЕ 8. КОМПЛЕКСНАЯ ОПТИМИЗАЦИЯ ОТЧЁТА OEE
-- ====================================================================
DROP INDEX IF EXISTS idx_oee_prod;
DROP INDEX IF EXISTS idx_oee_downtime;
DROP INDEX IF EXISTS idx_equip_status;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH production_data AS (
    SELECT p.equipment_id,
           SUM(p.operating_hours) AS total_operating_hours,
           SUM(p.tons_mined) AS total_tons
    FROM fact_production p
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY p.equipment_id
),
downtime_data AS (
    SELECT fd.equipment_id,
           SUM(fd.duration_min) / 60.0 AS total_downtime_hours,
           SUM(CASE WHEN fd.is_planned = FALSE THEN fd.duration_min ELSE 0 END) / 60.0 AS unplanned_hours
    FROM fact_equipment_downtime fd
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
    GROUP BY fd.equipment_id
)
SELECT e.equipment_name,
       et.type_name,
       COALESCE(pd.total_operating_hours, 0) AS operating_hours,
       ROUND(COALESCE(dd.total_downtime_hours, 0)::numeric, 1) AS downtime_hours,
       ROUND(COALESCE(dd.unplanned_hours, 0)::numeric, 1) AS unplanned_downtime,
       COALESCE(pd.total_tons, 0) AS tons_mined,
       CASE
           WHEN COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0) > 0
               THEN ROUND(COALESCE(pd.total_operating_hours, 0) /
                          (COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0)) * 100, 1)
           ELSE 0
       END AS availability_pct
FROM dim_equipment e
JOIN dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
LEFT JOIN production_data pd ON pd.equipment_id = e.equipment_id
LEFT JOIN downtime_data dd ON dd.equipment_id = e.equipment_id
WHERE e.status = 'active'
ORDER BY availability_pct ASC;

CREATE INDEX idx_oee_prod ON fact_production (date_id)
INCLUDE (equipment_id, operating_hours, tons_mined);
CREATE INDEX idx_oee_downtime ON fact_equipment_downtime (date_id)
INCLUDE (equipment_id, duration_min, is_planned);
CREATE INDEX idx_equip_status ON dim_equipment (status)
INCLUDE (equipment_id, equipment_name, equipment_type_id);

ANALYZE fact_production;
ANALYZE fact_equipment_downtime;
ANALYZE dim_equipment;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH production_data AS (
    SELECT p.equipment_id,
           SUM(p.operating_hours) AS total_operating_hours,
           SUM(p.tons_mined) AS total_tons
    FROM fact_production p
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY p.equipment_id
),
downtime_data AS (
    SELECT fd.equipment_id,
           SUM(fd.duration_min) / 60.0 AS total_downtime_hours,
           SUM(CASE WHEN fd.is_planned = FALSE THEN fd.duration_min ELSE 0 END) / 60.0 AS unplanned_hours
    FROM fact_equipment_downtime fd
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
    GROUP BY fd.equipment_id
)
SELECT e.equipment_name,
       et.type_name,
       COALESCE(pd.total_operating_hours, 0) AS operating_hours,
       ROUND(COALESCE(dd.total_downtime_hours, 0)::numeric, 1) AS downtime_hours,
       ROUND(COALESCE(dd.unplanned_hours, 0)::numeric, 1) AS unplanned_downtime,
       COALESCE(pd.total_tons, 0) AS tons_mined,
       CASE
           WHEN COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0) > 0
               THEN ROUND(COALESCE(pd.total_operating_hours, 0) /
                          (COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0)) * 100, 1)
           ELSE 0
       END AS availability_pct
FROM dim_equipment e
JOIN dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
LEFT JOIN production_data pd ON pd.equipment_id = e.equipment_id
LEFT JOIN downtime_data dd ON dd.equipment_id = e.equipment_id
WHERE e.status = 'active'
ORDER BY availability_pct ASC;

DROP INDEX IF EXISTS idx_oee_prod;
DROP INDEX IF EXISTS idx_oee_downtime;
DROP INDEX IF EXISTS idx_equip_status;

-- ====================================================================
-- ЗАДАНИЕ 9. ОПТИМИЗАЦИЯ ПАКЕТА ЗАПРОСОВ
-- ====================================================================
DROP INDEX IF EXISTS idx_q1_prod_mine_date;
DROP INDEX IF EXISTS idx_q2_downtime_equip;
DROP INDEX IF EXISTS idx_q5_downtime_unplanned;
DROP INDEX IF EXISTS idx_q3_telemetry_alarm;
DROP INDEX IF EXISTS idx_q4_ore_mine_date;

EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, SUM(p.tons_mined) AS daily_tons
FROM fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240301 AND 20240331
GROUP BY p.date_id
ORDER BY p.date_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, fd.start_time, fd.duration_min, dr.reason_name
FROM fact_equipment_downtime fd
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.equipment_id = 3
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.date_id, fd.start_time;

EXPLAIN (ANALYZE, BUFFERS)
SELECT t.time_id, s.sensor_code, t.sensor_value
FROM fact_equipment_telemetry t
JOIN dim_sensor s ON s.sensor_id = t.sensor_id
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE
ORDER BY t.time_id;

-- Исправление: в SQL-схеме используется moisture, а не moisture_pct.
EXPLAIN (ANALYZE, BUFFERS)
SELECT oq.date_id, AVG(oq.fe_content) AS avg_fe, AVG(oq.moisture) AS avg_moisture
FROM fact_ore_quality oq
WHERE oq.mine_id = 2
  AND oq.date_id BETWEEN 20240301 AND 20240331
GROUP BY oq.date_id
ORDER BY oq.date_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, e.equipment_name, dr.reason_name, fd.duration_min
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON e.equipment_id = fd.equipment_id
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.is_planned = FALSE
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.duration_min DESC
LIMIT 10;

CREATE INDEX idx_q1_prod_mine_date ON fact_production (mine_id, date_id) INCLUDE (tons_mined);
CREATE INDEX idx_q2_downtime_equip ON fact_equipment_downtime (equipment_id, date_id)
INCLUDE (start_time, duration_min, reason_id);
CREATE INDEX idx_q5_downtime_unplanned ON fact_equipment_downtime (date_id)
WHERE is_planned = FALSE;
CREATE INDEX idx_q3_telemetry_alarm ON fact_equipment_telemetry (date_id, time_id)
INCLUDE (sensor_id, sensor_value)
WHERE is_alarm = TRUE;
CREATE INDEX idx_q4_ore_mine_date ON fact_ore_quality (mine_id, date_id)
INCLUDE (fe_content, moisture);

ANALYZE fact_production;
ANALYZE fact_equipment_downtime;
ANALYZE fact_equipment_telemetry;
ANALYZE fact_ore_quality;

EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, SUM(p.tons_mined) AS daily_tons
FROM fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240301 AND 20240331
GROUP BY p.date_id
ORDER BY p.date_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, fd.start_time, fd.duration_min, dr.reason_name
FROM fact_equipment_downtime fd
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.equipment_id = 3
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.date_id, fd.start_time;

EXPLAIN (ANALYZE, BUFFERS)
SELECT t.time_id, s.sensor_code, t.sensor_value
FROM fact_equipment_telemetry t
JOIN dim_sensor s ON s.sensor_id = t.sensor_id
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE
ORDER BY t.time_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT oq.date_id, AVG(oq.fe_content) AS avg_fe, AVG(oq.moisture) AS avg_moisture
FROM fact_ore_quality oq
WHERE oq.mine_id = 2
  AND oq.date_id BETWEEN 20240301 AND 20240331
GROUP BY oq.date_id
ORDER BY oq.date_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, e.equipment_name, dr.reason_name, fd.duration_min
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON e.equipment_id = fd.equipment_id
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.is_planned = FALSE
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.duration_min DESC
LIMIT 10;

DROP INDEX IF EXISTS idx_q1_prod_mine_date;
DROP INDEX IF EXISTS idx_q2_downtime_equip;
DROP INDEX IF EXISTS idx_q5_downtime_unplanned;
DROP INDEX IF EXISTS idx_q3_telemetry_alarm;
DROP INDEX IF EXISTS idx_q4_ore_mine_date;

-- ====================================================================
-- ЗАДАНИЕ 10. СТРАТЕГИЧЕСКИЙ АНАЛИЗ
-- ====================================================================
SELECT
    relname AS table_name,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS current_indexes_size,
    ROUND((pg_total_relation_size(relid) - pg_relation_size(relid))::numeric /
          NULLIF(pg_relation_size(relid), 0) * 100, 1) AS index_to_table_pct
FROM pg_catalog.pg_statio_user_tables
WHERE schemaname = 'public'
  AND relname LIKE 'fact_%'
ORDER BY pg_relation_size(relid) DESC;

-- Рекомендованные DDL из итогового анализа:
-- CREATE INDEX idx_strategy_prod_brin ON fact_production USING brin (date_id);
-- CREATE INDEX idx_strategy_prod_mine_date ON fact_production (mine_id, date_id) INCLUDE (tons_mined);
-- CREATE INDEX idx_strategy_prod_equip_date ON fact_production (equipment_id, date_id)
--     INCLUDE (tons_mined, operating_hours, trips_count);
-- CREATE INDEX idx_strategy_telemetry_brin ON fact_equipment_telemetry USING brin (date_id);
-- CREATE INDEX idx_strategy_telemetry_alarm ON fact_equipment_telemetry (date_id, time_id)
--     INCLUDE (sensor_id, sensor_value) WHERE is_alarm = TRUE;
-- CREATE INDEX idx_strategy_downtime_equip_date ON fact_equipment_downtime (equipment_id, date_id)
--     INCLUDE (start_time, duration_min, reason_id);
-- CREATE INDEX idx_strategy_downtime_unplanned ON fact_equipment_downtime (date_id)
--     WHERE is_planned = FALSE;
-- CREATE INDEX idx_strategy_quality_mine_date ON fact_ore_quality (mine_id, date_id)
--     INCLUDE (fe_content, moisture);

-- ====================================================================
-- ОЧИСТКА ПОСЛЕ ЛАБОРАТОРНОЙ
-- ====================================================================
DROP INDEX IF EXISTS idx_prod_date_ff100;
DROP INDEX IF EXISTS idx_prod_date_ff90;
DROP INDEX IF EXISTS idx_prod_date_ff70;
DROP INDEX IF EXISTS idx_prod_date_ff50;
DROP INDEX IF EXISTS idx_prod_equip_date_v1;
DROP INDEX IF EXISTS idx_prod_equip_date_v2;
DROP INDEX IF EXISTS idx_prod_equip_only;
DROP INDEX IF EXISTS idx_prod_bloat_test;
DROP INDEX IF EXISTS idx_prod_equip_date_covering;
DROP INDEX IF EXISTS idx_oee_prod;
DROP INDEX IF EXISTS idx_oee_downtime;
DROP INDEX IF EXISTS idx_equip_status;
DROP INDEX IF EXISTS idx_q1_prod_mine_date;
DROP INDEX IF EXISTS idx_q2_downtime_equip;
DROP INDEX IF EXISTS idx_q5_downtime_unplanned;
DROP INDEX IF EXISTS idx_q3_telemetry_alarm;
DROP INDEX IF EXISTS idx_q4_ore_mine_date;
DROP STATISTICS IF EXISTS stat_prod_mine_shaft;
ALTER TABLE fact_production ALTER COLUMN mine_id SET STATISTICS -1;
ALTER TABLE fact_production ALTER COLUMN shaft_id SET STATISTICS -1;
ALTER TABLE fact_production ALTER COLUMN date_id SET STATISTICS -1;
ANALYZE fact_production;
ANALYZE fact_equipment_downtime;
ANALYZE fact_equipment_telemetry;
ANALYZE fact_ore_quality;
