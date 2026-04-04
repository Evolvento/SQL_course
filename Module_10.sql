-- ====================================================================
-- MODULE 10
-- Использование подзапросов
-- Лабораторные решения (SQL)
-- ====================================================================
SET search_path TO "Zelent", public;

-- ====================================================================
-- ЗАДАНИЕ 1. ОПЕРАТОРЫ С ДОБЫЧЕЙ ВЫШЕ СРЕДНЕЙ
-- ====================================================================
SELECT o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name,
       SUM(p.tons_mined) AS total_mined,
       (
           SELECT AVG(sub.total_tons)
           FROM (
               SELECT SUM(tons_mined) AS total_tons
               FROM fact_production
               WHERE date_id BETWEEN 20240301 AND 20240331
               GROUP BY operator_id
           ) sub
       ) AS avg_production
FROM fact_production p
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY o.operator_id, o.last_name, o.first_name
HAVING SUM(p.tons_mined) > (
    SELECT AVG(sub.total_tons)
    FROM (
        SELECT SUM(tons_mined) AS total_tons
        FROM fact_production
        WHERE date_id BETWEEN 20240301 AND 20240331
        GROUP BY operator_id
    ) sub
)
ORDER BY total_mined DESC;

-- ====================================================================
-- ЗАДАНИЕ 2. ДАТЧИКИ НА ОБОРУДОВАНИИ, УЧАСТВОВАВШЕМ В ДОБЫЧЕ
-- ====================================================================
SELECT s.sensor_code,
       st.type_name AS sensor_type,
       e.equipment_name,
       s.status
FROM dim_sensor s
JOIN dim_sensor_type st ON s.sensor_type_id = st.sensor_type_id
JOIN dim_equipment e ON s.equipment_id = e.equipment_id
WHERE s.equipment_id IN (
    SELECT DISTINCT equipment_id
    FROM fact_production
    WHERE date_id BETWEEN 20240101 AND 20240331
)
ORDER BY e.equipment_name, s.sensor_code;

-- ====================================================================
-- ЗАДАНИЕ 3. ОБОРУДОВАНИЕ БЕЗ ЗАПИСЕЙ О ДОБЫЧЕ (NOT IN)
-- ====================================================================
SELECT e.equipment_name,
       et.type_name,
       m.mine_name,
       e.status
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE e.equipment_id NOT IN (
    SELECT equipment_id
    FROM fact_production
    WHERE equipment_id IS NOT NULL
)
ORDER BY e.equipment_name;

-- Вариант без ловушки NULL через NOT EXISTS.
SELECT e.equipment_name,
       et.type_name,
       m.mine_name,
       e.status
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_production fp
    WHERE fp.equipment_id = e.equipment_id
)
ORDER BY e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 4. СМЕНЫ С ДОБЫЧЕЙ НИЖЕ СРЕДНЕЙ ПО ШАХТЕ
-- ====================================================================
SELECT m.mine_name,
       d.full_date,
       e.equipment_name,
       fp.tons_mined,
       ROUND((
           SELECT AVG(fp2.tons_mined)
           FROM fact_production fp2
           WHERE fp2.mine_id = fp.mine_id
             AND fp2.date_id BETWEEN 20240101 AND 20240331
       )::numeric, 2) AS mine_avg
FROM fact_production fp
JOIN dim_mine m ON fp.mine_id = m.mine_id
JOIN dim_date d ON fp.date_id = d.date_id
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
WHERE fp.date_id BETWEEN 20240101 AND 20240331
  AND fp.tons_mined < (
      SELECT AVG(fp2.tons_mined)
      FROM fact_production fp2
      WHERE fp2.mine_id = fp.mine_id
        AND fp2.date_id BETWEEN 20240101 AND 20240331
  )
ORDER BY (
    fp.tons_mined - (
        SELECT AVG(fp2.tons_mined)
        FROM fact_production fp2
        WHERE fp2.mine_id = fp.mine_id
          AND fp2.date_id BETWEEN 20240101 AND 20240331
    )
) ASC
LIMIT 15;

-- ====================================================================
-- ЗАДАНИЕ 5. ОБОРУДОВАНИЕ С ТРЕВОЖНЫМИ ПОКАЗАНИЯМИ (EXISTS)
-- ====================================================================
SELECT e.equipment_name,
       et.type_name,
       m.mine_name,
       (
           SELECT COUNT(*)
           FROM fact_equipment_telemetry t
           WHERE t.equipment_id = e.equipment_id
             AND t.is_alarm = TRUE
             AND t.date_id BETWEEN 20240301 AND 20240331
       ) AS alarm_count
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE EXISTS (
    SELECT 1
    FROM fact_equipment_telemetry t
    WHERE t.equipment_id = e.equipment_id
      AND t.is_alarm = TRUE
      AND t.date_id BETWEEN 20240301 AND 20240331
)
ORDER BY alarm_count DESC, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 6. ДАТЫ БЕЗ ДОБЫЧИ ДЛЯ equipment_id = 1
-- ====================================================================
SELECT d.full_date,
       d.day_of_week_name,
       d.is_weekend
FROM dim_date d
WHERE d.date_id BETWEEN 20240301 AND 20240331
  AND NOT EXISTS (
      SELECT 1
      FROM fact_production p
      WHERE p.equipment_id = 1
        AND p.date_id = d.date_id
  )
ORDER BY d.full_date;

-- ====================================================================
-- ЗАДАНИЕ 7. ДОБЫЧА > ALL САМОСВАЛОВ
-- ====================================================================
SELECT e.equipment_name,
       et.type_name,
       fp.date_id,
       fp.shift_id,
       fp.tons_mined
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
WHERE fp.tons_mined > ALL (
    SELECT fp2.tons_mined
    FROM fact_production fp2
    JOIN dim_equipment e2 ON fp2.equipment_id = e2.equipment_id
    JOIN dim_equipment_type et2 ON e2.equipment_type_id = et2.equipment_type_id
    WHERE et2.type_code = 'TRUCK'
)
ORDER BY fp.tons_mined DESC;

SELECT e.equipment_name,
       et.type_name,
       fp.date_id,
       fp.shift_id,
       fp.tons_mined
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
WHERE fp.tons_mined > (
    SELECT MAX(fp2.tons_mined)
    FROM fact_production fp2
    JOIN dim_equipment e2 ON fp2.equipment_id = e2.equipment_id
    JOIN dim_equipment_type et2 ON e2.equipment_type_id = et2.equipment_type_id
    WHERE et2.type_code = 'TRUCK'
)
ORDER BY fp.tons_mined DESC;

SELECT COUNT(*) AS rows_gt_any
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
WHERE et.type_code = 'LHD'
  AND fp.tons_mined > ANY (
      SELECT fp2.tons_mined
      FROM fact_production fp2
      JOIN dim_equipment e2 ON fp2.equipment_id = e2.equipment_id
      JOIN dim_equipment_type et2 ON e2.equipment_type_id = et2.equipment_type_id
      WHERE et2.type_code = 'TRUCK'
  );

-- ====================================================================
-- ЗАДАНИЕ 8. ПОСЛЕДНЯЯ ЗАПИСЬ ДОБЫЧИ ДЛЯ КАЖДОГО ОБОРУДОВАНИЯ
-- ====================================================================
-- Исправление относительно lab_results.md: добавлена корреляция по максимальной
-- смене на последнюю дату, чтобы получить именно последнюю запись.
SELECT e.equipment_name,
       et.type_name,
       d.full_date,
       fp.tons_mined,
       o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d ON fp.date_id = d.date_id
JOIN dim_operator o ON fp.operator_id = o.operator_id
WHERE fp.date_id = (
    SELECT MAX(fp2.date_id)
    FROM fact_production fp2
    WHERE fp2.equipment_id = fp.equipment_id
)
  AND fp.shift_id = (
      SELECT MAX(fp3.shift_id)
      FROM fact_production fp3
      WHERE fp3.equipment_id = fp.equipment_id
        AND fp3.date_id = fp.date_id
  )
ORDER BY d.full_date ASC, e.equipment_name;

-- ====================================================================
-- ЗАДАНИЕ 9. СРЕДНЕЕ ВРЕМЯ ПРОСТОЕВ ОБОРУДОВАНИЯ-ПЕРЕДОВИКОВ
-- ====================================================================
SELECT m.mine_name,
       COUNT(DISTINCT fd.equipment_id) AS top_equipment_count,
       ROUND(AVG(fd.duration_min)::numeric, 1) AS avg_downtime_min,
       ROUND(SUM(fd.duration_min)::numeric / 60, 1) AS total_downtime_hours
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE fd.is_planned = FALSE
  AND fd.equipment_id IN (
      SELECT fp.equipment_id
      FROM fact_production fp
      WHERE fp.date_id BETWEEN 20240101 AND 20240331
      GROUP BY fp.equipment_id
      HAVING SUM(fp.tons_mined) > (
          SELECT AVG(total_tons)
          FROM (
              SELECT SUM(tons_mined) AS total_tons
              FROM fact_production
              WHERE date_id BETWEEN 20240101 AND 20240331
              GROUP BY equipment_id
          ) sub
      )
  )
GROUP BY m.mine_name
ORDER BY total_downtime_hours DESC;

-- ====================================================================
-- ЗАДАНИЕ 10. OEE ПО ОБОРУДОВАНИЮ (Q1 2024)
-- ====================================================================
-- Исправление относительно lab_results.md: в SQL-схеме fact_ore_quality не
-- содержит equipment_id, поэтому quality_pct считается по mine_id оборудования.
SELECT x.equipment_name,
       x.type_name,
       x.availability_pct,
       x.performance_pct,
       x.quality_pct,
       ROUND(x.availability_pct * x.performance_pct * x.quality_pct / 10000.0, 1) AS oee_pct
FROM (
    SELECT e.equipment_name,
           et.type_name,
           ROUND(
               COALESCE(
                   (
                       SELECT SUM(fp.operating_hours)
                       FROM fact_production fp
                       WHERE fp.equipment_id = e.equipment_id
                         AND fp.date_id BETWEEN 20240101 AND 20240331
                   )
                   /
                   NULLIF(
                       (
                           SELECT SUM(fp.operating_hours)
                           FROM fact_production fp
                           WHERE fp.equipment_id = e.equipment_id
                             AND fp.date_id BETWEEN 20240101 AND 20240331
                       )
                       +
                       (
                           SELECT COALESCE(SUM(fd.duration_min) / 60.0, 0)
                           FROM fact_equipment_downtime fd
                           WHERE fd.equipment_id = e.equipment_id
                             AND fd.date_id BETWEEN 20240101 AND 20240331
                       ),
                       0
                   ) * 100,
                   0
               )::numeric,
               1
           ) AS availability_pct,
           ROUND(
               COALESCE(
                   (
                       SELECT SUM(fp.tons_mined)
                       FROM fact_production fp
                       WHERE fp.equipment_id = e.equipment_id
                         AND fp.date_id BETWEEN 20240101 AND 20240331
                   )
                   /
                   NULLIF(
                       (
                           SELECT SUM(fp.operating_hours)
                           FROM fact_production fp
                           WHERE fp.equipment_id = e.equipment_id
                             AND fp.date_id BETWEEN 20240101 AND 20240331
                       ) * et.max_payload_tons,
                       0
                   ) * 100,
                   0
               )::numeric,
               1
           ) AS performance_pct,
           ROUND(
               COALESCE(
                   (
                       SELECT COUNT(*) FILTER (WHERE q.fe_content >= 55)::numeric /
                              NULLIF(COUNT(*)::numeric, 0)
                       FROM fact_ore_quality q
                       WHERE q.mine_id = e.mine_id
                         AND q.date_id BETWEEN 20240101 AND 20240331
                   ) * 100,
                   0
               )::numeric,
               1
           ) AS quality_pct
    FROM dim_equipment e
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE e.status = 'active'
) x
ORDER BY oee_pct DESC, x.equipment_name;
