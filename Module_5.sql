-- ====================================================================
-- MODULE 5
-- Использование DML для изменения данных
-- Лабораторные решения (SQL)
-- ====================================================================
-- Если вы работаете в своей схеме, при необходимости раскомментируйте:
SET search_path TO Zelent, public;
--
-- ВАЖНО:
-- 1. Скрипт рассчитан на предварительно созданные practice_* таблицы:
--    module_05/scripts/create_practice_tables.sql
-- 2. По комментарию преподавателя задание 10 можно не делать.

-- ====================================================================
-- ЗАДАНИЕ 1. ДОБАВЛЕНИЕ НОВОГО ОБОРУДОВАНИЯ
-- ====================================================================
SELECT *
FROM practice_dim_equipment
WHERE equipment_id = 200
   OR inventory_number = 'INV-TRK-200';

INSERT INTO practice_dim_equipment (
    equipment_id,
    equipment_type_id,
    mine_id,
    equipment_name,
    inventory_number,
    manufacturer,
    model,
    year_manufactured,
    commissioning_date,
    status,
    has_video_recorder,
    has_navigation
)
VALUES (
    200,
    2,
    2,
    'Самосвал МоАЗ-7529',
    'INV-TRK-200',
    'МоАЗ',
    '7529',
    2025,
    DATE '2025-03-15',
    'active',
    TRUE,
    TRUE
);

SELECT
    equipment_id,
    equipment_name,
    inventory_number,
    status
FROM practice_dim_equipment
WHERE equipment_id = 200;

-- ====================================================================
-- ЗАДАНИЕ 2. МАССОВАЯ ВСТАВКА ОПЕРАТОРОВ
-- ====================================================================
SELECT *
FROM practice_dim_operator
WHERE operator_id >= 200
ORDER BY operator_id;

INSERT INTO practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
)
VALUES
    (200, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Машинист ПДМ',  '4 разряд', DATE '2025-03-01', 1),
    (201, 'TAB-201', 'Петрова', 'Елена',  'Сергеевна', 'Оператор скипа', '3 разряд', DATE '2025-03-01', 2),
    (202, 'TAB-202', 'Волков',  'Дмитрий','Алексеевич','Водитель самосвала', '5 разряд', DATE '2025-03-10', 2);

SELECT
    operator_id,
    tab_number,
    last_name,
    first_name,
    qualification
FROM practice_dim_operator
WHERE operator_id >= 200
ORDER BY operator_id;

-- ====================================================================
-- ЗАДАНИЕ 3. ЗАГРУЗКА ИЗ STAGING (INSERT ... SELECT)
-- ====================================================================
SELECT COUNT(*) AS before_insert_count
FROM practice_fact_production;

INSERT INTO practice_fact_production (
    production_id,
    date_id,
    shift_id,
    mine_id,
    shaft_id,
    equipment_id,
    operator_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    fuel_consumed_l,
    operating_hours
)
SELECT
    3000 + sp.staging_id,
    sp.date_id,
    sp.shift_id,
    sp.mine_id,
    sp.shaft_id,
    sp.equipment_id,
    sp.operator_id,
    sp.location_id,
    sp.ore_grade_id,
    sp.tons_mined,
    sp.tons_transported,
    sp.trips_count,
    sp.distance_km,
    sp.fuel_consumed_l,
    sp.operating_hours
FROM staging_production sp
WHERE sp.is_validated = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM practice_fact_production p
      WHERE p.date_id = sp.date_id
        AND p.shift_id = sp.shift_id
        AND p.equipment_id = sp.equipment_id
        AND p.operator_id = sp.operator_id
  );

SELECT COUNT(*) AS after_insert_count
FROM practice_fact_production;

SELECT
    production_id,
    date_id,
    shift_id,
    equipment_id,
    operator_id,
    tons_mined
FROM practice_fact_production
WHERE production_id >= 3000
ORDER BY production_id;

-- ====================================================================
-- ЗАДАНИЕ 4. INSERT ... RETURNING С ЛОГИРОВАНИЕМ
-- ====================================================================
WITH inserted_grade AS (
    INSERT INTO practice_dim_ore_grade (
        ore_grade_id,
        grade_name,
        grade_code,
        fe_content_min,
        fe_content_max,
        description
    )
    VALUES (
        300,
        'Экспортный',
        'EXPORT',
        63.00,
        68.00,
        'Руда для экспортных поставок'
    )
    ON CONFLICT (grade_code) DO NOTHING
    RETURNING ore_grade_id, grade_name, grade_code
)
INSERT INTO practice_equipment_log (
    equipment_id,
    action,
    details
)
SELECT
    0,
    'INSERT',
    'Добавлен сорт руды: ' || grade_name || ' (' || grade_code || ')'
FROM inserted_grade;

SELECT *
FROM practice_dim_ore_grade
WHERE grade_code = 'EXPORT';

SELECT
    log_id,
    equipment_id,
    action,
    details
FROM practice_equipment_log
WHERE action = 'INSERT'
  AND details LIKE 'Добавлен сорт руды:%'
ORDER BY log_id DESC;

-- ====================================================================
-- ЗАДАНИЕ 5. ОБНОВЛЕНИЕ СТАТУСА ОБОРУДОВАНИЯ
-- ====================================================================
UPDATE practice_dim_equipment
SET status = 'maintenance'
WHERE mine_id = 1
  AND year_manufactured <= 2018
RETURNING equipment_id, equipment_name, year_manufactured, status;

SELECT
    equipment_id,
    equipment_name,
    year_manufactured,
    status
FROM practice_dim_equipment
WHERE status = 'maintenance'
ORDER BY year_manufactured, equipment_id;

-- ====================================================================
-- ЗАДАНИЕ 6. UPDATE С ПОДЗАПРОСОМ
-- ====================================================================
UPDATE practice_dim_equipment e
SET has_navigation = TRUE
WHERE e.has_navigation = FALSE
  AND e.equipment_id IN (
      SELECT DISTINCT s.equipment_id
      FROM dim_sensor s
      JOIN dim_sensor_type st
          ON st.sensor_type_id = s.sensor_type_id
      WHERE s.status = 'active'
        AND st.type_code LIKE 'NAV_%'
  )
RETURNING e.equipment_id, e.equipment_name, e.has_navigation;

SELECT
    equipment_id,
    equipment_name,
    has_navigation
FROM practice_dim_equipment
WHERE has_navigation = TRUE
ORDER BY equipment_id;

-- ====================================================================
-- ЗАДАНИЕ 7. DELETE С УСЛОВИЕМ И АРХИВИРОВАНИЕМ
-- ====================================================================
WITH deleted_rows AS (
    DELETE FROM practice_fact_telemetry
    WHERE date_id = 20240315
      AND is_alarm = TRUE
    RETURNING
        telemetry_id,
        date_id,
        time_id,
        equipment_id,
        sensor_id,
        location_id,
        sensor_value,
        is_alarm,
        quality_flag,
        loaded_at
)
INSERT INTO practice_archive_telemetry (
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at
)
SELECT
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at
FROM deleted_rows;

SELECT COUNT(*) AS remaining_alarm_rows
FROM practice_fact_telemetry
WHERE date_id = 20240315
  AND is_alarm = TRUE;

SELECT
    telemetry_id,
    equipment_id,
    sensor_id,
    quality_flag,
    archived_at
FROM practice_archive_telemetry
ORDER BY archived_at DESC, telemetry_id DESC;

-- ====================================================================
-- ЗАДАНИЕ 8. MERGE - СИНХРОНИЗАЦИЯ СПРАВОЧНИКА
-- ====================================================================
SELECT reason_id, reason_name, reason_code, category
FROM practice_dim_downtime_reason
ORDER BY reason_id;

SELECT reason_name, reason_code, category
FROM staging_downtime_reasons
ORDER BY reason_code;

WITH current_max AS (
    SELECT COALESCE(MAX(reason_id), 0) AS max_reason_id
    FROM practice_dim_downtime_reason
),
source_data AS (
    SELECT
        s.reason_name,
        s.reason_code,
        s.category,
        s.description,
        CASE
            WHEN t.reason_id IS NULL THEN
                (SELECT max_reason_id FROM current_max)
                + ROW_NUMBER() OVER (ORDER BY s.reason_code)
        END AS new_reason_id
    FROM staging_downtime_reasons s
    LEFT JOIN practice_dim_downtime_reason t
        ON t.reason_code = s.reason_code
)
MERGE INTO practice_dim_downtime_reason AS target
USING source_data AS source
    ON target.reason_code = source.reason_code
WHEN MATCHED THEN
    UPDATE SET
        reason_name = source.reason_name,
        category = source.category,
        description = source.description
WHEN NOT MATCHED THEN
    INSERT (reason_id, reason_name, reason_code, category, description)
    VALUES (
        source.new_reason_id,
        source.reason_name,
        source.reason_code,
        source.category,
        source.description
    );

SELECT reason_id, reason_name, reason_code, category
FROM practice_dim_downtime_reason
ORDER BY reason_id;

-- ====================================================================
-- ЗАДАНИЕ 9. UPSERT - ИДЕМПОТЕНТНАЯ ЗАГРУЗКА
-- ====================================================================
INSERT INTO practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
)
VALUES
    (200, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Старший машинист ПДМ', '5 разряд', DATE '2025-03-01', 1),
    (201, 'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Старший оператор скипа', '4 разряд', DATE '2025-03-01', 2),
    (203, 'TAB-NEW', 'Козырев', 'Олег', 'Павлович', 'Машинист ПДМ', '4 разряд', DATE '2025-03-20', 1)
ON CONFLICT (tab_number) DO UPDATE SET
    position = EXCLUDED.position,
    qualification = EXCLUDED.qualification;

SELECT
    operator_id,
    tab_number,
    last_name,
    first_name,
    position,
    qualification
FROM practice_dim_operator
WHERE tab_number IN ('TAB-200', 'TAB-201', 'TAB-NEW')
ORDER BY tab_number;

-- ====================================================================
-- ЗАДАНИЕ 10. ПРОПУЩЕНО
-- ====================================================================
-- По комментарию преподавателя комплексный ETL-процесс можно не делать.
