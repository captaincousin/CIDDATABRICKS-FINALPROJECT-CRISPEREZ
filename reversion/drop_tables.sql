%sql
-- Drop_Tables.sql
-- Script de reversión para eliminar todas las tablas y schemas creados
-- por el proyecto ETL Medallion.

-----------------------------------------------------------
-- 1) GOLDEN TABLES
-----------------------------------------------------------
DROP TABLE IF EXISTS catalog_dev.golden.fact_student_performance;
DROP TABLE IF EXISTS catalog_dev.golden.agg_exam_by_gender;
DROP TABLE IF EXISTS catalog_dev.golden.agg_exam_by_hours_bucket;
DROP TABLE IF EXISTS catalog_dev.golden.agg_exam_by_attendance_bucket;
DROP TABLE IF EXISTS catalog_dev.golden.agg_exam_by_motivation;
DROP TABLE IF EXISTS catalog_dev.golden.agg_exam_school_vs_income;
DROP TABLE IF EXISTS catalog_dev.golden.corr_with_exam;

-----------------------------------------------------------
-- 2) SILVER TABLES
-----------------------------------------------------------
DROP TABLE IF EXISTS catalog_dev.silver.student_performance_silver;

-----------------------------------------------------------
-- 3) BRONZE TABLES
-----------------------------------------------------------
DROP TABLE IF EXISTS catalog_dev.bronze.student_performance_bronze;

-----------------------------------------------------------
-- 4) DROP SCHEMAS (bronze, silver, golden)
--    IF EMPTY ONLY — por eso uso "CASCADE" para simplificar.
-----------------------------------------------------------
DROP SCHEMA IF EXISTS catalog_dev.golden CASCADE;
DROP SCHEMA IF EXISTS catalog_dev.silver CASCADE;
DROP SCHEMA IF EXISTS catalog_dev.bronze CASCADE;

-----------------------------------------------------------
-- 5) DROP CATALOG (opcional)
-----------------------------------------------------------
--DROP CATALOG IF EXISTS catalog_dev CASCADE;

-----------------------------------------------------------
-- 6) DROP EXTERNAL LOCATIONS (opcional)
--   Solo si deseas dejar completamente limpio ADLS
-----------------------------------------------------------
-- DROP EXTERNAL LOCATION IF EXISTS `exlt-bronze`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt-silver`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt-golden`;

-- FIN DEL SCRIPT
