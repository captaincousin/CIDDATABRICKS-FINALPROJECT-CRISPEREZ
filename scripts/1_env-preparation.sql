-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storageName default "stcrisperezproject";

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_dev.silver;
CREATE SCHEMA IF NOT EXISTS catalog_dev.golden;
--CREATE SCHEMA IF NOT EXISTS catalog_dev.exploratory;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
URL 'abfss://bronze@stcrisperezproject.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `cbfaa23e-8ed3-4888-9eea-0f69279003a7-storage-credential-1762744537742`)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
URL 'abfss://silver@stcrisperezproject.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL  `cbfaa23e-8ed3-4888-9eea-0f69279003a7-storage-credential-1762744537742`)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
URL 'abfss://golden@stcrisperezproject.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL  `cbfaa23e-8ed3-4888-9eea-0f69279003a7-storage-credential-1762744537742`)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';
