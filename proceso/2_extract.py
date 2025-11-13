# Databricks notebook source
#Bronze (ingesta + tipado básico + estandarización ligera)

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import create_map, col, coalesce, lit

# Widgets
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("ruta_csv", "abfss://bronze@stcrisperezproject.dfs.core.windows.net/student_performance.csv")
dbutils.widgets.text("tabla_bronze", "student_performance_bronze")

catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
ruta_csv = dbutils.widgets.get("ruta_csv")
tabla_bronze = dbutils.widgets.get("tabla_bronze")

# COMMAND ----------


# Esquema explícito basado en las columnas que enviaste
schema = StructType([
    StructField("Hours_Studied", IntegerType(), True),
    StructField("Attendance", IntegerType(), True),
    StructField("Parental_Involvement", StringType(), True),
    StructField("Access_to_Resources", StringType(), True),
    StructField("Extracurricular_Activities", StringType(), True),
    StructField("Sleep_Hours", IntegerType(), True),
    StructField("Previous_Scores", IntegerType(), True),
    StructField("Motivation_Level", StringType(), True),
    StructField("Internet_Access", StringType(), True),
    StructField("Tutoring_Sessions", IntegerType(), True),
    StructField("Family_Income", StringType(), True),
    StructField("Teacher_Quality", StringType(), True),
    StructField("School_Type", StringType(), True),
    StructField("Peer_Influence", StringType(), True),
    StructField("Physical_Activity", IntegerType(), True),
    StructField("Learning_Disabilities", StringType(), True),
    StructField("Parental_Education_Level", StringType(), True),
    StructField("Distance_from_Home", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Exam_Score", IntegerType(), True)
])

# Ingesta CSV → DataFrame
raw_df = (
    spark.read
         .option("header", True)
         .schema(schema)
         .csv(ruta_csv)
)


# COMMAND ----------

# Normalizaciones ligeras

str_cols = [
    "Parental_Involvement","Access_to_Resources","Extracurricular_Activities",
    "Motivation_Level","Internet_Access","Family_Income","Teacher_Quality",
    "School_Type","Peer_Influence","Learning_Disabilities",
    "Parental_Education_Level","Distance_from_Home","Gender"
]

bronze_df = raw_df
for c in str_cols:
    bronze_df = bronze_df.withColumn(c, trim(initcap(col(c))))

# Mapas (inglés estricto)
yes_map     = create_map(lit("Yes"), lit("Yes"), lit("No"), lit("No"))
level_map   = create_map(lit("Low"), lit("Low"), lit("Medium"), lit("Medium"), lit("High"), lit("High"))
neutral_map = create_map(lit("Positive"), lit("Positive"), lit("Negative"), lit("Negative"), lit("Neutral"), lit("Neutral"))

bronze_df = (
    bronze_df
      .withColumn("Internet_Access",            coalesce(yes_map[col("Internet_Access")], col("Internet_Access")))
      .withColumn("Extracurricular_Activities", coalesce(yes_map[col("Extracurricular_Activities")], col("Extracurricular_Activities")))
      .withColumn("Learning_Disabilities",      coalesce(yes_map[col("Learning_Disabilities")], col("Learning_Disabilities")))
      .withColumn("Parental_Involvement",       coalesce(level_map[col("Parental_Involvement")], col("Parental_Involvement")))
      .withColumn("Motivation_Level",           coalesce(level_map[col("Motivation_Level")], col("Motivation_Level")))
      .withColumn("Family_Income",              coalesce(level_map[col("Family_Income")], col("Family_Income")))
      .withColumn("Teacher_Quality",            coalesce(level_map[col("Teacher_Quality")], col("Teacher_Quality")))
      .withColumn("Peer_Influence",             coalesce(neutral_map[col("Peer_Influence")], col("Peer_Influence")))
      .withColumn("ingestion_date", current_timestamp())
)



# COMMAND ----------

bronze_df.display()
print(f"El DataFrame tiene {bronze_df.count()} filas y {len(bronze_df.columns)} columnas.")


# COMMAND ----------

# Persistencia a BRONZE

bronze_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.{tabla_bronze}")
