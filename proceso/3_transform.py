# Databricks notebook source
#Silver

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Widgets
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("tabla_source", "student_performance_bronze")
dbutils.widgets.text("esquema_sink", "silver")
dbutils.widgets.text("tabla_sink", "student_performance_silver")

catalogo = dbutils.widgets.get("catalogo")
esq_src  = dbutils.widgets.get("esquema_source")
tab_src  = dbutils.widgets.get("tabla_source")
esq_sink = dbutils.widgets.get("esquema_sink")
tab_sink = dbutils.widgets.get("tabla_sink")

# Carga Bronze
bdf = spark.table(f"{catalogo}.{esq_src}.{tab_src}")


# COMMAND ----------



# Reglas de calidad (filtros y dominios válidos)

# Requeridos para el análisis
sdf = (
    bdf.dropna(how="all")
      .filter(col("Hours_Studied").isNotNull() & col("Attendance").isNotNull() & col("Exam_Score").isNotNull())
)

# Dominios válidos (estoy considerando solo inglés)
valid_levels = ["Low", "Medium", "High"]
valid_peer   = ["Positive", "Negative", "Neutral"]
valid_gender = ["Male", "Female"]

sdf = (
    sdf
      .filter(col("Parental_Involvement").isin(valid_levels))
      .filter(col("Motivation_Level").isin(valid_levels))
      .filter(col("Teacher_Quality").isin(valid_levels))
      .filter(col("Family_Income").isin(valid_levels))
      .filter(col("Peer_Influence").isin(valid_peer))
      .filter(col("Gender").isin(valid_gender))
)



# COMMAND ----------

# Variables derivadas útiles
level_to_score = create_map(lit("Low"), lit(1), lit("Medium"), lit(2), lit("High"), lit(3))
peer_to_score  = create_map(lit("Negative"), lit(1), lit("Neutral"), lit(2), lit("Positive"), lit(3))
access_to_res_map = create_map(lit("Low"), lit(1), lit("Medium"), lit(2), lit("High"), lit(3))

sdf = (
    sdf
      # Scores a partir de categorías
      .withColumn("motivation_score", level_to_score[col("Motivation_Level")])
      .withColumn("parental_involvement_score", level_to_score[col("Parental_Involvement")])
      .withColumn("family_income_score", level_to_score[col("Family_Income")])
      .withColumn("teacher_quality_score", level_to_score[col("Teacher_Quality")])
      .withColumn("peer_influence_score",  peer_to_score[col("Peer_Influence")])
      # Banderas binarias
      .withColumn("internet_access_int", when(col("Internet_Access") == "Yes", 1).otherwise(0))
      .withColumn("extracurricular_int", when(col("Extracurricular_Activities") == "Yes", 1).otherwise(0))
      .withColumn("learning_disabilities_int", when(col("Learning_Disabilities") == "Yes", 1).otherwise(0))
      # Buckets interpretables
      .withColumn("hours_bucket",
          when(col("Hours_Studied") <= 5, "0-5")
         .when(col("Hours_Studied") <= 10, "6-10")
         .when(col("Hours_Studied") <= 15, "11-15")
         .when(col("Hours_Studied") <= 20, "16-20")
         .otherwise("21+")
      )
      .withColumn("attendance_bucket",
          when(col("Attendance") < 50, "<50")
         .when(col("Attendance") <= 75, "50-75")
         .when(col("Attendance") <= 90, "76-90")
         .otherwise(">90")
      )
      .withColumn("sleep_bucket",
          when(col("Sleep_Hours") < 6, "Short")
         .when(col("Sleep_Hours") <= 8, "Optimal")
         .otherwise("Long")
      )
      # Índice de recursos (combinado e interpretable)
      .withColumn(
          "resource_index",
          col("internet_access_int") * 5 +
          col("extracurricular_int") * 2 +
          coalesce(col("Tutoring_Sessions"), lit(0)) +
          access_to_res_map[col("Access_to_Resources")]
      )
      # Eficiencia de estudio (maneja división por 0)
      .withColumn(
          "study_efficiency",
          when(col("Hours_Studied") > 0, (col("Exam_Score") - col("Previous_Scores")) / col("Hours_Studied"))
          .otherwise(lit(None).cast(DoubleType()))
      )
      # Bandera de riesgo
      .withColumn(
          "risk_flag",
          ((col("Attendance") < 60) | (col("Previous_Scores") < 50)) & (col("learning_disabilities_int") == 1)
      )
      .withColumn("processing_ts", current_timestamp())
)



# COMMAND ----------

# Persistencia a tabla SILVER
sdf.write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{tab_sink}")

display(spark.table(f"{catalogo}.{esq_sink}.{tab_sink}").limit(10))
