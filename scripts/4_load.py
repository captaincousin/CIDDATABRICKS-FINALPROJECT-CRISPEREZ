# Databricks notebook source
#Golden

from pyspark.sql.functions import *
from pyspark.sql import Row

# Widgets
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("tabla_source", "student_performance_silver")
dbutils.widgets.text("esquema_sink", "golden")

# Nombres de tablas Golden
dbutils.widgets.text("fact_table", "fact_student_performance")
dbutils.widgets.text("agg_gender_table", "agg_exam_by_gender")
dbutils.widgets.text("agg_hours_table", "agg_exam_by_hours_bucket")
dbutils.widgets.text("agg_att_table", "agg_exam_by_attendance_bucket")
dbutils.widgets.text("agg_mot_table", "agg_exam_by_motivation")
dbutils.widgets.text("agg_school_income_table", "agg_exam_school_vs_income")
dbutils.widgets.text("corr_table", "corr_with_exam")

catalogo = dbutils.widgets.get("catalogo")
esq_src  = dbutils.widgets.get("esquema_source")
tab_src  = dbutils.widgets.get("tabla_source")
esq_sink = dbutils.widgets.get("esquema_sink")

fact_table = dbutils.widgets.get("fact_table")
agg_gender_table = dbutils.widgets.get("agg_gender_table")
agg_hours_table = dbutils.widgets.get("agg_hours_table")
agg_att_table = dbutils.widgets.get("agg_att_table")
agg_mot_table = dbutils.widgets.get("agg_mot_table")
agg_school_income_table = dbutils.widgets.get("agg_school_income_table")
corr_table = dbutils.widgets.get("corr_table")

# Carga Silver
sdf = spark.table(f"{catalogo}.{esq_src}.{tab_src}")

# COMMAND ----------

"""
agg_exam_by_gender - comparar rendimiento promedio entre géneros.
agg_exam_by_hours_bucket - ver cómo cambia la nota según las horas de estudio agrupadas
agg_exam_by_attendance_bucket - evidenciar la relación asistencia ↔ desempeño.
agg_exam_by_motivation - comparar promedios por motivación.
agg_exam_school_vs_income - tabla cruzada para ver diferencias por tipo de escuela e ingreso familiar
corr_with_exam - dentificar qué variables numéricas se asocian más (positiva o negativamente) con la nota.
"""


# COMMAND ----------


# 1) Hecho principal (lo he creado para dashboard)
fact_df = sdf.select(
    "Hours_Studied","Attendance","Sleep_Hours","Previous_Scores","Exam_Score",
    "Motivation_Level","Parental_Involvement","Access_to_Resources","Family_Income",
    "Teacher_Quality","School_Type","Peer_Influence","Physical_Activity",
    "Learning_Disabilities","Parental_Education_Level","Distance_from_Home","Gender",
    "Tutoring_Sessions","Internet_Access","Extracurricular_Activities",
    "hours_bucket","attendance_bucket","sleep_bucket","resource_index",
    "motivation_score","parental_involvement_score","family_income_score","teacher_quality_score",
    "peer_influence_score","study_efficiency","risk_flag","processing_ts"
)
fact_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{fact_table}")


# 2) Agregaciones clave para visuales
sdf.groupBy("Gender").agg(avg("Exam_Score").alias("avg_exam"), count("*").alias("n")) \
   .write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{agg_gender_table}")

sdf.groupBy("hours_bucket").agg(avg("Exam_Score").alias("avg_exam"), count("*").alias("n")) \
   .write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{agg_hours_table}")

sdf.groupBy("attendance_bucket").agg(avg("Exam_Score").alias("avg_exam"), count("*").alias("n")) \
   .write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{agg_att_table}")

sdf.groupBy("Motivation_Level").agg(avg("Exam_Score").alias("avg_exam"), count("*").alias("n")) \
   .write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{agg_mot_table}")

sdf.groupBy("School_Type", "Family_Income").agg(avg("Exam_Score").alias("avg_exam"), count("*").alias("n")) \
   .write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{agg_school_income_table}")


# COMMAND ----------


# 3) Correlaciones simples con Exam_Score

numeric_cols = [
    "Hours_Studied","Attendance","Sleep_Hours","Previous_Scores",
    "Physical_Activity","Tutoring_Sessions","resource_index",
    "motivation_score","parental_involvement_score","teacher_quality_score","peer_influence_score"
]

rows = []
for c in numeric_cols:
    # corr puede devolver None si no hay varianza; maneja eso a float('nan') para no romper
    val = sdf.stat.corr(c, "Exam_Score")
    rows.append(Row(feature=c, corr=float(val) if val is not None else float('nan')))

spark.createDataFrame(rows).write.mode("overwrite").saveAsTable(f"{catalogo}.{esq_sink}.{corr_table}")


# Vistas rápidas

display(spark.table(f"{catalogo}.{esq_sink}.{fact_table}").limit(10))
display(spark.table(f"{catalogo}.{esq_sink}.{agg_gender_table}").limit(10))
