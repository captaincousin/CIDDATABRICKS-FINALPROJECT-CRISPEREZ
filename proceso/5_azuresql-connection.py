# Databricks notebook source
from pyspark.sql import functions as F

# =============================
# Widgets según tu proyecto
# =============================
dbutils.widgets.text("catalogo",      "catalog_dev")
dbutils.widgets.text("schema_golden", "golden")    # origen UC
dbutils.widgets.text("target_host",   "sql-crisperez-project.database.windows.net")
dbutils.widgets.text("target_db",     "db-crisperez-project")
dbutils.widgets.text("target_schema", "golden")    # destino en Azure SQL

# Tablas de tu zona Golden
dbutils.widgets.text("table_list_csv", ",".join([
    "fact_student_performance",
    "agg_exam_by_gender",
    "agg_exam_by_hours_bucket",
    "agg_exam_by_attendance_bucket",
    "agg_exam_by_motivation",
    "agg_exam_school_vs_income",
    "corr_with_exam"
]))

# overwrite 1ª vez, luego append si haces incrementales
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite","append"])

# =============================
# Credenciales desde Key Vault
# =============================
# Scope Key Vault-backed: azure-kv-crisperez
sql_user     = dbutils.secrets.get(scope="kv-crisperez", key="sql-user")
sql_password = dbutils.secrets.get(scope="kv-crisperez", key="sql-password")

# =============================
# JDBC URL (Azure SQL)
# =============================
jdbcUrl = (
    f"jdbc:sqlserver://{dbutils.widgets.get('target_host')}:1433;"
    f"database={dbutils.widgets.get('target_db')};"
    f"encrypt=true;trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)

write_opts = {
    "url": jdbcUrl,
    "user": sql_user,
    "password": sql_password,
    "batchsize": "10000",
    "truncate": "true"   # permite overwrite con TRUNCATE si la tabla existe
}

catalogo      = dbutils.widgets.get("catalogo")
schema_golden = dbutils.widgets.get("schema_golden")
target_schema = dbutils.widgets.get("target_schema")
write_mode    = dbutils.widgets.get("write_mode")
tables        = [t.strip() for t in dbutils.widgets.get("table_list_csv").split(",") if t.strip()]

def export_table(src_fqn: str, dst_schema: str, dst_table: str, mode: str):
    print(f"→ {src_fqn}  →  {dst_schema}.{dst_table}")
    df = spark.table(src_fqn)

    # (Opcional) normalizar nombres de columnas para SQL Server si tuvieras espacios/caracteres raros:
    # df = df.select([F.col(c).alias(c.replace(" ", "_")) for c in df.columns])

    df.write.format("jdbc") \
      .options(**write_opts) \
      .option("dbtable", f"{dst_schema}.{dst_table}") \
      .mode(mode).save()

for t in tables:
    export_table(f"{catalogo}.{schema_golden}.{t}", target_schema, t, write_mode)

print("✅ Exportación Golden → Azure SQL completada")


# COMMAND ----------

jdbc_hostname = "sql-crisperez-project.database.windows.net"
jdbc_port = "1433"
jdbc_database = "db-crisperez-project"

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;"


# COMMAND ----------


query = """
SELECT TOP (10) *
FROM golden.agg_exam_by_gender
"""

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", sql_user) \
    .option("password", sql_password) \
    .option("query", query) \
    .load()

display(df)
