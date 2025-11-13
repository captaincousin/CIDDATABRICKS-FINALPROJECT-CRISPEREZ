# Databricks notebook source
# MAGIC %sql
# MAGIC -- permisos_usuarios.sql
# MAGIC -- ------------------------------------------------------------
# MAGIC -- 1) Crear grupo y agregar miembros (tus dos cuentas)
# MAGIC -- ------------------------------------------------------------
# MAGIC --CREATE GROUP IF NOT EXISTS usuarios;
# MAGIC
# MAGIC -- Reemplaza por los correos exactos si fuera necesario.
# MAGIC --ALTER GROUP usuarios ADD USER 'mariita.2004@hotmail.com';
# MAGIC --ALTER GROUP usuarios ADD USER 'mariita.2004_hotmail.com#EXT#@mariita2004hotmail.onmicrosoft.com';
# MAGIC
# MAGIC -- ------------------------------------------------------------
# MAGIC -- (Importante al ejecutar Pipeline Final) Permisos al usuario que corre el job en el Storage Credential.
# MAGIC -- ------------------------------------------------------------
# MAGIC --GRANT CREATE EXTERNAL LOCATION
# MAGIC --ON STORAGE CREDENTIAL `cbfaa23e-8ed3-4888-9eea-0f69279003a7-storage-credential-1762744537742`
# MAGIC --TO `usuarios`;
# MAGIC
# MAGIC -- ------------------------------------------------------------
# MAGIC -- 2) Permiso para usar el catálogo (requerido para cualquier acción dentro de catalog_dev)
# MAGIC -- ------------------------------------------------------------
# MAGIC GRANT USE CATALOG ON CATALOG catalog_dev TO `usuarios`;
# MAGIC
# MAGIC -- ------------------------------------------------------------
# MAGIC -- 3) Permisos sobre los schemas del modelo Medallion
# MAGIC --    (USE SCHEMA para consultar, CREATE para crear tablas,
# MAGIC --     SELECT/MODIFY para leer y escribir datos/tablas)
# MAGIC -- ------------------------------------------------------------
# MAGIC GRANT USE SCHEMA, CREATE, SELECT, MODIFY ON SCHEMA catalog_dev.bronze TO `usuarios`;
# MAGIC GRANT USE SCHEMA, CREATE, SELECT, MODIFY ON SCHEMA catalog_dev.silver TO `usuarios`;
# MAGIC GRANT USE SCHEMA, CREATE, SELECT, MODIFY ON SCHEMA catalog_dev.golden TO `usuarios`;
# MAGIC
# MAGIC -- ------------------------------------------------------------
# MAGIC -- 4) Acceso a las External Locations (lectura/escritura de archivos en ADLS)
# MAGIC --    Necesario si escribes/lees físicamente en estas ubicaciones.
# MAGIC -- ------------------------------------------------------------
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-bronze` TO `usuarios`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-silver` TO `usuarios`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-golden` TO `usuarios`;
# MAGIC
