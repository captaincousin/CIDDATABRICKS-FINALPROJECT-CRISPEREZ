%sql
-- Revoke.sql
-- Revierte los permisos asignados al grupo `usuarios` durante el proyecto.

-----------------------------------------------------------
-- 1) REVOKE permisos sobre CATALOG
-----------------------------------------------------------
REVOKE USE CATALOG ON CATALOG catalog_dev FROM `usuarios`;

-----------------------------------------------------------
-- 2) REVOKE permisos sobre schemas Bronze / Silver / Golden
-----------------------------------------------------------
REVOKE USE SCHEMA, CREATE, SELECT, MODIFY ON SCHEMA catalog_dev.bronze FROM `usuarios`;
REVOKE USE SCHEMA, CREATE, SELECT, MODIFY ON SCHEMA catalog_dev.silver FROM `usuarios`;
REVOKE USE SCHEMA, CREATE, SELECT, MODIFY ON SCHEMA catalog_dev.golden FROM `usuarios`;

-----------------------------------------------------------
-- 3) REVOKE permisos sobre External Locations
-----------------------------------------------------------
REVOKE READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-bronze` FROM `usuarios`;
REVOKE READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-silver` FROM `usuarios`;
REVOKE READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-golden` FROM `usuarios`;

-----------------------------------------------------------
-- 4) (Opcional) Quitar usuarios del grupo
-----------------------------------------------------------
-- ALTER GROUP usuarios DROP USER 'mariita.2004@hotmail.com';
-- ALTER GROUP usuarios DROP USER 'mariita.2004_hotmail.com#EXT#@mariita2004hotmail.onmicrosoft.com';

-----------------------------------------------------------
-- 5) (Opcional) Eliminar el grupo
-----------------------------------------------------------
-- DROP GROUP IF EXISTS usuarios;

-- FIN DEL SCRIPT
