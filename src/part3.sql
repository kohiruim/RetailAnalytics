CREATE ROLE administrator WITH LOGIN SUPERUSER CREATEDB CREATEROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO administrator;

CREATE ROLE visitor WITH LOGIN;
GRANT CONNECT ON DATABASE postgres TO visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;

-- REASSIGN OWNED BY administrator TO postgres;
-- DROP OWNED BY administrator;
-- DROP ROLE administrator;
--
-- REASSIGN OWNED BY visitor TO postgres;
-- DROP OWNED BY visitor;
-- DROP ROLE visitor;
