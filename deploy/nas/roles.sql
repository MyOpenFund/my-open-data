-- my-open-data catalogue: two roles and one database, on the stack's own Postgres (service init)
-- or on any Postgres. Run as a superuser; reruns are no-ops:
--   psql -U postgres -d postgres -v db=lake_catalog \
--        -v publisher_password=<hex> -v reader_password=<hex> -f roles.sql
-- lake_publisher owns the database (DuckLake creates its tables in schema public);
-- lake_reader can connect and read every table, now and future, and nothing else.
-- compose.yaml carries the block below verbatim (tests/test_deploy_roles.py checks it).
-- BEGIN roles.sql
\set ON_ERROR_STOP on
SELECT format('CREATE ROLE lake_publisher LOGIN PASSWORD %L', :'publisher_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lake_publisher') \gexec
SELECT format('CREATE ROLE lake_reader LOGIN PASSWORD %L', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lake_reader') \gexec
SELECT format('CREATE DATABASE %I OWNER lake_publisher', :'db')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db') \gexec
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO lake_publisher, lake_reader;
\connect :"db"
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO lake_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO lake_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE lake_publisher IN SCHEMA public
    GRANT SELECT ON TABLES TO lake_reader;
-- END roles.sql
