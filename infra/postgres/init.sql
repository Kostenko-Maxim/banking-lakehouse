SELECT 'CREATE DATABASE airflow OWNER bank' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='airflow')\gexec
SELECT 'CREATE DATABASE metastore OWNER bank' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='metastore')\gexec
SELECT 'CREATE DATABASE analytics OWNER bank' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='analytics')\gexec
