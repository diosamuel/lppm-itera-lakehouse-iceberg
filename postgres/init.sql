-- Airflow database
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
\c airflow
GRANT ALL ON SCHEMA public TO airflow;
ALTER SCHEMA public OWNER TO airflow;

-- Superset database
CREATE DATABASE superset;
CREATE USER superset WITH PASSWORD 'superset';
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
\c superset
GRANT ALL ON SCHEMA public TO superset;
ALTER SCHEMA public OWNER TO superset;
