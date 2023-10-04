FROM gvenzl/oracle-xe:21.3.0-slim

COPY docker-health-check.sh /opt/oracle/docker-health-check.sh
COPY chinook.sql /container-entrypoint-initdb.d/chinook.sql

EXPOSE 1521 15210
