#!/bin/sh

LOGFILE=shell_log.txt

sqlplus -s /nolog <<-EOF>> ${LOGFILE}
WHENEVER OSERROR EXIT 9;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
connect chinook/p4ssw0rd@xe
DBMS_OUTPUT.put_line('Connected to db');
EOF

sql_return_code=$?

if [ $sql_return_code != 0 ]
then
echo "The health script failed. Please refer to the log results.txt for more information"
echo "Error code $sql_return_code"
exit 1;
else
exit 0;
fi
