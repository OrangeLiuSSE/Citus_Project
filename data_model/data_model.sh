#!/usr/bin/env bash

# Author: Orange 

CO_HOSTNAME='xcnf0'

# start postgres
/home/stuproj/cs4224f/pgsql/bin/pg_ctl -D /temp/teamf-data -l logfile start
sleep 5

# create table in co node
if [ $HOSTNAME = $CO_HOSTNAME ]
    then
        psql -d project -f '/home/stuproj/cs4224f/citus_script_file/data_model/TableCreator.sql'
        psql -d project -c 'select * from citus_shards'
	psql -d project -c 'ALTER DATABASE project SET citus.enable_repartition_joins=true'
    else
        sleep 100
fi
