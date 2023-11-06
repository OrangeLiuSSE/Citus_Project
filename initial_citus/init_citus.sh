#!/usr/bin/env bash

# Author: Orange 

# Modify PG configure

PGSQLCONF='/temp/teamf-data/postgresql.conf'
PGSQLHBA='/temp/teamf-data/pg_hba.conf'
CO_HOSTNAME='xcnf0'

sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" $PGSQLCONF
sed -i 's/#port = 5432/port = 5102/g' $PGSQLCONF
sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'citus'/g" $PGSQLCONF

sed -i "s/shared_buffers = 128MB/shared_buffers = 2GB/g" $PGSQLCONF
sed -i "s/#work_mem = 4MB/work_mem = 128MB/g" $PGSQLCONF

if [ $HOSTNAME = $CO_HOSTNAME ]
    then
        sed -i "s/host    all             all             127.0.0.1\/32            trust/host   all             all           0.0.0.0\/0                 trust/g" $PGSQLHBA
        sed -i "s/host    replication     all             127.0.0.1\/32            trust/host   replication     all           192.168.48.0\/24           trust/g" $PGSQLHBA
    else
        sed -i 's/host    all             all             127.0.0.1\/32            trust/host   all             all           192.168.48.0\/24           trust/g' $PGSQLHBA
        sed -i 's/host    replication     all             127.0.0.1\/32            trust/host   replication     all           192.168.48.0\/24           trust/g' $PGSQLHBA
fi


# start postgres
/home/stuproj/cs4224f/pgsql/bin/pg_ctl -D /temp/teamf-data -l logfile start
psql -d postgres -c "create database project;"
sleep 5

# Set Citus Extension
psql -c "CREATE EXTENSION citus;"
sleep 5

if [ $HOSTNAME = $CO_HOSTNAME ]
    then
        # Add coordinator nodes
        psql -c "SELECT citus_set_coordinator_host('192.168.48.128', 5102);"

        # Add worker nodes
        psql -c "SELECT * from citus_add_node('192.168.48.129', 5102);"
        psql -c "SELECT * from citus_add_node('192.168.48.130', 5102);"
        psql -c "SELECT * from citus_add_node('192.168.48.131', 5102);"
        psql -c "SELECT * from citus_add_node('192.168.48.132', 5102);"

        sleep 5

        psql -c "SELECT * FROM citus_get_active_worker_nodes();"

fi



sleep 5
