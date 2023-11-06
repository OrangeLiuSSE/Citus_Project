#!/bin/bash

# Author: Orange 

# Modify PG configure
node_idx=$(cut -c 6 <<< "$(hostname)")
PGSQLCONF='/temp/teamf-data/postgresql.conf'
PGSQLHBA='/temp/teamf-data/pg_hba.conf'
CO_HOSTNAME='xgph10'
INSTALLDIR=$HOME/pgsql

# TEAM_ID=f # IMPORTANT: change x to your actual team identifier
# # DATADIR - directory containing database files
# DATADIR=/temp/team${TEAM_ID}-data


# rm -rf /temp/teamf-data


# mkdir -p ${DATADIR}
# ${INSTALLDIR}/bin/initdb -D ${DATADIR} --no-locale --encoding=UTF8
# if [ -e ${DATADIR}/postgresql.conf ]; then
# 	grep -q citus ${DATADIR}/postgresql.conf
# 	if [ $0 -ne 0 ]; then
# 		echo "shared_preload_libraries = 'citus'" >> ${DATADIR}/postgresql.conf
# 	fi
# else
# 	echo "ERROR: ${DATADIR}/postgresql.conf missing!"
# fi


# sleep 20


# sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" $PGSQLCONF
# sed -i 's/#port = 5432/port = 5102/g' $PGSQLCONF
# sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'citus'/g" $PGSQLCONF

# sed -i "s/shared_buffers = 128MB/shared_buffers = 2GB/g" $PGSQLCONF
# sed -i "s/#work_mem = 4MB/work_mem = 128MB/g" $PGSQLCONF

# if [ $HOSTNAME = $CO_HOSTNAME ]
#     then
#         sed -i "s/host    all             all             127.0.0.1\/32            trust/host   all             all           0.0.0.0\/0                 trust/g" $PGSQLHBA
#         sed -i "s/host    replication     all             127.0.0.1\/32            trust/host   replication     all           192.168.51.0\/24           trust/g" $PGSQLHBA
#     else
#         sed -i 's/host    all             all             127.0.0.1\/32            trust/host   all             all           192.168.51.0\/24           trust/g' $PGSQLHBA
#         sed -i 's/host    replication     all             127.0.0.1\/32            trust/host   replication     all           192.168.51.0\/24           trust/g' $PGSQLHBA
# fi


# start postgres
/home/stuproj/cs4224f/pgsql/bin/pg_ctl -D /temp/teamf-data -l logfile start
# psql -d postgres -c "create database project;"
sleep 5

# # Set Citus Extension
# psql -c "CREATE EXTENSION citus;"
# sleep 5

# if [ $HOSTNAME = $CO_HOSTNAME ]
#     then
#         # Add coordinator nodes
#         psql -c "SELECT citus_set_coordinator_host('xgph10', 5102);"

#         # Add worker nodes
#         psql -c "SELECT * from citus_add_node('xgph11', 5102);"
#         psql -c "SELECT * from citus_add_node('xgph12', 5102);"
#         psql -c "SELECT * from citus_add_node('xgph13', 5102);"
#         psql -c "SELECT * from citus_add_node('xgph14', 5102);"

#         sleep 5

#         psql -c "SELECT * FROM citus_get_active_worker_nodes();"

# fi
# sleep 5


# if [ $HOSTNAME = $CO_HOSTNAME ]
#     then
#         psql -d project -f '/home/stuproj/cs4224f/citus_script_file/data_model/TableCreator.sql'
#         psql -d project -c 'select * from citus_shards'
# 	    psql -d project -c 'ALTER DATABASE project SET citus.enable_repartition_joins=true'
#     else
#         sleep 60
# fi


# job_pids=()
# for (( i=0; i<4; i++ )); do
#     client_id=$((($node_idx - 0) * 4 + i))
#     echo -e "[$node_idx | $(date +"%T")] 5.b. Creating client #$client_id..."
#     python /home/stuproj/cs4224f/citus_script_file/transaction/total_transaction.py /home/stuproj/cs4224f/proj_data/project_files/xact_files/"$client_id".txt 1>/home/stuproj/cs4224f/citus_script_file/result/"$client_id".log &
#     job_pids+=($!)
# done
# wait "${job_pids[@]}"
# echo -e "[$node_idx | $(date +"%T")] 5.c All clients on this node finished their transactions..."


# sleep 36000


# create table in co node
if [ $HOSTNAME = $CO_HOSTNAME ]
    then
        psql -d project -c 'select sum(W_YTD) from Warehouse' > warehouse.txt
        psql -d project -c 'select sum(D_YTD), sum(D_NEXT_O_ID) from District' > District.txt
        psql -d project -c 'select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer' > Customer.txt
        psql -d project -c 'select max(O_ID), sum(O_OL_CNT) from public.order' > order.txt
        psql -d project -c 'select sum(OL_AMOUNT), sum(OL_QUANTITY) from order_line' > order_line.txt
        psql -d project -c 'select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from stock' > stock.txt
    else
        sleep 30
fi
