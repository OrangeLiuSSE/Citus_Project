# CS5424 Citus

> This README is only for the Citus implementation.

## Citus Implementation

Contents of the repo:
- [initial_citus/](initial_citus/): contains shell scripts for initial citus.
- [data_model/](/data_model/): contains Python and SQL scripts to update, preprocess, and pre-load the original data set.
- [transactions/](transaction/): contains Python scripts for each transaction defined in the project and the script of driver of citus.
- [dbstate](dbstate/): is the final state information of every table of the database.
- [final_result](final_result/): is the output of the Python scripts of 20 clients result.

### Model Design

We use SQL file to construct the database and initial the data

### Data Preprocessing

Before using the provided data to set up the initial database, we need to update and generate several `*csv` files using Python scripts. 

> For your convenience, you can just download preprocessed data set at [here](https://drive.google.com/drive/folders/1vTZxsb8-AfB_gGoVFfvdjevcSmEUOR7i?usp=drive_link).

#### Notes

There are some slight modifications made to some of the original preload `*.csv` files:

- `order.csv`: some values in column `O CARRIER ID` are `null` in the original file, which are not of valid `null` type in Citus, so these values are replaced by ``(empty value) instead.
- `order-line.csv`: some values in column `OL_DELIVERY_D` are `null` in the original file, which are not of valid `timestamp` type in Citus, so these values are replaced by ``(empty value) instead.


### CLuster Setup

We manualy select 5 separate servers in the long partition of the Slurm with consecutive IPs (e.g., xgph[10-14]). 
The smallest IP among them will be selected as the Coordinator node. 

### File Preparation

**Configuration files**: modify the pg_hba.conf and postgresql.conf as below:
    - shared buffers: 2GB (In Citus Docs, they retrive that 1/4 of RAM might be the best)
    - work mem: 128M (In Citus Docs, they retrive that, if user want to improve the performance, they can increase the work mem, and default is 4MB)
    - Modify the IP Address to 192.168.51.0/24 in pg hba.conf

**Code files**: Put this repo in `/home/stuproj/cs4224f/citus_script_file`.

**Data files**: Put data (given in the link in the **Data Preload**) in `/home/stuproj/cs4224f/proj_data/project_files/data_files`.

**Transaction files**: Put transaction files in `/home/stuproj/cs4224f/citus_script_file/transactions/`.

### Folder Creation

Create the following 2 folders if they not exist:

- `/home/stuproj/cs4224f/citus_script_file/final_result`: stores result for each client on each node.
- `/home/stuproj/cs4224f/citus_script_file/dbstate`: is used for get final db state.

### System Initialization

Before we submit the job to Slurm, we should switch the current working directory to `/home/stuproj/cs4224f/citus_script_file/transaction` using the `cd` command.

Create a script, assumed it is called `start.sh`, with the following contents:

```bash
#!/bin/bash -l

# main scripts
srun bash /home/stuproj/cs4224f/citus_script_file/transaction/citus_driver.sh
```

Run the following commands to submit the job:

`sbatch --partition=long --nodelist=xgph[10-14] --time="1440" --output="logs/slurm-%A.out" ./start.sh`

### Output Analysis

The output of the job will be put at `/home/stuproj/cs4224f/citus_script_file/final_result`

