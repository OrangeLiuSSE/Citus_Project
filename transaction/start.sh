
sbatch --nodelist=xgph[10-14] --partition=long --time="2000" --output="logs/slurm-%A.out" /home/stuproj/cs4224f/citus_script_file/transaction/citus_driver.sh

# 