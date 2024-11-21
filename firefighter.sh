#!/bin/sh
#SBATCH --job-name globalMeltdown
#SBATCH --nodes=1 --tasks-per-node=2 --mem=16GB
#SBATCH --output=%j_log.txt
#SBATCH --time 0-24:00:00
#SBATCH --mail-user conrad.urban.gy@nps.edu
#SBATCH --mail-type END 

. /etc/profile
source /smallwork/$USER/comp3/bin/activate

# parameters
# --first           : first run, will read in the data and create a database    : default False  
# --parallel        : run in parallel mode, one proccess for each year          : default False
# --db-path         : path to database file                                     : default 'fire.db'

python meltdown.py