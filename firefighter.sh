#!/bin/sh
#SBATCH --array=0
#SBATCH --job-name globalMeltdown
#SBATCH --nodes=1 --tasks-per-node=2 --mem=16GB
#SBATCH --output=%j_log.txt
#SBATCH --time 0-24:00:00
#SBATCH --mail-user conrad.urban.gy@nps.edu
#SBATCH --mail-type END 

. /etc/profile
source /smallwork/$USER/comp3/bin/activate

python meltdown.py