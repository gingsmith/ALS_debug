# run bals, graphlab, and spark join als

set -e

if [ $# -eq 0 ]
	then
		echo ERROR usage: ./rank_scaling.sh master trainfile
		exit 1
fi

MASTER=$1
TRAINFILE=$2

# ----------------- rank 10

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=$MASTER
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=$TRAINFILE
--rank=10 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank10

# ----------------- rank 20

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=$MASTER
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=$TRAINFILE
--rank=20 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank20

# ----------------- rank 30

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=$MASTER
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=$TRAINFILE
--rank=30 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank30

# ----------------- rank 40

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=$MASTER
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=$TRAINFILE
--rank=40 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank40

# ----------------- rank 50

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=$MASTER
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=$TRAINFILE
--rank=50 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank50
