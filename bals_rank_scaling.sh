#!/bin/bash

# script to reproduce scaling issues with join_als

set -e

if [ $# -eq 0 ]
	then
		echo ERROR usage: ./rank_scaling.sh master jar sparkhome trainfile
		exit 1
fi

MASTER=$1
JAR=$2
SPARKHOME=$3
TRAINFILE=$4


# run with smaller rank (10)
sbt/sbt "run-main als_debug.Broadcast_ALS
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--m=17770
--npslits=32
--n=480189
--rank=10" 2>&1 | tee bals_rank_10_log_0610

# slightly bigger rank (20)
sbt/sbt "run-main als_debug.Broadcast_ALS 
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--m=17770
--npslits=32
--n=480189
--rank=20" 2>&1 | tee bals_rank_20_log_0610

# a little bigger... rank (30)
sbt/sbt "run-main als_debug.Broadcast_ALS 
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--m=17770
--npslits=32
--n=480189
--rank=30" 2>&1 | tee bals_rank_30_log_0610

# bigger rank (100)
sbt/sbt "run-main als_debug.Broadcast_ALS
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--m=17770
--npslits=32
--n=480189
--rank=100" 2>&1 | tee bals_rank_100_log_0610