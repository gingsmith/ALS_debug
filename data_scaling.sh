#!/bin/bash

# script to reproduce memory issues with join_als

set -e

if [ $# -eq 0 ]
	then
		echo ERROR usage: ./data_scaling.sh master jar sparkhome trainfile
		exit 1
fi

MASTER=$1
JAR=$2
SPARKHOME=$3
TRAINFILE=$4


# run on smaller problem (netflix)
sbt/sbt "run-main als_debug.Join_ALS 
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--rank=10
--niter=10
--nsplits=96
--big=false" 2>&1 | tee small_data_log_0604

# run on bigger problem (4x netflix)
sbt/sbt "run-main als_debug.Join_ALS
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--rank=10 
--niter=10
--big=true
--m=17770
--npslits=96
--n=480189" 2>&1 | tee large_data_log_0604
