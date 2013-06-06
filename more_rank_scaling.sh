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
sbt/sbt "run-main als_debug.Join_ALS 
--blocked=true
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--nsplits=32
--rank=40" 2>&1 | tee rank_40_log_0606

# slightly bigger rank (20)
sbt/sbt "run-main als_debug.Join_ALS 
--blocked=true
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--nsplits=32
--rank=50" 2>&1 | tee rank_50_log_0606

# a little bigger... rank (30)
sbt/sbt "run-main als_debug.Join_ALS 
--blocked=true
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--nsplits=32
--rank=60" 2>&1 | tee rank_60_log_0606

# bigger rank (100)
sbt/sbt "run-main als_debug.Join_ALS
--blocked=true
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--nsplits=32
--rank=70" 2>&1 | tee rank_70_log_0606