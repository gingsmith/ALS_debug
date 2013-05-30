#!/bin/bash

# script to reproduce scaling issues with join_als

set -e

if [ $# -eq 0 ]
	then
		echo ERROR usage: ./bad_memory.sh master jar sparkhome trainfile
		exit 1
fi

MASTER=$1
JAR=$2
SPARKHOME=$3
TRAINFILE=$4


# run with smaller rank (10)
sbt/sbt "run-main als_debug.Join_ALS 
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--nsplits=32
--rank=10" 2>&1 | tee small_rank_log

# run with larger rank (100)
sbt/sbt "run-main als_debug.Join_ALS
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--niter=10
--nsplits=32
--rank=100" 2>&1 | tee large_rank_log