#!/bin/bash

# script to test ALS functionality

set -e

if [ $# -eq 0 ]
	then
		echo ERROR usage: ./data_scaling.sh master jar sparkhome trainfile testfile
		exit 1
fi

MASTER=$1
JAR=$2
SPARKHOME=$3
TRAINFILE=$4
TESTFILE=$5

# create synthetic data
sbt/sbt "run-main als_debug.Create_MC_Data
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train_sampfact=20
--sigma=0.1
--noise=true
--test=true
--test_sampfact=0.1
--trainfile=$TRAINFILE
--testfile=$TESTFILE
--rank=10 
--m=500
--n=500"

# make data larger !
sbt/sbt "run-main als_debug.Matrix_Replicate
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--repfact=25
--nsplits=25
--m=500
--n=500"

TRAINFILE+="_replicated"

# run BALS ( could also do same thing with other input data )
sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=$MASTER
--jars=$JAR
--sparkhome=$SPARKHOME
--train=$TRAINFILE
--rank=10 
--niter=10
--m=2500
--n=2500"
