#!/bin/bash

# script to reproduce scaling issues with join_als

# run with smaller rank (10)
sbt/sbt "run-main als_debug.Join_ALS 
--master=""
--jars=""
--sparkhome="/root/spark"
--train="/data/netflix_randSplit1_data.txt" 
--niter=10
--nsplits=32
--rank=10"

# run with larger rank (100)
sbt/sbt "run-main als_debug.Join_ALS
--master=""
--jars=""
--sparkhome="root/spark"
--train="/data/netflix_randSplit1_data.txt" 
--niter=10
--nsplits=32
--rank=100"