#!/bin/bash

# script to reproduce memory issues with join_als

# run on smaller problem (netflix)
sbt/sbt "run-main als_debug.Join_ALS 
--master=""
--jars=""
--sparkhome=""
--train="/data/netflix_randSplit1_data.txt" 
--rank=10
--niter=10
--big=false"

# run on bigger problem (4x netflix)
sbt/sbt "run-main als_debug.Join_ALS
--master=""
--jars=""
--sparkhome="" 
--train="/data/netflix_randSplit1_data.txt" 
--rank=10 
--niter=10
--big=true
--m=17770
--n=480189"