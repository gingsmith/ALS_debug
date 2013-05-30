ALS_debug
=========

code used to test distributed ALS with spark

Instructions for use on EC2:

**Note: I launch the following cluster using spot instances:
--cluster-type standalone -t m2.4xlarge
this runs the lastest spark AMI: ami-a60193cf

1) use version 0.7.0 of spark
2) checkout code @ https://github.com/gingsmith/ALS_debug.git
3) run setup_cluster.sh
4) get data from: 
5) run bad_scaling.sh, passing in <master> <jars> <sparkhome> <trainfile> as appropriate
6) run bad_memory.sh, passing in <master> <jars> <sparkhome> <trainfile> as appropriate

Instructions for local use:

1) checkout code @ https://github.com/gingsmith/ALS_debug.git
2) get data from: 
3) run bad_scaling.sh, passing in <master> <jars> <sparkhome> <trainfile> as appropriate
4) run bad_memory.sh, passin in <master> <jars> <sparkhome> <trainfile> as appropriate