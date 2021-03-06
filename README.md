ALS_debug
=========

code used to test distributed ALS with spark  

Instructions for use on EC2:  

** Note: I launch the following cluster using spot instances:  
-s 4 --cluster-type standalone -t m2.4xlarge  
this runs the lastest spark AMI: ami-a60193cf  

1) use version 0.7.0 of spark  
2) clone code @ https://github.com/gingsmith/ALS_debug.git  
3) run setup_cluster.sh from ALS_debug directory  
4) get data from: http://www.cs.berkeley.edu/~vsmith/data/netflix_randSplit1_data.txt  
5) run bad_scaling.sh, passing in [master jars sparkhome trainfile] as appropriate  
6) run bad_memory.sh, passing in [master jars sparkhome trainfile] as appropriate  

** depending on the size of the cluster, it may be important to change "nsplits", as well  

example:  

export PUBLIC_HOSTNAME=`ec2-metadata -p | cut -d: -f2 | tr -d ' '`

./rank_scaling.sh spark://$PUBLIC_HOSTNAME:7077 /mnt/ALS_debug/target/als_debug-assembly-1.0.jar /root/spark hdfs://$PUBLIC_HOSTNAME:9000/data/netflix_randSplit1_data.txt

./data_scaling.sh spark://$PUBLIC_HOSTNAME:7077 /mnt/ALS_debug/target/als_debug-assembly-1.0.jar /root/spark hdfs://$PUBLIC_HOSTNAME:9000/data/netflix_randSplit1_data.txt

Instructions for local use:  

1) clone code @ https://github.com/gingsmith/ALS_debug.git  
2) get data from: http://www.cs.berkeley.edu/~vsmith/data/netflix_randSplit1_data.txt  
3) run bad_scaling.sh, passing in [master jars sparkhome trainfile] as appropriate  
4) run bad_memory.sh, passin in [master jars sparkhome trainfile] as appropriate  

** may want to manually change "nsplits"  