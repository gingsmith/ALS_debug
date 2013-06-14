# run bals, graphlab, and spark join als

# ----------------- rank 10

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated
--rank=10 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank10

mpiexec -env CLASSPATH $(~/ephemeral-hdfs/bin/hadoop classpath) -n 25 /mnt/graphlabapi/release/toolkits/collaborative_filtering/als   --ncpus=8 --lambda=0.1 --tol=0.0 --max_iter=10 --D=10 --interval=1000000000  --matrix hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated > 25x_graphlab_rank10

sbt/sbt "run-main als_debug.BlockedALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt
--rank=10 
--niter=10
--nsplits=25
--repfact=5
--m=10677
--n=69878" 2>&1 | tee 25_blocked_ALS_rank10

# ----------------- rank 20

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated
--rank=20 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank20

mpiexec -env CLASSPATH $(~/ephemeral-hdfs/bin/hadoop classpath) -n 25 /mnt/graphlabapi/release/toolkits/collaborative_filtering/als   --ncpus=8 --lambda=0.1 --tol=0.0 --max_iter=10 --D=20 --interval=1000000000  --matrix hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated > 25x_graphlab_rank20

sbt/sbt "run-main als_debug.BlockedALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt
--rank=20 
--niter=10
--nsplits=25
--repfact=5
--m=10677
--n=69878" 2>&1 | tee 25_blocked_ALS_rank20

# ----------------- rank 30

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated
--rank=30 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank30

mpiexec -env CLASSPATH $(~/ephemeral-hdfs/bin/hadoop classpath) -n 25 /mnt/graphlabapi/release/toolkits/collaborative_filtering/als   --ncpus=8 --lambda=0.1 --tol=0.0 --max_iter=10 --D=30 --interval=1000000000  --matrix hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated > 25x_graphlab_rank30

sbt/sbt "run-main als_debug.BlockedALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt
--rank=30 
--niter=10
--nsplits=25
--repfact=5
--m=10677
--n=69878" 2>&1 | tee 25_blocked_ALS_rank30

# ----------------- rank 40

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated
--rank=40 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank40

mpiexec -env CLASSPATH $(~/ephemeral-hdfs/bin/hadoop classpath) -n 25 /mnt/graphlabapi/release/toolkits/collaborative_filtering/als   --ncpus=8 --lambda=0.1 --tol=0.0 --max_iter=10 --D=40 --interval=1000000000  --matrix hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated > 25x_graphlab_rank40

sbt/sbt "run-main als_debug.BlockedALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt
--rank=40 
--niter=10
--nsplits=25
--repfact=5
--m=10677
--n=69878" 2>&1 | tee 25_blocked_ALS_rank40

# ----------------- rank 10

sbt/sbt "run-main als_debug.Broadcast_ALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated
--rank=50 
--niter=10
--m=53385
--n=349390" 2>&1 | tee 25x_broadcast_ALS_rank50

mpiexec -env CLASSPATH $(~/ephemeral-hdfs/bin/hadoop classpath) -n 25 /mnt/graphlabapi/release/toolkits/collaborative_filtering/als   --ncpus=8 --lambda=0.1 --tol=0.0 --max_iter=10 --D=50 --interval=1000000000  --matrix hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt_replicated > 25x_graphlab_rank50

sbt/sbt "run-main als_debug.BlockedALS
--big=true
--master=spark://ec2-54-226-230-133.compute-1.amazonaws.com:7077
--jars=/mnt/ALS_debug/target/als_debug-assembly-1.0.jar
--sparkhome=/root/spark
--train=hdfs://ec2-54-226-230-133.compute-1.amazonaws.com:9000/data/movielens_10m_randSplit1_data.txt
--rank=50 
--niter=10
--nsplits=25
--repfact=5
--m=10677
--n=69878" 2>&1 | tee 25_blocked_ALS_rank50