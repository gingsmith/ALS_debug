#!/bin/bash

set -e

# assumes you're already on the cluster, have checked out code into mnt/

# assemble the code
sbt/sbt assembly

# get jblas
cd ~/../mnt/
git clone git://github.com/mikiobraun/jblas.git
cd jblas
git checkout jblas-1.2.3

# install atlas, gfortran
yum install -y atlas-sse3-devel
yum install -y gcc-gfortran
~/ephemeral-hdfs/bin/slaves.sh yum install -y atlas-sse3-devel
~/ephemeral-hdfs/bin/slaves.sh yum install -y gcc-gfortran
~/ephemeral-hdfs/bin/slaves.sh yum install -y libgfortran

# make jar
./configure --libpath=/usr/lib64/atlas-sse3:/usr/lib64:/usr/lib:/lib --download-lapack
make
ant jar

# replace old jar
cp jblas-1.2.3-SNAPSHOT.jar /root/.ivy2/cache/org.scalanlp/jblas/jars/jblas-1.2.1.jar

cd ../ALS_debug

sbt/sbt assembly