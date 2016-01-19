#!/bin/bash
hadoop fs -test -d /$1-skolemized
if[ $? == 0 ] 
then
    echo "directory" $1"-skolemized already exists"
    exit 1
    
hadoop fs -test -d /$1-subrepl
if[ $? == 0 ]
then
    echo "directory" $1"-subrepl already exists"
    exit 1
hadoop fs -test -d /$1-obj
if[ $? == 0 ] 
then
    echo "directory" $1"-obj already exists"
    exit 1

hadoop fs -test -d /$1-filtered
if[ $? == 0 ] 
then
    echo "directory" $1"-filtered already exists"
    exit 1
    
hadoop fs -test -d /$1-objrepl
if[ $? == 0 ] 
then
    echo "directory" $1"-objrepl already exists"
    exit 1
hadoop fs -test -d /$1-normalized-1
if[ $? == 0 ] 
then
    echo "directory" $1"-normalized-1 already exists"
    exit 1
hadoop fs -test -d /$1-normalized-2
if[ $? == 0 ] 
then
    echo "directory" $1"-normalized-2 already exists"
    exit 1

hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/bnode_skolemization.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/bnode_skolemization.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/bnode_skolemization.sh' -input /$1 -output /$1-skolemized
hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/reducer.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/mapper.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/mapper.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/mapper.sh -m subject' -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/reducer.sh -reducer '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/reducer.sh -m subject' -input /$1-skolemized -output /$1-subrepl
hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/rdf_filter.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/rdf_filter.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/rdf_filter.sh' -input /$1-subrepl -output /$1-obj  
hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/rdf_filter.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/rdf_filter_inverse.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/rdf_filter_inverse.sh' -input /$1-subrepl /$1-filtered
hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/reducer.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/mapper.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/mapper.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/mapper.sh -m object' -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/reducer.sh -reducer '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/reducer.sh -m object' -input /$1-obj -output /$1-objrepl
hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/bnode_skolemization.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/undo_bnode_skolemization.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/undo_bnode_skolemization.sh' -input /$1-objrepl -output /$1-normalized-1
hadoop jar /applications/bigdata/users/bigdata/hadoop/hadoop-streaming-2.6.0.jar -D mapred.reduce.tasks=20 -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/bnode_skolemization.py -file /applications/bigdata/users/bigdata/hadoop_rdf_normalizer/undo_bnode_skolemization.sh -mapper '/applications/bigdata/users/bigdata/hadoop_rdf_normalizer/undo_bnode_skolemization.sh' -input /$1-filtered -output /$1-normalized-2
