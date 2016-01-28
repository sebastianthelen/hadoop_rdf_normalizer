#!/bin/bash
hadoop fs -test -d $1-skolemized
if [ $? == 0 ];
then
    echo "directory" $1"-skolemized already exists"
    exit 1
fi
    
hadoop fs -test -d $1-subrepl
if [ $? == 0 ];
then
    echo "directory" $1"-subrepl already exists"
    exit 1
fi

hadoop fs -test -d $1-obj
if [ $? == 0 ];
then
    echo "directory" $1"-obj already exists"
    exit 1
fi

hadoop fs -test -d $1-filtered
if [ $? == 0 ];
then
    echo "directory" $1"-filtered already exists"
    exit 1
fi

hadoop fs -test -d $1-objrepl
if [ $? == 0 ];
then
    echo "directory" $1"-objrepl already exists"
    exit 1
fi

hadoop fs -test -d $1-normalized-1
if [ $? == 0 ];
then
    echo "directory" $1"-normalized-1 already exists"
    exit 1
fi

hadoop fs -test -d $1-normalized-bnodes
if [ $? == 0 ]; 
then
    echo "directory" $1"-normalized-bnodes already exists"
    exit 1
fi

hadoop fs -test -d $1-normalized
if [ $? == 0 ];
then
    echo "directory" $1"-normalized already exists"
    exit 1
fi

echo "*** Starting normalization ... ***"

hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/bnode_skolemization.py -file /applications/hadoop/users/hduser/rdf_normalizer/bnode_skolemization.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/bnode_skolemization.sh' -input $1 -output $1-skolemized
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/reducer.py -file /applications/hadoop/users/hduser/rdf_normalizer/mapper.py -file /applications/hadoop/users/hduser/rdf_normalizer/mapper.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/mapper.sh -m subject' -file /applications/hadoop/users/hduser/rdf_normalizer/reducer.sh -reducer '/applications/hadoop/users/hduser/rdf_normalizer/reducer.sh -m subject' -input $1-skolemized -output $1-subrepl
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/rdf_filter.py -file /applications/hadoop/users/hduser/rdf_normalizer/rdf_filter.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/rdf_filter.sh' -input $1-subrepl -output $1-obj
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/rdf_filter.py -file /applications/hadoop/users/hduser/rdf_normalizer/rdf_filter_inverse.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/rdf_filter_inverse.sh' -input $1-subrepl -output $1-filtered
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/reducer.py -file /applications/hadoop/users/hduser/rdf_normalizer/mapper.py -file /applications/hadoop/users/hduser/rdf_normalizer/mapper.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/mapper.sh -m object' -file /applications/hadoop/users/hduser/rdf_normalizer/reducer.sh -reducer '/applications/hadoop/users/hduser/rdf_normalizer/reducer.sh -m object' -input $1-obj -output $1-objrepl
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/bnode_skolemization.py -file /applications/hadoop/users/hduser/rdf_normalizer/undo_bnode_skolemization.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/undo_bnode_skolemization.sh' -input $1-objrepl -output $1-normalized-1
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/bnode_skolemization.py -file /applications/hadoop/users/hduser/rdf_normalizer/undo_bnode_skolemization.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/undo_bnode_skolemization.sh' -input $1-filtered -output $1-normalized-2
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=1 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/filter.py -file /applications/hadoop/users/hduser/rdf_normalizer/filter_inverse.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/filter_inverse.sh' -input $1-normalized-1 -input $1-normalized-2 -output $1-normalized-bnodes
hadoop jar $HADOOP_HOME/hadoop-streaming-2.7.1.jar -D mapred.reduce.tasks=$2 -D mapred.reduce.slowstart.completed.maps=$3 -file /applications/hadoop/users/hduser/rdf_normalizer/filter.py -file /applications/hadoop/users/hduser/rdf_normalizer/filter.sh -mapper '/applications/hadoop/users/hduser/rdf_normalizer/filter.sh' -input $1-normalized-1 -input $1-normalized-2 -output $1-normalized

echo "*** Normalization done. ***"
