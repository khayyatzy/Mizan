if [ $# -ne 5 ]
then
	echo $?
	echo "Command error, input format = $0 [graph_name in "hdfs:input/"] [partition size] [partition_factor] [duplicate outDegree] [MPI_PROCESSORS]"
	exit -1
fi

file_name=metisInput_block_$1
file_loc=/tmp/$file_name
innerPart=$(($2*$3))
echo $innerPart

echo "**** Phase1: running modHash on input graph ****"
./hadoop_run_modhash.sh $1 $innerPart false

if [ $? -ne 0 ]
then
        echo "**** Something wrong with phase modHash ****"
        exit -1
fi

echo "**** Phase2: running semiBlock partitioning on modHash output ****"
echo "**** Running semiBlockPartitioning stage ****"
hadoop jar preMizan.jar partitioning.semiBlockPartitioning_hadoop 3 m_output/mizan_$1_mhash_$innerPart mizan_output

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage semiBlockPartitioning ****"
        exit -1
fi

echo "**** Running ParMetis Stage ****" 
./parallelMetis $file_loc $2 mizan_output6/part-r-00000 $5

if [ $? -ne 0 ]
then
        echo "**** Something wrong with ParMetis ****"
        exit -1 
fi

hadoop dfs -rm mizan_tmp/$file_name
hadoop dfs -rmr m_output/mizan_$1_block_$2
hadoop dfs -put $file_loc.part.$2 mizan_tmp/$file_name

echo "**** Running semiBlockPostPartitioning stage ****"
hadoop jar preMizan.jar postPartition.semiBlockPostPartitioning_hadoop 7 $2 m_output/mizan_$1_mhash_$innerPart mizan_tmp/$file_name mizan_output m_output/mizan_$1_block_$2 $4

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage semiBlockPostPartitioning ****"
        exit -1
fi

