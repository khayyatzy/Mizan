if [ $# -ne 3 ]
then
	echo $?
	echo "Command error, input format = $0 [graph_name in "hdfs:input/"] [partition size] [duplicate outDegree]"
	exit -1
fi

hadoop dfsadmin -safemode wait

hadoop dfs -rmr mizan_output*

echo "**** Running modHashPartitioning ****"
hadoop jar preMizan.jar partitioning.modHashPartitioning_hadoop $2 input/$1 mizan_output1

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage modHashPartitioning ****"
        exit -1
fi

echo "**** Running postMetisMatchVerPart Stage ****"
hadoop jar preMizan.jar postPartition.postMetisMatchVertexPartition_hadoop input/$1 mizan_output1 mizan_output2

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage postMetisMatchVerPart ****"
        exit -1
fi

echo "**** Running groupSubGraph Stage ****"
hadoop dfs -rmr m_output/mizan_$1_mhash_$2
hadoop jar preMizan.jar postPartition.groupSubGraph_hadoop $2 mizan_output2_b m_output/mizan_$1_mhash_$2 $3

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage groupSubGraph ****"
        exit -1
fi
