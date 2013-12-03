if [ $# -ne 4 ]
then
	echo $?
	echo "Command error, input format = $0 [graph_name in "hdfs:input/"] [partition size] [duplicate outDegree] [MPI processors]"
	exit -1
fi

file_name=metisInput_$1
file_loc=/tmp/$file_name

hadoop dfsadmin -safemode wait

hadoop dfs -rmr mizan_output*

echo "**** Running preMGraphFix stage ****"
hadoop jar preMizan.jar prePartition.preMetisGraphFix_hadoop input/$1 mizan_output1

if [ $? -ne 0 ]
then
	echo "**** Something wrong with stage preMGraphFix ****"
	exit -1
fi

echo "**** Running preMCountAppend & preMFixEmptyVertex stages ****"
hadoop jar preMizan.jar prePartition.preMetisCountAppend_hadoop mizan_output1 mizan_output2 mizan_output3

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stages preMCountAppend & preMFixEmptyVertex ****"
        exit -1
fi

echo "**** Running ParMetis Stage ****" 
./parallelMetis $file_loc $2 mizan_output3/part-r-00000 $4

if [ $? -ne 0 ]
then
        echo "**** Something wrong with ParMetis ****"
        exit -1 
fi


hadoop dfs -rm mizan_tmp/$file_name
hadoop dfs -put $file_loc.part.$2 mizan_tmp/$file_name

echo "**** Running postMVertexAssignID Stage ****"
hadoop jar preMizan.jar postPartition.postMetisVertexAssignID_hadoop mizan_tmp/$file_name mizan_output4

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage postMVertexGAssignID ****"
        exit -1 
fi


echo "**** Running postMetisMatchVerPart Stage ****"
hadoop jar preMizan.jar postPartition.postMetisMatchVertexPartition_hadoop input/$1 mizan_output4 mizan_output5

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage postMetisMatchVerPart ****"
        exit -1
fi

echo "**** Running groupSubGraph Stage ****"
hadoop dfs -rmr m_output/mizan_$1_minc_$2
hadoop jar preMizan.jar postPartition.groupSubGraph_hadoop $2 mizan_output5_b m_output/mizan_$1_minc_$2 $3

if [ $? -ne 0 ]
then
        echo "**** Something wrong with stage groupSubGraph ****"
        exit -1
fi
