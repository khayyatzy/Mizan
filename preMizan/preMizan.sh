if [ $# -ne 2 ] && [ $# -ne 3 ]
then
        echo "Command error, input format = $0 [input graph] [cluster/partition size] [OPTINAL: Vertex weight count -- default = 0]"
        exit -1
fi

weightCnt=0
if [ $# -eq 3 ]
then
    weightCnt=$3
fi

inputFile=$(basename $1)

hadoop dfsadmin -safemode wait
hadoop dfs -rm input/$inputFile
hadoop dfs -put $1 input/$inputFile

echo "Select your partitioning type:"
    echo "   1) Hash Graph Partitioning"
    echo "   2) Range Graph Partitioning"
while true; do
    read -p "" yn
    case $yn in
        [1]* ) cd hadoopScripts; ./hadoop_run_modhash.sh $inputFile $2 true $weightCnt ; break;;
        [2]* ) cd hadoopScripts; ./hadoop_run_range.sh $inputFile $2 true $weightCnt ; break;;
        * ) echo "Please answer by typing 1 or 2.";;
    esac
done
