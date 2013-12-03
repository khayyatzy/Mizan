part=11

hadoop dfs -rmr output1 output2 output3 output4
hadoop jar metisFormat preMetisGraphFix_hadoop input/$1 output1
hadoop jar metisFormat preMetisCountAppend_hadoop output1 output2 output3

rm output/*
hadoop dfs -get output3/part-r-00000 output/part-r-00000
/home/khayyzy/jars/metis-4.0.3/pmetis $PWD/output/part-r-00000 $part
hadoop dfs -rm input/part-r-00000.part.*
hadoop dfs -put output/part-r-00000.part.$part input/part-r-00000.part.$part

hadoop jar metisFormat postMetisSubGraphAssignID_hadoop input/part-r-00000.part.$part output4
