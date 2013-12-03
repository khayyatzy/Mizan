mkdir tmp
rm -rf tmp/*
javac -cp ${HADOOP_HOME}/hadoop-core-1.0.0.jar -d tmp src/modClasses/* src/postPartition/* src/prePartition/* src/partitioning/* src/statistics/* src/graphFormat/*
jar -cvf metisFormat -C tmp/ .

cp metisFormat ~/bin/preMizan.jar
#scp metisFormat clouduser@10.70.42.23:/home/clouduser/zuhair/mizan/hadoop_part/preMizan.jar
scp metisFormat khayyzy@office:bin/preMizan.jar 
scp metisFormat khayyat@10.70.42.23:bin/preMizan.jar
