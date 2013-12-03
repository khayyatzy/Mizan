/*
 * readStatus.cpp
 *
 *  Created on: Mar 25, 2012
 *      Author: khayyzy
 */

#ifndef DATAGENERAL_H_
#define DATAGENERAL_H_

enum enableVTH {
	noneEnable, EnableWithMessages, EnableAll,
};

enum distType {
	hashed, minCuts, range,
};

enum fileStatus {
	success, fail_fileNotFound, fail_fileSystemNotReachable, fail_unknown,
};

enum migrationMode{
	NONE,DelayMigrationOnly,MixMigration,PregelWorkStealing,
};
enum fileSystem {
	HDFS, OS_DISK_MASTER, OS_DISK_ALL, NOFS
};
enum edgeStorage {
	InOutNbrStore, InNbrStore, OutNbrStore, NoneNbrStore
};

enum computeStatus {
	completed, running, inComplete, interrupted, failed,
};
enum aggregatorScope {
	local, gobal,
};
enum graphMutateCommands {
	DeleteVertex,
	DeleteVertexInform,
	DeleteOutEdge,
	DeleteInEdge,
	AddVertex,
	AddOutEdge,
	AddInEdge,
};
struct subGraphSource {
	fileSystem fileSystemType;
	char * filePath;
};
template<class K, class M>
struct messageStore {
	K key;
	M value;
};
template<class K>
struct graphMutatorStore {
	graphMutateCommands cmd;
	K req;
	K target;
};
#endif
