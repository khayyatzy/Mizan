/*
 * general.h
 *
 *  Created on: Jun 13, 2012
 *      Author: refops
 */

#ifndef GENERALMIZAN_H_
#define GENERALMIZAN_H_
#include <map>

#include "IAggregator.h"
#include "computation/systemWideInfo.h"
#include "communication/sysComm.h"
#include "boost/thread.hpp"
#include "dataManager/dataStructures/general.h"

template<class K, class V1, class M, class A> class sysComm;
template<class K, class V1, class M, class A> class userComm;

template<class K, class V1, class M, class A>
struct systemDataPointer {
	std::map<char *, IAggregator<A> *> aggContainer;
	boost::mutex aggContainerLock;
	systemWideInfo<K> sysInfo;
	sysComm<K, V1, M, A> * sc;
	userComm<K, V1, M, A> * uc;
};

struct MizanArgs {
	int algorithm;
	int clusterSize;
	std::string graphName;
	fileSystem fs;
	distType partition;
	std::string hdfsUserName;
	migrationMode migration;
	communicationType communication;
	int superSteps;
};

#endif /* GENERALMIZAN_H_ */
