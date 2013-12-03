//============================================================================
// Name        : Mizan.cpp
// Author      : Zuhair Khayyat
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#define Verbose 1

#include "Mizan.h"
#include "Mizan.cpp"
#include "dataManager/dataManager.h"

#include "unistd.h"
#include <stdio.h>
#include <stdlib.h>
#include "unitTest.h"
#include "algorithms/dimEst.h"
#include "algorithms/pageRank.h"
#include "algorithms/pageRankTopK.h"
#include "algorithms/AdSim.h"
#include "tools/argParser.h"
#include "algorithms/maxAggregator.h"
#include "general.h"

using namespace std;

int main(int argc, char** argv) {

	argParser argp;
	MizanArgs myArgs = argp.parse(argc, argv);

	char ** inputBaseFile = argp.setInputPaths(myArgs.fs, myArgs.clusterSize,
			myArgs.graphName, myArgs.hdfsUserName, myArgs.partition);

#ifdef Verbose
	time_t begin_time = time(NULL);
#endif

	bool groupVoteToHalt;
	edgeStorage storageType;
	distType partType;

	int myWorkerID;

	if (myArgs.algorithm == 1) {
		groupVoteToHalt = true;
		storageType = OutNbrStore;
		pageRank us(myArgs.superSteps);
		pageRankCombiner prc;

		Mizan<mLong, mDouble, mDouble, mLong> * mmk = new Mizan<mLong, mDouble,
				mDouble, mLong>(myArgs.communication, &us, storageType,
				inputBaseFile, myArgs.clusterSize, myArgs.fs, myArgs.migration);

		mmk->registerMessageCombiner(&prc);

		mmk->setVoteToHalt(groupVoteToHalt);

		/*string output;
		 output.append("/user/");
		 output.append(myArgs.hdfsUserName.c_str());
		 output.append("/m_run_output/");
		 output.append(myArgs.graphName.c_str());
		 mmk->setOutputPath(output.c_str());*/

		//User Defined aggregator
		char * maxAgg = "maxAggregator";
		maxAggregator maxi;
		mmk->registerAggregator(maxAgg, &maxi);

		mmk->run(argc, argv);

		myWorkerID = mmk->getPEID();

		delete mmk;

	} else if (myArgs.algorithm == 2) {
		groupVoteToHalt = false;
		storageType = OutNbrStore;
		pageRankTopK prt(5, 10, myArgs.superSteps);

		Mizan<mLong, mDoubleArray, mDouble, mLong> * mmk = new Mizan<mLong,
				mDoubleArray, mDouble, mLong>(myArgs.communication, &prt,
				storageType, inputBaseFile, myArgs.clusterSize, myArgs.fs,
				myArgs.migration);
		mmk->setVoteToHalt(groupVoteToHalt);

		mmk->run(argc, argv);
		myWorkerID = mmk->getPEID();
		delete mmk;

	} else if (myArgs.algorithm == 3) {
		groupVoteToHalt = true;
		storageType = InNbrStore;
		dimEst dE(myArgs.superSteps);

		Mizan<mLong, mLongArray, mLongArray, mLong> * mmk = new Mizan<mLong,
				mLongArray, mLongArray, mLong>(myArgs.communication, &dE,
				storageType, inputBaseFile, myArgs.clusterSize, myArgs.fs,
				myArgs.migration);
		mmk->setVoteToHalt(groupVoteToHalt);

		mmk->run(argc, argv);
		myWorkerID = mmk->getPEID();
		delete mmk;

	} else if (myArgs.algorithm == 4) {
		groupVoteToHalt = false;
		storageType = InOutNbrStore;
		AdSim alg(myArgs.superSteps);

		Mizan<mLong, mLong, mLongArray, mLong> * mmk = new Mizan<mLong, mLong,
				mLongArray, mLong>(myArgs.communication, &alg, storageType,
				inputBaseFile, myArgs.clusterSize, myArgs.fs, myArgs.migration);
		mmk->setVoteToHalt(groupVoteToHalt);

		mmk->run(argc, argv);
		myWorkerID = mmk->getPEID();
		delete mmk;
	}

#ifdef Verbose
	if (myWorkerID == 0) {
		std::cout << "-----TIME: Total Running Time = "
				<< float(time(NULL) - begin_time) << std::endl;
	}
#endif

	if (myWorkerID == 0) {
		cout << "!!!bye bye -- terminating Mizan, see you later!!!" << endl;
	}

	return 0;
}
