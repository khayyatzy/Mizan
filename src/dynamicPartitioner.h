/*
 * dynamicPartitioner.h
 *
 *  Created on: Aug 7, 2012
 *      Author: refops
 */

#ifndef DYNAMICPARTITIONER_H_
#define DYNAMICPARTITIONER_H_

#include <vector>
#include "dataManager/dataManager.h"
#include "computation/computeManager.h"

template<class K, class V1, class M, class A> class computeManager;
template<class K, class V1, class M, class A> class dataManager;
template<class K, class V1, class M, class A>
class dynamicPartitioner {
	dataManager<K, V1, M, A> * dataManagerPtr;
	computeManager<K, V1, M, A> * computeManagerPtr;
	int myRank;
	double imbalanceZ;
public:
	dynamicPartitioner(dataManager<K, V1, M, A> * inDataManagerPtr,
			computeManager<K, V1, M, A> * inComputeManagerPtr, int inMyRank, double inZ) :
			dataManagerPtr(inDataManagerPtr), computeManagerPtr(
					inComputeManagerPtr), myRank(inMyRank) {
		imbalanceZ = inZ;
	}
	bool testForImbalance(std::map<int, long long> * timeMap, double &average) {
		double timeAve = 0;
		long long maxTime = 0;
		for (int i = 0; i < timeMap->size(); i++) {
			timeAve = timeAve + timeMap->at(i);
			maxTime = max(maxTime,timeMap->at(i));
		}
		timeAve = timeAve / (double) timeMap->size();
		average = timeAve;

		double tmp = 0;
		double timeStdv = 0;
		for (int i = 0; i < timeMap->size(); i++) {
			tmp = pow(timeAve - timeMap->at(i), 2);
			timeStdv = timeStdv + tmp;
		}
		timeStdv = timeStdv / (double) timeMap->size();
		timeStdv = sqrt(timeStdv);

		double zTime = 0;
		for (int i = 0; i < timeMap->size(); i++) {
			zTime = abs(
					(double) (((double) timeMap->at(i)) - maxTime)//timeAve)
							/ (double) timeStdv);
			if (zTime > imbalanceZ) {
				return true;
			}
		}
		return false;
	}
	int partitionMode(std::map<int, long long> * timeMap,
			std::map<int, long long> * networkMap) {

		//processing time
		//processing network

		vector<long long> timeVec;
		double timeAve = 0;

		vector<long long> networkVec;
		double networkAve = 0;

		for (int i = 0; i < timeMap->size(); i++) {
			timeAve = timeAve + timeMap->at(i);
			timeVec.push_back(timeMap->at(i));

			/*if (myRank == 0) {
			 cout << i << "-" << timeMap->at(i) << "-" << networkMap->at(i)
			 << std::endl;
			 }*/

			networkAve = networkAve + networkMap->at(i);
			networkVec.push_back(networkMap->at(i));
		}
		sort(timeVec.begin(), timeVec.end());
		timeAve = timeAve / timeVec.size();

		sort(networkVec.begin(), networkVec.end());
		networkAve = networkAve / networkVec.size();

		/*if (myRank == 0) {
		 cout << "timeAve = " << timeAve << " networkAve " << networkAve
		 << std::endl;
		 }*/

		double timeStdv = 0;

		double networkStdv = 0;

		double tmp;
		for (int i = 0; i < timeVec.size(); i++) {
			tmp = pow(timeAve - timeVec[i], 2);
			timeStdv = timeStdv + tmp;

			tmp = pow(networkAve - networkVec[i], 2);
			networkStdv = networkStdv + tmp;
		}
		timeStdv = timeStdv / ((float) (timeVec.size()));
		timeStdv = sqrt(timeStdv);

		networkStdv = networkStdv / ((float) (networkVec.size()));
		networkStdv = sqrt(networkStdv);

		/*if (myRank == 0) {
		 cout << "timeStdv = " << timeStdv << " networkStdv " << networkStdv
		 << std::endl;
		 }*/

		//check for vector similarity
		long long timePtr;
		long long networkPtr;
		/*int timeLoc;
		 int networkLoc;*/
		std::set<int> timeLocPos;
		std::set<int> timeLocNeg;
		std::set<int> networkLocPos;
		std::set<int> networkLocNeg;

		std::set<int> timeLoc;
		std::set<int> networkLoc;

		double zTime;
		double zNetwork;
		//double z = 0.30;
		int simClusterCnt = 0;
		int simClusterSum = 0;

		int simSortCnt = 0;
		int simSortSum = 0;

		for (int i = 0; i < timeVec.size(); i++) {

			//group by Z value
			timePtr = timeMap->at(i);
			networkPtr = networkMap->at(i);

			zTime = (double) (timePtr - timeAve) / (double) timeStdv;
			zNetwork = (double) (networkPtr - networkAve)
					/ (double) networkStdv;

			if (zTime > imbalanceZ) {
				timeLocPos.insert(i);
			} else if (zTime < -imbalanceZ) {
				timeLocNeg.insert(i);
			}

			if (abs(zTime) > imbalanceZ) {
				if (zNetwork > imbalanceZ) {
					networkLocPos.insert(i);
				} else if (zNetwork < -imbalanceZ) {
					networkLocNeg.insert(i);
				}
			}

			//sort by network and finish time
			timeLoc.clear();
			networkLoc.clear();

			for (int j = 0; j < timeVec.size(); j++) {
				if (timeVec[j] == timePtr) {
					timeLoc.insert(j);
				}
				if (networkVec[j] == networkPtr) {
					networkLoc.insert(j);
				}
			}

			bool isSubSet = false;
			set<int>::iterator it;
			set<int>::iterator it2;

			for (it = timeLoc.begin(); it != timeLoc.end(); it++) {
				if (((it2 = networkLoc.find((*it))) != networkLoc.end())) {
					isSubSet = true;
				}
			}

			zTime = (double) abs(timePtr - timeAve) / (double) timeStdv;
			zNetwork = (double) abs(networkPtr - networkAve)
					/ (double) networkStdv;

			//cout << "zTime = " << zTime << std::endl;
			//cout << "zNetwork = " << zNetwork << std::endl;
			if (zTime > imbalanceZ && zNetwork > imbalanceZ) {
				if (isSubSet) {
					simSortCnt++;
				}
				simSortSum++;
			} else if (zTime > imbalanceZ || zNetwork > imbalanceZ) {
				simSortSum++;
			}
		}

		set<int>::iterator it;
		set<int>::iterator it2;

		for (it = timeLocPos.begin(); it != timeLocPos.end(); it++) {
			if (((it2 = networkLocPos.find((*it))) != networkLocPos.end())) {
				simClusterCnt++;
			}
			simClusterSum++;
		}
		for (it = timeLocNeg.begin(); it != timeLocNeg.end(); it++) {
			if (((it2 = networkLocNeg.find((*it))) != networkLocNeg.end())) {
				simClusterCnt++;
			}
			simClusterSum++;
		}

		if (myRank == 0) {
			cout << "simClusterCnt = " << simClusterCnt << " simClusterSum = "
					<< simClusterSum << std::endl;
			cout << "simSortCnt = " << simSortCnt << " simSortSum = "
					<< simSortSum << std::endl;
		}

		double clusterBalance = (double) simClusterCnt / (double) simClusterSum;

		double sortBalance = (double) simSortCnt / (double) simSortSum;

		if (clusterBalance >= 0.5 && sortBalance >= 0.5
				&& simClusterSum > (timeMap->size() / 4)
				&& simSortSum > (timeMap->size() / 4)) {
			//high correlation
			return 2;
		} else if ((clusterBalance > 0.5
				&& simClusterSum > (timeMap->size() / 4))
				|| (sortBalance > 0.5 && simSortSum > (timeMap->size() / 4))) {
			//week correlation
			return 1;
		}
		//no correlation
		return 0;
	}

	int findPEPairLong(std::map<int, long long> * sample,
			std::set<int> * ignoreList, double average) {
		vector<long long> times;
		//cout << "sample->size() = " << sample->size() << std::endl;

		for (int i = 0; i < sample->size(); i++) {

			if (sample->at(i) < average
					&& ignoreList->find(i) == ignoreList->end()) {
				times.push_back(sample->at(i));
			}
		}

		if(times.size()==0){
			return myRank;
		}
		sort(times.begin(), times.end());

		int myTime = sample->at(myRank);
		int timePos = myRank;
		int error = 0;
		for (int i = 0; i < sample->size(); i++) {
			if (times[i] == myTime) {
				timePos = ((sample->size() - 1) - i);
				//cout << "PE" << myRank << " got timePos = " << timePos << std::endl;
				break;
			}
		}
		int pairPos = myRank;
		int myTime2 = times[timePos];
		for (int i = 0; i < sample->size(); i++) {
			if (sample->at(i) == myTime2) {
				pairPos = i;
				//cout << "PE" << myRank << " got pairPos = " << pairPos << std::endl;
				break;
			}
		}

		return pairPos;
	}
	void findCandidateExecTime(int maxMin, float vertexZ, double timeDiff,
			long long outDiff, long long inDiff, int dst, double mean) {
		dataManagerPtr->lockDataManager();
		double stdv = 0;
		double tmp;
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			tmp = pow(mean - tmpObj->getSSResTime(), 2);
			//cout << " tmpObj->getSSResTime() = " << tmpObj->getSSResTime() << std::endl;
			stdv = stdv + tmp;
		}
		stdv = stdv / ((double) (dataManagerPtr->vertexSetSize()));
		stdv = sqrt(stdv);
		double reqZ = vertexZ * (float) maxMin;
		//cout << "PE " << myRank << " reqZ = " << reqZ << " stdv = " << stdv << " mean " << mean << std::endl;
		double vz;
		long long sumOutMsg = 0;
		long long sumInMsg = 0;
		double sumTime = 0;
		int countNodes = 0;
		double comm = 0;
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			if (!tmpObj->isHalted()) {
				vz = (tmpObj->getSSResTime() - mean) / stdv;
				//cout << "PE " << myRank << " vz = " << vz << std::endl;
				/*comm = 0.75 * ((double) tmpObj->getMessageCountGlobal())
				 - ((double) tmpObj->getMessageCountLocal());*/
				//cout << " comm = " << comm << endl;
				if (reqZ < vz && (!tmpObj->isHalted())
						&& (timeDiff - sumTime - tmpObj->getSSResTime())
								> (-1)) {
					countNodes++;
					sumTime = sumTime + tmpObj->getSSResTime();
					sumOutMsg = sumOutMsg + tmpObj->getOutGlobal();
					sumInMsg = sumInMsg + tmpObj->getInTotal();
					//sumInMsg = sumInMsg + tmpObj->getInGlobal();
					dataManagerPtr->getVertexObjByPos(i)->setMigrationMark();
					dataManagerPtr->copyVertexToSoftDynamicContainer(tmpObj,
							dst);
				} else {
					dataManagerPtr->getVertexObjByPos(i)->resetMigrationMark();
				}
			}
			if (sumTime > timeDiff) {
				break;
			}
		}
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			if (!tmpObj->isHalted() && !tmpObj->isMigrationMarked()) {

				countNodes++;
				sumOutMsg = sumOutMsg + tmpObj->getOutGlobal();
				sumInMsg = sumInMsg + tmpObj->getInTotal();
				//sumInMsg = sumInMsg + tmpObj->getInGlobal();
				dataManagerPtr->getVertexObjByPos(i)->setMigrationMark();
				dataManagerPtr->copyVertexToSoftDynamicContainer(tmpObj, dst);
			}

			if (sumOutMsg > outDiff || sumInMsg > inDiff) {
				break;
			}
		}
		std::cout << "PE" << this->myRank << " want to transfer " << countNodes
				<< " nodes with TD = " << (timeDiff - sumTime) << "-"
				<< (outDiff - sumOutMsg) << "-" << (inDiff - sumInMsg) << " to "
				<< dst << " original timeDIff = " << timeDiff << std::endl;
		dataManagerPtr->unlockDataManager();
	}
	void findCandidateMessageInComm(int maxMin, float vertexZ, long long diff,
			int dst, float mean) {
		dataManagerPtr->lockDataManager();

		float stdv = 0;
		float tmp;
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			tmp = pow(mean - tmpObj->getInTotal(), 2);
			//tmp = pow(mean - tmpObj->getInGlobal(), 2);
			stdv = stdv + tmp;
		}
		stdv = stdv / ((float) (dataManagerPtr->vertexSetSize()));
		stdv = sqrt(stdv);
		float reqZ = vertexZ * (float) maxMin;
		//cout << "PE " << myRank << " reqZ = " << reqZ << " stdv = " << stdv << std::endl;
		float vz;
		long long sumComm = 0;
		int countNodes = 0;
		double comm = 0;
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			if (!tmpObj->isHalted()) {
				vz = (tmpObj->getInTotal() - mean) / stdv;
				//vz = (tmpObj->getInGlobal() - mean) / stdv;
				//cout << "PE " << myRank << " vz = " << vz << std::endl;
				/*	comm = 0.75 * ((double) tmpObj->getMessageCountGlobal())
				 - ((double) tmpObj->getMessageCountLocal());*/
				//cout << " comm = " << comm << endl;
				if (reqZ < vz && (!tmpObj->isHalted())
						&& (diff - sumComm - tmpObj->getInTotal()) > (-1)) {
					countNodes++;
					sumComm = sumComm + tmpObj->getInTotal();
					//sumComm = sumComm + tmpObj->getInGlobal();
					dataManagerPtr->getVertexObjByPos(i)->setMigrationMark();
					dataManagerPtr->copyVertexToSoftDynamicContainer(tmpObj,
							dst);
				} else {
					dataManagerPtr->getVertexObjByPos(i)->resetMigrationMark();
				}
			}
			if (sumComm > diff) {
				break;
			}
		}
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			if (!tmpObj->isHalted() && !tmpObj->isMigrationMarked()
					&& tmpObj->getInTotal() > 0
					//&& tmpObj->getInGlobal() > 0
					&& (diff - sumComm - tmpObj->getInTotal()) > (-1)) {

				countNodes++;
				sumComm = sumComm + tmpObj->getInTotal();
				//sumComm = sumComm + tmpObj->getInGlobal();
				dataManagerPtr->getVertexObjByPos(i)->setMigrationMark();
				dataManagerPtr->copyVertexToSoftDynamicContainer(tmpObj, dst);
			}

			if (sumComm > diff) {
				break;
			}
		}
		std::cout << "PE" << this->myRank << " want to transfer " << countNodes
				<< " nodes with TD = " << (diff - sumComm) << " to " << dst
				<< " original diff = " << diff << std::endl;

		dataManagerPtr->unlockDataManager();
	}
	void findCandidateMessageOutGLDiff(int maxMin, float vertexZ,
			long long diff, int dst, float mean) {
		dataManagerPtr->lockDataManager();

		float stdv = 0;
		float tmp;
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			//tmp = pow(mean - tmpObj->getOutDiff(), 2);
			tmp = pow(mean - tmpObj->getOutGlobal(), 2);
			stdv = stdv + tmp;
		}
		stdv = stdv / ((float) (dataManagerPtr->vertexSetSize()));
		stdv = sqrt(stdv);
		float reqZ = vertexZ * (float) maxMin;
		//cout << "PE " << myRank << " reqZ = " << reqZ << " stdv = " << stdv << std::endl;
		float vz;
		long long sumComm = 0;
		int countNodes = 0;
		double comm = 0;
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			if (!tmpObj->isHalted()) {
				vz = (tmpObj->getOutGlobal() - mean) / stdv;
				//cout << "PE " << myRank << " vz = " << vz << std::endl;
				/*	comm = 0.75 * ((double) tmpObj->getMessageCountGlobal())
				 - ((double) tmpObj->getMessageCountLocal());*/
				//cout << " comm = " << comm << endl;
				if (reqZ < vz && (!tmpObj->isHalted())
						&& (diff - sumComm - tmpObj->getOutGlobal()) > (-1)) {
					countNodes++;
					sumComm = sumComm + tmpObj->getOutGlobal();
					dataManagerPtr->getVertexObjByPos(i)->setMigrationMark();
					dataManagerPtr->copyVertexToSoftDynamicContainer(tmpObj,
							dst);
				} else {
					dataManagerPtr->getVertexObjByPos(i)->resetMigrationMark();
				}
			}
			if (sumComm > diff) {
				break;
			}
		}
		for (int i = 0; i < dataManagerPtr->vertexSetSize(); i++) {
			mObject<K, V1, M> * tmpObj = dataManagerPtr->getVertexObjByPos(i);
			if (!tmpObj->isHalted() && !tmpObj->isMigrationMarked()
					&& tmpObj->getOutGlobal() > 0
					&& (diff - sumComm - tmpObj->getOutGlobal()) > (-1)) {

				countNodes++;
				sumComm = sumComm + tmpObj->getOutGlobal();
				dataManagerPtr->getVertexObjByPos(i)->setMigrationMark();
				dataManagerPtr->copyVertexToSoftDynamicContainer(tmpObj, dst);
			}

			if (sumComm > diff) {
				break;
			}
		}
		std::cout << "PE" << this->myRank << " want to transfer " << countNodes
				<< " nodes with TD = " << (diff - sumComm) << " to " << dst
				<< " original diff = " << diff << std::endl;

		dataManagerPtr->unlockDataManager();
	}
	bool multiGrubbsTestLong(double &msgDiff, long long &diff1,
			long long &diff2, std::map<int, long long> * sample,
			std::map<int, long long> * sample1,
			std::map<int, long long> * sample2, int dst, float globalZ) {

		float mean = 0;
		float mean1 = 0;
		float mean2 = 0;

		for (int i = 0; i < sample->size(); i++) {
			mean = mean + sample->at(i);

			mean1 = mean1 + sample1->at(i);
			mean2 = mean2 + sample2->at(i);
		}
		//cout << "PE" << myRank << " mean = " << mean << " dst = " << dst
		//<< std::endl;
		if (mean == 0 || dst == myRank) {
			return false;
		}
		mean = mean / (float) sample->size();
		mean1 = mean1 / (float) sample->size();
		mean2 = mean2 / (float) sample->size();
		//cout << "PE"<<myRank<<" mean = " << mean << " dst = " << dst<< std::endl;

		float stdv = 0;
		float tmp;
		for (int i = 0; i < sample->size(); i++) {
			tmp = pow(mean - (float) sample->at(i), 2);
			stdv = stdv + tmp;
		}
		stdv = stdv / (float) (sample->size());
		stdv = sqrt(stdv);
		msgDiff = sample->at(myRank) - ((sample->at(dst)==0)?mean:sample->at(dst)); //mean; //
		diff1 = sample1->at(myRank) - ((sample1->at(dst)==0)?mean1:sample1->at(dst)); //mean1;
		diff2 = sample2->at(myRank) - ((sample2->at(dst)==0)?mean2:sample2->at(dst)); //mean2;
		float z = msgDiff / stdv;

		//cout << "PE" << myRank << " z = " << z << std::endl;
		//<< " diff = " << diff/2 << " sample->at(myRank) = " << sample->at(myRank) << " stdv = " << stdv << endl;

		diff1 = diff1 / 2;
		diff2 = diff2 / 2;
		msgDiff = msgDiff / 2;
		if (z < globalZ) {
			return false;
		} else {
			return true;
		}

	}
	bool grubbsTestLong(long long &diff, std::map<int, long long> * sample,
			int dst, float globalZ) {
		float mean = 0;
		for (int i = 0; i < sample->size(); i++) {
			mean = mean + sample->at(i);
		}
		if (mean == 0 || dst == myRank) {
			return false;
		}
		mean = mean / (float) sample->size();
		//cout << "PE"<<myRank<<" mean = " << mean << " dst = " << dst<< std::endl;

		float stdv = 0;
		float tmp;
		for (int i = 0; i < sample->size(); i++) {
			tmp = pow(mean - (float) sample->at(i), 2);
			stdv = stdv + tmp;
		}
		stdv = stdv / (float) (sample->size());
		stdv = sqrt(stdv);
		diff = sample->at(myRank) - ((sample->at(dst)==0)?mean:sample->at(dst)); //mean; //
		float z = diff / stdv;

		//cout << "PE" << myRank << " z = " << z << std::endl;
		//<< " diff = " << diff/2 << " sample->at(myRank) = " << sample->at(myRank) << " stdv = " << stdv << endl;

		diff = diff / 2;
		if (z < globalZ) {
			return false;
		} else {
			return true;
		}

	}
	virtual ~dynamicPartitioner() {
	}
};

#endif /* DYNAMICPARTITIONER_H_ */
