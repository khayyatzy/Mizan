/*
 * compute.h
 *
 *  Created on: Apr 3, 2012
 *      Author: Zuhair Khayyat
 */

#ifndef COMPUTEMANAGER_H_
#define COMPUTEMANAGER_H_

#include "../dataManager/dataStructures/data/IdataType.h"
#include "../dataManager/dataStructures/data/mDouble.h"
#include "../dataManager/dataStructures/data/mKArray.h"
#include "../dataManager/dataStructures/data/mDoubleArray.h"
#include "../dataManager/dataStructures/data/mLongArray.h"
#include "../dataManager/dataStructures/data/mIntTagDouble.h"
#include "../dataManager/dataStructures/data/mCharArray.h"
#include "../dataManager/dataStructures/graph/mObject.h"
#include "../dataManager/dataManager.h"
#include "../communication/userComm.h"
#include "../dataManager/dataStructures/general.h"
#include "../dataManager/userVertexObject.h"
#include "../IsuperStep.h"
#include "../dataManager/dataStructures/graph/messageIterator.h"
#include "../communication/sysComm.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "systemWideInfo.h"
#include <cmath>
#include "../Icombiner.h"
#include "../IAggregator.h"
#include "../dataManager/dataStructures/data/mArrayIntTagNK.h"
#include "../dataManager/dataStructures/data/mKCharArrayPair.h"
#include "messageManager.h"
#include "../dataManager/dataStructures/graph/MessageContainer.h"
#include "../Icombiner.h"

template<class K, class V1, class M, class A> class dataManager;
template<class K, class V1, class M, class A> class IsuperStep;
template<class K, class V1, class M, class A> class userComm;
template<class K, class V1, class M, class A> class sysComm;
template<class K, class V1, class M, class A> class messageManager;

template<class K, class V1, class M, class A>
class computeManager {

private:
	dataManager<K, V1, M, A> * myDataManager;
	IsuperStep<K, V1, M, A> * userCode;

	userComm<K, V1, M, A> * uc;
	sysComm<K, V1, M, A> * sc;
	systemWideInfo<K> * sysInfo;
	int * superStepCnt;
	int migrateCnt;

	int resTime;
	int blockHaltePeCnt;
	bool blockHaltPe;
	int blockCnt;
	boost::mutex blockSSLock;

	std::map<char *, IAggregator<A> *> * aggContainer;
	std::queue<mObject<K, V1, M> *> rejectedMessageQueue;

	std::map<int, float> ssAveResTime;
	std::map<int, float> ssAveMsgQueue;

	std::queue<messageStore<K, M> > pendingMessages;

	std::queue<graphMutatorStore<K> > pendingMutations;

	messageManager<K, V1, M, A> * mmg;
	MessageContainer<K, M> mc;

	//Vertex Statistics
	double aveRunTime;
	double sumRunTime;

	int sumIO;
	float aveIO;

	//in Stats
	int sumInCommGlobal;
	float aveInCommGlobal;

	int sumInCommLocal;
	float aveInCommLocal;

	int sumInCommTotal;
	float aveInCommTotal;

	//x in Stats
	int xSumInCommGlobal;
	float xAveInCommGlobal;

	int xSumInCommLocal;
	float xAveInCommLocal;

	int xSumInCommTotal;
	float xAveInCommTotal;

	int sumOutCommGlobal;
	float aveOutCommGlobal;

	int sumOutCommLocal;
	float aveOutCommLocal;

	bool pendingMessageSync;
	bool groupVoteToHalt;

	time_t begin_time_ss;
	boost::mutex * aggLock;

	time_t appStart;

	bool withCombiner;

	Icombiner<K, V1, M, A> * cmb;

public:
	computeManager() {
		//myDataManager = inDataManager;
		cnt = 0;
	}
	computeManager(dataManager<K, V1, M, A> * inDataManager,
			sysComm<K, V1, M, A> * inSysComm,
			userComm<K, V1, M, A> * inUserComm, IsuperStep<K, V1, M, A> * inST,
			systemWideInfo<K> * inSysInfo, int * inPtrSS,
			std::map<char *, IAggregator<A> *> * inAggContainer,
			boost::mutex * inAggLock, bool inGroupVoteToHalt,
			bool inWithCombiner) {

		withCombiner = inWithCombiner;
		mmg = new messageManager<K, V1, M, A>(inWithCombiner, inUserComm, &mc);

		myDataManager = inDataManager;
		sc = inSysComm;
		uc = inUserComm;
		userCode = inST;
		sysInfo = inSysInfo;
		superStepCnt = inPtrSS;
		aggContainer = inAggContainer;
		cnt = 0;
		groupVoteToHalt = inGroupVoteToHalt;
		aggLock = inAggLock;

		xSumInCommGlobal = 0;
		xAveInCommGlobal = 0;

		xSumInCommLocal = 0;
		xAveInCommLocal = 0;

		xSumInCommTotal = 0;
		xAveInCommTotal = 0;

	}
	void storeInCommInfo() {
		xSumInCommGlobal = sumInCommGlobal;
		xAveInCommGlobal = aveInCommGlobal;

		xSumInCommLocal = sumInCommLocal;
		xAveInCommLocal = aveInCommLocal;

		xSumInCommTotal = sumInCommTotal;
		xAveInCommTotal = aveInCommTotal;
	}
	virtual ~computeManager() {
		delete mmg;
	}

	void setCombiner(Icombiner<K, V1, M, A> * inCmb) {
		cmb = inCmb;
	}
	time_t getAppStart() {
		return appStart;
	}
	time_t getSSBegin() {
		return begin_time_ss;
	}

	int getSSResTime() {
		return resTime;
	}
	double getVertexSumResTime() {
		return sumRunTime;
	}
	double getVerAveResTime() {
		return aveRunTime;
	}

	int getSumIO() {
		return sumIO;
	}

	float getAveIO() {
		return aveIO;
	}
	int getSumInCommLocal() {
		return sumInCommLocal;
	}

	float getAveInCommLocal() {
		return aveInCommLocal;
	}
	int getSumInCommGlobal() {
		return sumInCommGlobal;
	}

	float getAveInCommGlobal() {
		return aveInCommGlobal;
	}

	///////
	int getXSumInCommLocal() {
		return xSumInCommLocal;
	}

	float getXAveInCommLocal() {
		return xAveInCommLocal;
	}
	int getXSumInCommGlobal() {
		return xSumInCommGlobal;
	}

	float getXAveInCommGlobal() {
		return xAveInCommGlobal;
	}
	///////

	int getSumOutCommGlobal() {
		return sumOutCommGlobal;
	}

	float getAveOutCommGlobal() {
		return aveOutCommGlobal;
	}

	int getSumOutCommLocal() {
		return sumOutCommLocal;
	}

	float getAveOutCommLocal() {
		return aveOutCommLocal;
	}

	void init() {

		//myDataManager->initDebug();
		myDataManager->readIntoMemory();
		myDataManager->closeFile();

		/*cout << "\t-PE" << sc->getRank() << " Finished reading and closed file."
		 << endl;
		 cout << "\t-PE" << sc->getRank() << " has "
		 << myDataManager->vertexSetSize()
		 << " vertexes stored locally and "
		 << myDataManager->getCountEdges() << " edges." << endl;*/

#ifdef Verbose
		const clock_t begin_time = clock();
#endif
		for (int i = 0; i < myDataManager->vertexSetSize(); i++) {
			sc->InformOwnership(
					myDataManager->getVertexObjByPos(i)->getVertexID());
		}
#ifdef Verbose
		resTime= float(clock() - begin_time) / CLOCKS_PER_SEC;
		/*std::cout << "PE"<< sc->getRank() <<" -----TIME: sc->InformOwnership() Running Time = "
		 << resTime << std::endl;*/
#endif

		//mLong value(myDataManager->vertexSetSize());

		K * array = new K[2];
		array[0] = myDataManager->getMinVertex();
		array[1] = myDataManager->getMaxVertex();

		mArrayIntTagNK<K> value(2, myDataManager->vertexSetSize(), array);

		//cout << "\t-PE"<<sc->getRank() << " before InitVertexCount sysCommand" << endl;

		appStart = time(NULL);
		sc->BroadcastSysMessageValue(InitVertexCount, value, INSTANT_PRIORITY);

		/*cout << "\t-PE" << sc->getRank() << " total memory = "
		 << myDataManager->getTotalSystemMemory() << endl;
		 cout << "\t-PE" << sc->getRank() << " Available memory = "
		 << myDataManager->getAvaliableSystemMemory() << endl;*/

	}
	void userInit() {
#ifdef Verbose
		const clock_t begin_time = clock();
#endif

		userVertexObject<K, V1, M, A> aaa(sysInfo, aggContainer, aggLock,
				&pendingMutations);
		for (int i = 0; i < myDataManager->vertexSetSize(); i++) {
			mObject<K, V1, M> * kkk = myDataManager->getVertexObjByPos(i);
			aaa.setVertexObj(kkk);
			userCode->initialize(&aaa);
		}

		//cout << "\t-PE" << sc->getRank() << " sending FinishInit" << endl;
		aggregate();
		sc->BroadcastSysMessage(FinishInit, AFTER_DATABUFFER_PRIORITY);
		//cout << "\t-PE" << sc->getRank() << " finish sending FinishInit."
		//<< endl;

#ifdef Verbose
		resTime= float(clock() - begin_time) / CLOCKS_PER_SEC;
		if(sc->getRank()==0) {
			std::cout << "PE"<< sc->getRank() <<" -----TIME: userInit() Running Time = "
			<< resTime << std::endl;
		}

#endif

	}
	void dataMutate(graphMutateCommands& cmd, K& req, K& target) {
		myDataManager->lockDataManager();
		mObject<K, V1, M> * vertexObj;
		switch (cmd) {
		case DeleteVertex:
			if (vertexExists(req)) {
				myDataManager->derigsterNBR(req);
				myDataManager->deleteVertex(req);
			}
			break;
		case DeleteOutEdge:
			if (vertexExists(req)) {
				vertexObj = myDataManager->getVertexObjByKey(req);
				vertexObj->deleteOutEdge(target);
				if (!vertexExists(target)) {
					myDataManager->derigsterInEdgeNBR(target, req);
				}
			}
			if (vertexExists(target)) {
				vertexObj = myDataManager->getVertexObjByKey(target);
				vertexObj->deleteInEdge(req);
				if (!vertexExists(req)) {

					myDataManager->derigsterOutEdgeNBR(req, target);
				}
			}
			break;
		case DeleteInEdge:

			if (vertexExists(req)) {
				vertexObj = myDataManager->getVertexObjByKey(req);
				vertexObj->deleteInEdge(target);
				if (!vertexExists(target)) {
					myDataManager->derigsterOutEdgeNBR(target, req);
				}
			}
			if (vertexExists(target)) {
				vertexObj = myDataManager->getVertexObjByKey(target);
				vertexObj->deleteOutEdge(req);
				if (!vertexExists(req)) {
					myDataManager->derigsterInEdgeNBR(req, target);
				}
			}
			break;
		case AddVertex:
			if (!vertexExists(target)) {
				mObject<K, V1, M> * newVertex = new mObject<K, V1, M>(target);
				sc->InformOwnership(target);
			}
			break;
		case AddOutEdge:
			if (vertexExists(req)) {
				vertexObj = myDataManager->getVertexObjByKey(req);
				vertexObj->addOutEdge(target);

				if (!vertexExists(target)) {
					myDataManager->rigsterInEdgeNBR(target, req);
				}
			}
			if (vertexExists(target)) {
				vertexObj = myDataManager->getVertexObjByKey(target);
				vertexObj->addInEdge(req);

				if (!vertexExists(req)) {
					myDataManager->rigsterOutEdgeNBR(req, target);
				}
			}
			break;
		case AddInEdge:
			if (vertexExists(req)) {
				vertexObj = myDataManager->getVertexObjByKey(req);
				vertexObj->addInEdge(target);

				if (!vertexExists(target)) {
					myDataManager->rigsterOutEdgeNBR(target, req);
				}
			}
			if (vertexExists(target)) {
				vertexObj = myDataManager->getVertexObjByKey(target);
				vertexObj->addOutEdge(req);

				if (!vertexExists(req)) {
					myDataManager->rigsterInEdgeNBR(req, target);
				}
			}
			break;
		}
		/*std::cout << " PE" << sc->getRank() << " is finishing dataMutate"
		 << std::endl;*/
		myDataManager->unlockDataManager();
	}
	void dataMutateWithInform(graphMutatorStore<K>& myMutation) {
		switch (myMutation.cmd) {
		case AddVertex:
			break;
		case DeleteVertex:
			/*std::cout << "PE" << sc->getRank() << " is sending DeleteVertex"
			 << std::endl;*/
			mObject<K, V1, M> * vertex;
			vertex = myDataManager->getVertexObjByKey(myMutation.req);
			for (int i = 0; i < vertex->getInEdgeCount(); i++) {
				K targetVertex = vertex->getInEdgeID(i);

				K * keyArray = new K[2];
				keyArray[0] = myMutation.req;
				keyArray[1] = targetVertex;
				mArrayIntTagNK<K> data(DeleteOutEdge, 2, keyArray);
				sc->sendSysMessageValue(GraphMutation, data, INSTANT_PRIORITY,
						targetVertex);
			}
			for (int i = 0; i < vertex->getOutEdgeCount(); i++) {
				K targetVertex = vertex->getOutEdgeID(i);

				K * keyArray = new K[2];
				keyArray[0] = myMutation.req;
				keyArray[1] = targetVertex;
				mArrayIntTagNK<K> data(DeleteInEdge, 2, keyArray);
				sc->sendSysMessageValue(GraphMutation, data, INSTANT_PRIORITY,
						targetVertex);
			}
			break;
		default:
			/*std::cout << "PE" << sc->getRank() << " is sending "
			 << myMutation.cmd << std::endl;*/
			K targetVertex = myMutation.target;
			K * keyArray = new K[2];
			keyArray[0] = myMutation.req;
			keyArray[1] = myMutation.target;
			mArrayIntTagNK<K> data(myMutation.cmd, 2, keyArray);
			sc->sendSysMessageValue(GraphMutation, data, INSTANT_PRIORITY,
					targetVertex);
			break;
		}
		dataMutate(myMutation.cmd, myMutation.req, myMutation.target);
		/*std::cout << " PE" << sc->getRank()
		 << " is finishing dataMutateWithInform & dataMutate"
		 << std::endl;*/
	}
	void aggregate() {
		aggContainer->begin();
		typename std::map<char *, IAggregator<A> *>::iterator it;

		for (it = aggContainer->begin(); it != aggContainer->end(); it++) {
			int length = strlen((*it).first);
			char * aggKey = (char*) calloc(length + 1, sizeof(char));
			strncpy(aggKey, (*it).first, length);
			A tag = (*it).second->getValue();
			mKCharArrayPair<A> value(tag, length + 1, aggKey);
			sc->BroadcastSysMessageValue(Aggregator, value, INSTANT_PRIORITY);
		}
	}
	void postSS(bool haltPE) {
		aggregate();
		clearPendingMessages();

		//sumOutCommGlobal = uc->getGlobal();
		//sumOutCommLocal = uc->getLocal();

		//aveOutCommGlobal = (float) sumOutCommGlobal
		//	/ (float) myDataManager->vertexSetSize();
		//aveOutCommLocal = (float) sumOutCommLocal
		//		/ (float) myDataManager->vertexSetSize();

#ifdef Verbose
		if(sc->getRank()==0) {
			std::cout << "PE" << sc->getRank() << " -----TIME: superStep() Running Time = "
			<< getSSResTime() << " Vertex Running Time = " << getVertexSumResTime()
			<< std::endl;
			cout.flush();
		}
#endif

		//Sending info to all
		//std::cout << "getSumComm() = " << getSumComm() << std::endl;
		mLong * array = new mLong[3];
		array[0].setValue(sc->getRank());
		array[1].setValue(getSSResTime());
		array[2].setValue(getVertexSumResTime());
		//array[3].setValue(getSumOutCommLocal());
		//array[4].setValue(getSumOutCommGlobal());

		//array[2].setValue(getSumInComm());
		//array[4].setValue(getSumIO());

		mLongArray value(3, array);

		if (haltPE) {
			sc->BroadcastSysMessageValue(Terminate, value,
					AFTER_DATABUFFER_PRIORITY);

		} else {
			sc->BroadcastSysMessageValue(EndofSS, value,
					AFTER_DATABUFFER_PRIORITY);
		}
	}

	void clearPendingMessages() {
		if (pendingMessages.size() > 0) {
			cout << "\t-PE" << sc->getRank() << " has pending messages = "
					<< pendingMessages.size() << endl;
		}
		boost::mutex::scoped_lock manageBlockSS_lock =
				boost::mutex::scoped_lock(this->blockSSLock);
		this->resetPendingMessageSync();

		while (!pendingMessages.empty()) {
			messageStore<K, M> a = pendingMessages.front();
			//cout << "PE" << sc->getRank() << " sending message to " << a.key.getValue() << " at SS " << *superStepCnt<< endl;
			uc->sendMessage(a.key, a.value);
			pendingMessages.pop();
		}
		manageBlockSS_lock.unlock();
	}
	bool singleVertexStep(mObject<K, V1, M> * vertexObj) {
		userVertexObject<K, V1, M, A> aaa(sysInfo, aggContainer, aggLock,
				&pendingMutations);
		return singleVertexStep(vertexObj, &aaa);
	}

	inline bool singleVertexStep(mObject<K, V1, M> * kkk,
			userVertexObject<K, V1, M, A> * aaa) {

		kkk->startNewSS();
		messageIterator<M> messages(kkk->getMessageQueue());
		aaa->setVertexObj(kkk);
		float runTime = 0;
		//const clock_t begin_time = clock();
		int tmpLocal = mmg->getLocal();
		int tmpGlobal = mmg->getGlobal();

		if (!kkk->isMigrationMarked() && !kkk->isHalted()) {

			//time_t begin_time_vss = time(NULL);
			//const clock_t begin_time_vss = clock();

			userCode->compute(&messages, aaa, mmg);

			//time_t stop_time_vss = time(NULL);
			//const clock_t stop_time_vss = clock();
			//double ttt = ((double) (stop_time_vss - begin_time_vss))
			//		/ ((double) CLOCKS_PER_SEC);
			//kkk->setSSResTime(ttt);

			kkk->setOutLocal(mmg->getLocal() - tmpLocal);
			kkk->setOutGlobal(mmg->getGlobal() - tmpGlobal);
			return true;
		}
		kkk->setOutLocal(mmg->getLocal() - tmpLocal);
		kkk->setOutGlobal(mmg->getGlobal() - tmpGlobal);
		return false;
	}
	void superStep() {

		userCode->setCurSuperStep((*superStepCnt));
		const clock_t sss = clock();
		//const clock_t begin_time_ss = clock();
		begin_time_ss = time(NULL);

		bool haltPE = true;

		userVertexObject<K, V1, M, A> aaa(sysInfo, aggContainer, aggLock,
				&pendingMutations);

		int haltTrue = 0;
		int haltFalse = 0;
		double runTime = 0;
		sumRunTime = 0;

		mmg->resetCounters();

		int vertexSize = myDataManager->vertexSetSize();
		mObject<K, V1, M> * kkk;

		/*cout << "PE"<<sc->getRank()<<" mObject size = " << sizeof(mObject<K, V1, M>) << std::endl;
		 cout << "PE"<<sc->getRank()<<" K size = " << sizeof(K) << std::endl;
		 cout << "PE"<<sc->getRank()<<" V size = " << sizeof(V1) << std::endl;
		 cout << "PE"<<sc->getRank()<<" M size = " << sizeof(M) << std::endl;
		 cout << "PE"<<sc->getRank()<<" edge size = " << sizeof(edge<K,V1>) << std::endl;
		 */

		/*cout << "PE" << sc->getRank() << " myDataManager->iterateData " << std::endl;*/

		while ((kkk = myDataManager->iterateData(false)) != 0) {

			//if (actualVertexProcess % (myDataManager->vertexSetSize() / 3)
			// == 0) {
			/* cout << "\t-PE " << sc->getRank() << " is processing vertex["
			 << actualVertexProcess << "] at SS " << *superStepCnt
			 << endl;*/
			//}*/
			if (singleVertexStep(kkk, &aaa)) {
				//runTime = kkk->getSSResTime();
				//std::cout << "runTime = " << runTime << std::endl;
				if (kkk->isHalted()) {
					haltTrue++;
				} else {
					haltFalse++;
				}
				haltPE = haltPE && kkk->isHalted();
			} else {
				runTime = 0;
			}

			sumRunTime = sumRunTime + runTime;
		}
		/*cout << "PE" << sc->getRank() << " getSoftDynamicVertexSendSize " << std::endl;*/

		for (int i = 0; i < myDataManager->getSoftDynamicVertexSendSize();
				i++) {
			/*if (i == 0) {
			 <<<<<<< .mine
			 cout << "\t-PE " << sc->getRank()
			 << " is processing SDvertex with size = "
			 << myDataManager->getSoftDynamicVertexSendSize()
			 << endl;
			 }*/

			mObject<K, V1, M> *kkk =
					myDataManager->getSoftDynamicVertexSendObjByPos(i);

			if (singleVertexStep(kkk, &aaa)) {
				runTime = kkk->getSSResTime();

				if (kkk->isHalted()) {
					haltTrue++;
				} else {
					haltFalse++;
				}
				haltPE = haltPE && kkk->isHalted();
			} else {
				runTime = 0;
			}

			sumRunTime = sumRunTime + runTime;
		}

		const clock_t sss2 = clock();
		double syzz = ((double) (sss2 - sss)) / ((double) CLOCKS_PER_SEC);

		if (sc->getRank() == 0) {
			cout << "PE" << sc->getRank() << " Processing SS time = " << syzz
					<< std::endl;
		}

		const clock_t begin_time_vss = clock();

		messageManager<K, V1, M, A> dummy(false, uc, 0);
		//cout << "PE" << sc->getRank() << " mc.hasMoreKeys() = "
		//		<< mc.hasMoreKeys() << std::endl;
		int ijk = 0;
		int lmo = 0;
		while (mc.hasMoreKeys()) {
			K dst;
			std::queue<M, std::list<M> > * queue = mc.getMessageList(dst);
			lmo = lmo + queue->size();
			messageIterator<M> mit(queue);
			cmb->combineMessages(dst, &mit, &dummy);
			delete queue;
			ijk++;

		}
		const clock_t stop_time_vss = clock();
		double ttt = ((double) (stop_time_vss - begin_time_vss))
				/ ((double) CLOCKS_PER_SEC);

		if (sc->getRank() == 0) {
			cout << "PE" << sc->getRank() << " mc size = " << ijk
					<< " out from = " << lmo << " time = " << ttt << std::endl;
		}
		const clock_t begin_time_vss2 = clock();

		dummy.sendQueue();
		mmg->sendQueue();

		const clock_t stop_time_vss2 = clock();
		ttt = ((double) (stop_time_vss2 - begin_time_vss2))
				/ ((double) CLOCKS_PER_SEC);
		if (sc->getRank() == 0) {
			cout << "PE" << sc->getRank() << " comm time = " << ttt
					<< std::endl;
		}
		/*cout << "PE" << sc->getRank() << " steal " << std::endl;*/

		myDataManager->waitForSteel();

		/*cout << "PE" << sc->getRank() << " isEmptyRejectedMessageQueue " << std::endl;*/

		while (!isEmptyRejectedMessageQueue()) {
			kkk = getVertexFromRejectedMessageQueue();
			if (singleVertexStep(kkk, &aaa)) {
				runTime = kkk->getSSResTime();

				if (kkk->isHalted()) {
					haltTrue++;
				} else {
					haltFalse++;
				}
				haltPE = haltPE && kkk->isHalted();
			} else {
				runTime = 0;
			}

			sumRunTime = sumRunTime + runTime;
		}

		/*cout << "PE" << sc->getRank() << " stat " << std::endl;*/

		aveRunTime = sumRunTime / (double) vertexSize;
		//int sumAllInOut = uc->getGlobal()+uc->getLocal() + sumCommGLDiff;
		time_t stop_time_ss = time(NULL);
		resTime = stop_time_ss - begin_time_ss;
		//resTime= float(clock() - begin_time_ss) / CLOCKS_PER_SEC;
		//messageGLDiffSwapSort(maxCommObj, minCommObj);
		postSS(haltPE);
	}
	void gatherStats() {
		sumIO = 0;
		aveIO = 0;

		sumInCommGlobal = 0;
		aveInCommGlobal = 0;

		aveInCommLocal = 0;
		sumInCommLocal = 0;

		sumOutCommGlobal = 0;
		aveOutCommGlobal = 0;

		aveOutCommLocal = 0;
		sumOutCommLocal = 0;

		myDataManager->lockDataManager();

		mObject<K, V1, M> * mVertex;
		for (int i = 0; i < myDataManager->vertexSetSize(); i++) {
			mVertex = myDataManager->getVertexObjByPos(i);

			sumInCommGlobal = sumInCommGlobal + mVertex->getInGlobal();
			sumInCommLocal = sumInCommLocal + mVertex->getInLocal();

			sumOutCommGlobal = sumOutCommGlobal + mVertex->getOutGlobal();
			sumOutCommLocal = sumOutCommLocal + mVertex->getOutLocal();
		}

		aveInCommGlobal = (float) sumInCommGlobal
				/ (float) myDataManager->vertexSetSize();
		aveInCommLocal = (float) sumInCommLocal
				/ (float) myDataManager->vertexSetSize();

		aveOutCommGlobal = (float) sumOutCommGlobal
				/ (float) myDataManager->vertexSetSize();
		aveOutCommLocal = (float) sumOutCommLocal
				/ (float) myDataManager->vertexSetSize();

		myDataManager->unlockDataManager();
	}
	bool vertexExists(K& src) {
		return myDataManager->vertexExists(src);
	}
	void appendIncomeQueueNbr(K& src, M& message, DATA_CMDS inOut) {
		myDataManager->appendIncomeQueueNbr(src, message, inOut, *superStepCnt);
	}
#include <set>
	int cnt;
	//std::set<K> bla;

	int getCnt() {
		return cnt;
	}
	/*std::set<K> * getbla(){
	 return &bla;
	 }*/
	void appendIncomeQueue(K& dst, M& message, int srcRank) {
		bool inStat = myDataManager->appendMessage(dst, message, *superStepCnt);
		if (!inStat) {
			messageStore<K, M> msg = { dst, message };

			boost::mutex::scoped_lock manageBlockSS_lock =
					boost::mutex::scoped_lock(this->blockSSLock);

			if (getPendingMessageSync() == false) {
				cnt++;
				/*if (bla.find(dst) == bla.end()) {
				 std::cout << "PE" << sc->getRank() << " wrong MessageID "
				 << dst.getValue() << " is recvd from " << srcRank
				 << std::endl;
				 }
				 bla.insert(dst);*/
			} else {
				pendingMessages.push(msg);
			}
			manageBlockSS_lock.unlock();
		}
	}
	void appendLocalIncomeQueue(K& dst, M& message) {
		myDataManager->appendLocalMessage(dst, message, *superStepCnt);
	}
	void appendIncomeQueueAll(M& message) {
		for (int i = 0; i < myDataManager->vertexSetSize(); i++) {
			myDataManager->appendMessage(
					myDataManager->getVertexObjByPos(i)->getVertexID(), message,
					*superStepCnt);
		}
	}
	bool cleanUp(enableVTH enableVertices) {
		myDataManager->resetIterator();
		mObject<K, V1, M> * tmpVer;
		bool myVoteToHalt = true;

		if (enableVertices == noneEnable) {
			while ((tmpVer = myDataManager->iterateData(false)) != 0) {
				tmpVer->resetMsgCnt();
				myVoteToHalt = myVoteToHalt & tmpVer->isHalted();
			}
		} else if (enableVertices == EnableWithMessages) {
			while ((tmpVer = myDataManager->iterateData(false)) != 0) {
				if (tmpVer->getInTotal() > 0) {
					tmpVer->enable();
				}
				myVoteToHalt = myVoteToHalt & tmpVer->isHalted();
				tmpVer->resetMsgCnt();
			}
		} else {
			myVoteToHalt = false;
			while ((tmpVer = myDataManager->iterateData(false)) != 0) {
				tmpVer->resetMsgCnt();
				tmpVer->enable();
			}
		}
		myDataManager->resetIterator();
		return myVoteToHalt;
	}
	void messageGLDiffSwapSort(mObject<K, V1, M> * max,
			mObject<K, V1, M> * min) {
		myDataManager->lockDataManager();
		mObject<K, V1, M> * tmp1 = max;
		mObject<K, V1, M> * tmp2;
		for (int i = 0; i < myDataManager->vertexSetSize(); i++) {
			tmp2 = myDataManager->getVertexObjByPos(i);
			if (tmp1->getMessageCountGLDiff() > tmp2->getMessageCountGLDiff()) {
				myDataManager->swapVertexBuffes(tmp1->getVertexID(),
						tmp2->getVertexID());
				tmp1 = tmp2;
			}
		}

		tmp1 = min;
		for (int i = myDataManager->vertexSetSize() - 1; i > 0; i--) {
			tmp2 = myDataManager->getVertexObjByPos(i);
			if (tmp1->getMessageCountGLDiff() < tmp2->getMessageCountGLDiff()) {
				myDataManager->swapVertexBuffes(tmp1->getVertexID(),
						tmp2->getVertexID());
				tmp1 = tmp2;
			}
		}
		myDataManager->unlockDataManager();
	}
	void performDataMutations() {
		//pendingMutations
		//cout << " Implement bla bla bla" << endl;
		while (!pendingMutations.empty()) {
			graphMutatorStore<K> myMutation = pendingMutations.front();

			dataMutateWithInform(myMutation);

			pendingMutations.pop();
		}
	}
	void setPendingMessageSync() {
		pendingMessageSync = true;
	}
	void resetPendingMessageSync() {
		pendingMessageSync = false;
		cnt = 0;
		//bla.clear();
	}
	bool getPendingMessageSync() {
		return pendingMessageSync;
	}

	void appendToRejectedMessageQueue(mObject<K, V1, M> * vertex) {

		rejectedMessageQueue.push(vertex);
	}
	bool isEmptyRejectedMessageQueue() {

		if (rejectedMessageQueue.size() == 0) {
			return true;
		}
		return false;
	}
	mObject<K, V1, M> * getVertexFromRejectedMessageQueue() {
		mObject<K, V1, M> * vertex = rejectedMessageQueue.front();
		rejectedMessageQueue.pop();
		return vertex;
	}
//User API
//virtual void initialize();
//virtual void compute();
//void terminate();

//User defined optimizations
//virtual void messageCombiner()=0;
//virtual void globalCombiner()=0;

//User Defined mObject Data
//virtual void vertexUserDataSeralizer();
//virtual void vertexUserDataDeseralize();

//PreDefined UserAPI
//K getID();
//K getValue();
//K getOldValue();

}
;

#endif /* COMPUTE_H_ */
