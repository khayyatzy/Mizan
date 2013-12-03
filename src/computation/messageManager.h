/*
 * messageManager.h
 *
 *  Created on: Nov 27, 2012
 *      Author: refops
 */

#ifndef MESSAGEMANAGER_H_
#define MESSAGEMANAGER_H_

#include "../communication/userComm.h"
#include "../dataManager/dataStructures/graph/MessageContainer.h"

template<class K, class V1, class M, class A> class userComm;

template<class K, class V1, class M, class A>
class messageManager {
	int localMessages;
	int globalMessages;
	bool withMessageCombiner;
	userComm<K, V1, M, A> * uc;
	MessageContainer<K, M> * mc;

	//
	std::vector<K> bla_k;
	std::vector<M> bla;
public:
	int getLocal() {
		return localMessages;
	}
	int getGlobal() {
		return globalMessages;
	}
	void resetCounters() {
		localMessages = 0;
		globalMessages = 0;
	}
	messageManager(bool inWithMessageCombiner, userComm<K, V1, M, A> * iUc,
			MessageContainer<K, M> * inMc) :
			withMessageCombiner(inWithMessageCombiner), uc(iUc), mc(inMc) {
	}

	void sendQueue() {
		for (int i = 0; i < bla.size(); i++) {
			uc->sendMessage(bla_k[i], bla[i]);
		}
		bla.clear();
		bla_k.clear();
	}
	void sendMessage(K id, M msg) {
		if (uc->isLocal(id)) {
			localMessages++;
			uc->sendMessage(id, msg);
		} else {
			globalMessages++;
			if (withMessageCombiner) {
				mc->put(id, msg);
			} else {
				//uc->sendMessage(id, msg);
				bla.push_back(msg);
				bla_k.push_back(id);
			}
		}
	}

	void sendMessageToInNbrs(K srcId, M msg) {
		uc->sendMessageToInNbrs(srcId, msg);
	}
	void sendMessageOutNbrs(K srcId, M msg) {
		uc->sendMessageOutNbrs(srcId, msg);
	}

	void sendMessageToAll(M msg) {
		uc->sendMessageToAll(msg);
	}

	virtual ~messageManager() {
	}
	;
};

#endif /* MESSAGEMANAGER_H_ */
