/*
 * userComm.h
 *
 *  Created on: May 15, 2012
 *      Author: refops
 */

#ifndef USERCOMM_H_
#define USERCOMM_H_

#include "commManager.h"

template<class K, class V1, class M, class A>
class userComm {
private:

	comm_manager<K, V1, M, A>* myCommMang;
public:
	userComm(comm_manager<K, V1, M, A>* x) {
		myCommMang = x;

	}
	bool isLocal(K id) {
			return myCommMang->isLocal(id);
	}

	void sendMessage(K id, M msg) {
		//cout<<"("<<myCommMang->getRank()<<") is sending a message to ID: "<<((mLong)id).getValue()<<endl;
		myCommMang->sendMessage(id, msg);

	}

	void sendMessageToInNbrs(K srcId, M msg) {

		myCommMang->sendMessageToInNbrs(srcId, msg);
	}
	void sendMessageOutNbrs(K srcId, M msg) {

		myCommMang->sendMessageOutNbrs(srcId, msg);
	}

	void sendMessageToAll(M msg) {

		myCommMang->sendMessageToAll(msg);
	}
};

#endif /* USERCOMM_H_ */
