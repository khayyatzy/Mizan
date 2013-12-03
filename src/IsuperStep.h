/*
 * IsuperStep.h
 *
 *  Created on: May 15, 2012
 *      Author: refops
 */

#ifndef ISUPERSTEP_H_
#define ISUPERSTEP_H_

#include "dataManager/userVertexObject.h"
#include "computation/messageManager.h"
#include "dataManager/dataStructures/graph/messageIterator.h"

template<class K, class V1,class M, class A> class userComm;

template<class K, class V1,class M, class A>
class IsuperStep {
	int curSuperStep;
public:
	void setCurSuperStep(int curSS){
		curSuperStep = curSS;
	}
	int getCurSuperStep(){
		return curSuperStep;
	}
	virtual void initialize(userVertexObject<K,V1,M,A> * data)=0;
	virtual void compute(messageIterator<M> * messages,userVertexObject<K,V1,M,A> * data,messageManager<K,V1,M,A> * mManager)=0;
	virtual ~IsuperStep(){};
};

#endif /* ISUPERSTEP_H_ */
