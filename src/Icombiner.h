/*
 * Icombiner.h
 *
 *  Created on: May 22, 2012
 *      Author: refops
 */

#ifndef ICOMBINER_H_
#define ICOMBINER_H_
#include <list>
#include "computation/messageManager.h"
#include "dataManager/dataStructures/graph/messageIterator.h"

template<class K, class V1,class M, class A>
class Icombiner {
public:
	Icombiner(){};
	virtual void combineMessages(K dst,messageIterator<M> * messages,messageManager<K,V1,M,A> * mManager)=0;
	virtual ~Icombiner(){};
};

#endif /* ICOMBINER_H_ */
