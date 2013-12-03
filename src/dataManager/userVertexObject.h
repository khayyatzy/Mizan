/*
 * userData.h
 *
 *  Created on: May 15, 2012
 *      Author: refops
 */

#ifndef USERVERTEXOBJECT_H_
#define USERVERTEXOBJECT_H_

#include "dataStructures/graph/mObject.h"
#include "../computation/systemWideInfo.h"
#include "../IAggregator.h"
#include "dataStructures/general.h"

template<class K, class V1, class M, class A>
class userVertexObject {
private:
	mObject<K, V1, M> * myVertex;
	systemWideInfo<K> * sysInfo;
	std::map<char *, IAggregator<A> *> * aggContainer;
	boost::mutex * aggLock;
	std::queue<graphMutatorStore<K> > * pendingMutations;

public:
	userVertexObject(systemWideInfo<K> * inSysInfo,
			std::map<char *, IAggregator<A> *> * inAggContainer,
			boost::mutex * inAggLock,
			std::queue<graphMutatorStore<K> >* inPendingMutations) {
		sysInfo = inSysInfo;
		aggContainer = inAggContainer;
		pendingMutations = inPendingMutations;
		aggLock = inAggLock;
	}
	virtual ~userVertexObject() {
	}
	void aggregate(char * aggName, A value) {
		aggLock->lock();
		aggContainer->at(aggName)->aggregate(value);
		aggLock->unlock();
	}
	A getAggregatorValue(char * aggName) {
		return aggContainer->at(aggName)->getValue();
	}
	K getMaxVertex() {
		return sysInfo->getMaxVertex();
	}
	K getMinVertex() {
		return sysInfo->getMinVertex();
	}
	void setVertexObj(mObject<K, V1, M> * vertex) {
		myVertex = vertex;
	}
	int getCurrentSS() {
		return myVertex->getCurrentSS();
	}
	long getGlobalVertexCount() {
		return sysInfo->getCounter("_Graph_Vertex_Size");
	}
	//vertex methods
	K getVertexID() {
		return myVertex->getVertexID();
	}
	V1 getVertexValue() {
		return myVertex->getVertexValue();
	}
	void setVertexValue(V1 newValue) {
		myVertex->setVertexValue(newValue);
	}

	void delVertex() {
		graphMutatorStore<K> mutateVertex;
		mutateVertex.cmd = DeleteVertex;
		mutateVertex.req = myVertex->getVertexID();
		mutateVertex.target = myVertex->getVertexID();
		pendingMutations->push(mutateVertex);
	}
	void addVertex(K id) {
		graphMutatorStore<K> mutateVertex;
		mutateVertex.cmd = AddVertex;
		mutateVertex.req = myVertex->getVertexID();
		mutateVertex.target = id;
		pendingMutations->push(mutateVertex);
	}
	void delOutEdge(K vertex) {
		graphMutatorStore<K> mutateVertex;
		mutateVertex.cmd = DeleteOutEdge;
		mutateVertex.req = myVertex->getVertexID();
		mutateVertex.target = vertex;
		pendingMutations->push(mutateVertex);
	}
	void delInEdge(K vertex) {
		graphMutatorStore<K> mutateVertex;
		mutateVertex.cmd = DeleteInEdge;
		mutateVertex.req = myVertex->getVertexID();
		mutateVertex.target = vertex;
		pendingMutations->push(mutateVertex);
	}
	void addOutEdge(K vertex) {
		graphMutatorStore<K> mutateVertex;
		mutateVertex.cmd = AddOutEdge;
		mutateVertex.req = myVertex->getVertexID();
		mutateVertex.target = vertex;
		pendingMutations->push(mutateVertex);
	}
	void addInEdge(K vertex) {
		graphMutatorStore<K> mutateVertex;
		mutateVertex.cmd = AddInEdge;
		mutateVertex.req = myVertex->getVertexID();
		mutateVertex.target = vertex;
		pendingMutations->push(mutateVertex);
	}

	int getOutEdgeCount() {
		return myVertex->getOutEdgeCount();
	}
	int getInEdgeCount() {
		return myVertex->getInEdgeCount();
	}
	K getOutEdgeID(int pos) {
		return myVertex->getOutEdgeID(pos);
	}
	K getInEdgeID(int pos) {
		return myVertex->getInEdgeID(pos);
	}

	V1 getOutEdgeValue(int pos) {
		return myVertex->getOutEdgeValue(pos);
	}
	V1 getInEdgeValue(int pos) {
		return myVertex->getInEdgeValue(pos);
	}
	V1 getOutEdgeValue(K vertex) {
		return myVertex->getOutEdgeValue(vertex);
	}
	V1 getInEdgeValue(K vertex) {
		return myVertex->getInEdgeValue(vertex);
	}
	void setOutEdgeValue(K vertex, V1 edgeValue) {
		myVertex->setOutEdgeValue(vertex, edgeValue);
	}
	void setInEdgeValue(K vertex, V1 edgeValue) {
		myVertex->setInEdgeValue(vertex, edgeValue);
	}
	bool hasOutEdge(K targetVertexId) {
		return myVertex->hasOutEdge(targetVertexId);
	}
	bool hasInEdge(K targetVertexId) {
		return myVertex->hasInEdge(targetVertexId);
	}

	void voteToHalt() {
		myVertex->voteToHalt();
	}
	/*
	 K * getAllOutEdgesID(int &count) {
	 }
	 V * getAllOutEdgesValues(int &count) {
	 }
	 K * getAllInEdgesID(int &count) {
	 }
	 V * getAllInEdgesValues(int &count) {
	 }
	 void delOutEdge(K vertex) {
	 }
	 void delInEdge(K vertex) {
	 }


	 void voteToHalt() {
	 myVertex->voteToHalt();
	 }
	 */

};
#endif /* USERDATA_H_ */
