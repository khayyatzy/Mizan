/*
 * object.h
 *
 *  Created on: Apr 3, 2012
 *      Author: refops
 */

#ifndef MOBJECTMINI_H_
#define MOBJECTMINI_H_

#include "vertex.h"
#include "edge.h"
#include <boost/array.hpp>
#include <set>
#include <vector>
#include <queue>
#include "../data/mInt.h"
#include "boost/thread.hpp"
//#include "../../../communication/dataStructures/MyQueue.h"

template<class K, class V1>
class mObjectMini {
private:
	K vertexID;
	std::vector<K> internalOutEdges;
	std::vector<K> internalInEdges;

public:
	mObjectMini() {
	}
	mObjectMini(K verID) {
		vertexID = verID;
	}
	mObjectMini(K verID, V1 verVal) {
		vertexID = verID;
	}
	virtual ~mObjectMini() {
		//delete[](SSMsgCnt);
	}
	K getVertexID() {
		return vertexID;
	}
	void addOutEdge(K vertex) {
		internalOutEdges.push_back(vertex);
	}
	void addInEdge(K vertex) {
		internalInEdges.push_back(vertex);
	}
	int getOutEdgeCount() {
		return internalOutEdges.size();
	}
	int getInEdgeCount() {
		return internalInEdges.size();
	}
	K getOutEdgeID(int pos) {

		return internalOutEdges[pos];
	}
	K getInEdgeID(int pos) {
		return internalOutEdges[pos];
	}
	bool hasOutEdge(K targetVertexId) {
		for (int i = 0; i < internalOutEdges.size(); i++) {
			if (targetVertexId == internalOutEdges[i]) {
				return true;
			}
		}
		return false;
	}
	bool hasInEdge(K targetVertexId) {
		for (int i = 0; i < internalOutEdges.size(); i++) {
			if (targetVertexId == internalOutEdges[i]) {
				return true;
			}
		}
		return false;
	}
	void deleteOutEdge(K vertex) {
		for (int i = 0; i < internalOutEdges.size(); i++) {
			if (vertex == internalOutEdges[i]) {
				internalOutEdges.erase(internalOutEdges.begin() + i);
				break;
			}
		}
	}
	void deleteInEdge(K vertex) {
		for (int i = 0; i < internalInEdges.size(); i++) {
			if (vertex == internalInEdges[i]) {
				internalInEdges.erase(internalInEdges.begin() + i);
				break;
			}
		}
	}
}
;

#endif /* OBJECT_H_ */
