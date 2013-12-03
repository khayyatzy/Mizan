/*
 * object.h
 *
 *  Created on: Apr 3, 2012
 *      Author: refops
 */

#ifndef MOBJECT_H_
#define MOBJECT_H_

#include "vertex.h"
#include "edge.h"
#include <boost/array.hpp>
#include <set>
#include <vector>
#include <queue>
#include <list>
#include "../data/mInt.h"
#include "../data/mLong.h"
#include "../data/mDouble.h"
#include "boost/thread.hpp"
#include <string>
#include <stdio.h>

template<class K, class V1, class M>
class mObject {
private:
	K vertexID;
	V1 vertexVal;
	bool terminated;
	int curSuperStep;
	double ssResTime;
	bool isReplicated;

	std::vector<edge<K, V1> *> outEdges;
	std::vector<edge<K, V1> *> inEdges;
	//std::list<edge<K, V1> > outEdges;
	//std::list<edge<K, V1> > inEdges;

//  std::queue<M, std::deque<M> > * evenMessageQueue;
//	std::queue<M, std::deque<M> > * oddMessageQueue;
	std::queue<M, std::list<M> > * evenMessageQueue;
	std::queue<M, std::list<M> > * oddMessageQueue;

	bool migrationMark;

	int SSInMsgCntLocal;
	int SSInMsgCntGlobal;

	int SSOutMsgCntGlobal;
	int SSOutMsgCntLocal;

public:
	mObject() {
		init();
	}
	mObject(K verID) {
		vertexID = verID;
		init();
	}
	mObject(K verID, V1 verVal) {
		vertexID = verID;
		vertexVal = verVal;
		init();
	}
	void enable() {
		terminated = false;
	}
	virtual ~mObject() {
		delete evenMessageQueue;
		delete oddMessageQueue;
		for (int i = 0; i < outEdges.size(); i++) {
			delete (outEdges[i]);
		}
		for (int i = 0; i < inEdges.size(); i++) {
			delete (inEdges[i]);
		}
	}
	void setIsReplicated(bool state) {
		isReplicated = state;
	}
	bool getIsReplicated(){
		return isReplicated;
	}

	void setOutLocal(int inVal) {
		SSOutMsgCntLocal = inVal;
	}
	void setOutGlobal(int inVal) {
		SSOutMsgCntGlobal = inVal;
	}
	int getOutLocal() {
		return SSOutMsgCntLocal;
	}
	int getOutGlobal() {
		return SSOutMsgCntGlobal;
	}
	int getOutDiff() {
		return (SSOutMsgCntGlobal - SSOutMsgCntLocal);
	}
	std::string toString() {
		std::string output;
		output.append(vertexID.toString());
		output.append("\t");
		output.append(vertexVal.toString());
		return output;
	}
	void setSS(int ss) {
		this->curSuperStep = ss;
	}
	void init() {
		evenMessageQueue = new std::queue<M, std::list<M> >();
		oddMessageQueue = new std::queue<M, std::list<M> >();
		reset();
	}
	void reset() {
		migrationMark = false;
		curSuperStep = 0;
		terminated = false;
		SSInMsgCntLocal = 0;
		SSInMsgCntGlobal = 0;
		SSOutMsgCntLocal = 0;
		SSOutMsgCntGlobal = 0;

	}

	int getInGlobal() {
		return SSInMsgCntGlobal;
	}
	int getInDiff() {
		return (SSInMsgCntGlobal - SSInMsgCntLocal);
	}
	int getInLocal() {
		return SSInMsgCntLocal;
	}
	int getInTotal() {
		return (SSInMsgCntLocal + SSInMsgCntGlobal);
	}
	int getMessageCount() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			return evenMessageQueue->size();

		} else {
			return oddMessageQueue->size();
		}
	}
	void resetMsgCnt() {
		ssResTime = 0;
		SSInMsgCntLocal = 0;
		SSInMsgCntGlobal = 0;
	}
//vertex methods
	K getVertexID() {
		return vertexID;
	}
	V1 getVertexValue() {
		return vertexVal;
	}
	void setVertexValue(V1 newVal) {
		vertexVal.cleanUp();
		vertexVal = newVal;
	}

//edges methods
	void addOutEdge(K vertex) {
		edge<K, V1> * e = new edge<K, V1>(vertex);
		outEdges.push_back(e);
	}
	void addInEdge(K vertex) {
		edge<K, V1> * e = new edge<K, V1>(vertex);
		inEdges.push_back(e);
	}
	void addOutEdge(K vertex, V1 val) {
		edge<K, V1> * e = new edge<K, V1>(vertex, val);
		outEdges.push_back(e);
	}
	void addInEdge(K vertex, V1 val) {
		edge<K, V1> * e = new edge<K, V1>(vertex, val);
		inEdges.push_back(e);
	}
	void deleteOutEdge(K vertex) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (vertex == outEdges[i]->getID()) {
				delete (outEdges[i]);
				outEdges.erase(outEdges.begin() + i);
				break;
			}
		}
	}
	void deleteInEdge(K vertex) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (vertex == inEdges[i]->getID()) {
				delete (inEdges[i]);
				inEdges.erase(inEdges.begin() + i);
				break;
			}
		}
	}

	int getOutEdgeCount() {
		return outEdges.size();
	}
	int getInEdgeCount() {
		return inEdges.size();
	}
	K getOutEdgeID(int pos) {

		return outEdges[pos]->getID();
	}
	K getInEdgeID(int pos) {
		return inEdges[pos]->getID();
	}

//Possible sync problem
	std::vector<edge<K, V1> *> * getOutEdges() {
		return &outEdges;
	}
	std::vector<edge<K, V1> *> * getInEdges() {
		return &inEdges;
	}

	V1 getOutEdgeValue(int pos) {
		return outEdges[pos]->getValue();
	}
	V1 getInEdgeValue(int pos) {
		return inEdges[pos]->getValue();
	}
	V1 getOutEdgeValue(K vertex) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (vertex == outEdges[i]->getID()) {
				return outEdges[i]->getValue();
			}
		}
	}
	V1 getInEdgeValue(K vertex) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (vertex == inEdges[i]->getID()) {
				return inEdges[i]->getValue();
			}
		}
	}
	void setOutEdgeValue(K vertex, V1 edgeValue) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (vertex == outEdges[i]->getID()) {
				outEdges[i]->setValue(edgeValue);
				break;
			}
		}
	}
	void setInEdgeValue(K vertex, V1 edgeValue) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (vertex == inEdges[i]->getID()) {
				inEdges[i]->setValue(edgeValue);
				break;
			}
		}
	}
	bool hasOutEdge(K targetVertexId) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (targetVertexId == outEdges[i]->getID()) {
				return true;
			}
		}
		return false;
	}
	bool hasInEdge(K targetVertexId) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (targetVertexId == inEdges[i]->getID()) {
				return true;
			}
		}
		return false;
	}

	void appendMessageInQueue(M message, int location) {
		//this->insert_message.lock();
		if (location == 0) {
			evenMessageQueue->push(message);
		} else {
			oddMessageQueue->push(message);
		}
		//insert_message_lock.unlock();
		//this->insert_message.unlock();
	}
	void appendMessage(M message, int inSuperStep) {

		int evenOddTest = inSuperStep & 1;
		if (evenOddTest == 0) {
			appendMessageInQueue(message, 1);
			//SSMsgCntOdd++;
		} else {
			appendMessageInQueue(message, 0);
			//SSMsgCntEven++;
		}
		SSInMsgCntGlobal++;

	}
	void appendLocalMessage(M message, int inSuperStep) {
		/*boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
		 this->insert);*/
		int evenOddTest = inSuperStep & 1;
		if (evenOddTest == 0) {
			//oddMessageQueue->push(message);
			appendMessageInQueue(message, 1);
			//SSMsgCntOddLocal++;
		} else {
			appendMessageInQueue(message, 0);
			//evenMessageQueue->push(message);
			//SSMsgCntEvenLocal++;
		}
		SSInMsgCntLocal++;

		/*insert_lock.unlock();*/
	}
	std::queue<M, std::list<M> > * getMessageQueue() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			/*if (((mLong) this->vertexID).getValue() == 94919) {
			 std::cout << "------\treturn an even queue for "
			 << ((mLong) this->vertexID).getValue() << std::endl;
			 }*/
			return evenMessageQueue;
		} else {
			/*if (((mLong) this->vertexID).getValue() == 94919) {
			 std::cout << "------\treturn an odd queue for "
			 << ((mLong) this->vertexID).getValue() << std::endl;
			 }*/
			return oddMessageQueue;
		}
	}
	std::queue<M, std::list<M> > * getMessageQueue(int evenOdd) {
		if (evenOdd == 0) {
			return evenMessageQueue;
		} else {
			return oddMessageQueue;
		}
	}
	void setMessageQueue(int evenOdd, std::queue<M, std::list<M> > * q) {
		if (evenOdd == 0) {
			evenMessageQueue = q;
		} else {
			oddMessageQueue = q;
		}
	}
	void setMessageQueueCopy(int evenOdd,
			std::queue<M, std::list<M> > * queue) {
		if (evenOdd == 0) {
			while (queue->size() > 0) {
				evenMessageQueue->push(queue->front());
				queue->pop();
			}
		} else {
			while (queue->size() > 0) {
				oddMessageQueue->push(queue->front());
				queue->pop();
			}
		}
	}

	void voteToHalt() {
		terminated = true;
	}
	bool isHalted() {
		return terminated;
	}
	void startNewSS() {
		curSuperStep++;
		if (terminated && getMessageCount() != 0) {
			terminated = false;
		}
	}
	int getCurrentSS() {
		return curSuperStep;
	}
	void setSSResTime(double inSSResTime) {
		ssResTime = inSSResTime;
	}
	double getSSResTime() {
		return ssResTime;
	}
	int getCompactSize() {
		return vertexID.byteSize() + vertexVal.byteSize()
				+ (4 * mInt().byteSize()) + 7 + (mDouble().byteSize() + 1);
	}

	int getByteSize() {
		return (vertexID.byteSize() + 1)
				+ (vertexVal.byteSize() + mInt().byteSize() + 1) + 2
				+ (outEdges.size() * vertexID.byteSize() + outEdges.size()
						+ mInt().byteSize() + 1)
				+ (outEdges.size() * vertexVal.byteSize() + outEdges.size())
				+ (inEdges.size() * vertexID.byteSize() + inEdges.size()
						+ mInt().byteSize() + 1)
				+ (inEdges.size() * vertexVal.byteSize() + inEdges.size()) + 1
				+ (mInt().byteSize() + 1) + (mDouble().byteSize() + 1)
				+ (mInt().byteSize() + 1) + (mInt().byteSize() + 1)
				+ (mInt().byteSize() + 1) + (mInt().byteSize() + 1) + 1;
	}
	int getByteSizeMsg() {
		return getByteSize() + (2 * sizeof(int))
				+ (evenMessageQueue->size()
						* (evenMessageQueue->size() > 0 ?
								evenMessageQueue->front().byteSize() : 0)
						+ (1 + evenMessageQueue->size()) * mInt().byteSize()
						+ (1 + evenMessageQueue->size()) * 2)
				+ (oddMessageQueue->size()
						* (oddMessageQueue->size() > 0 ?
								oddMessageQueue->front().byteSize() : 0)
						+ (oddMessageQueue->size() + 1) * mInt().byteSize()
						+ (oddMessageQueue->size() + 1) * 2);
	}
	char * byteEncodeCompact(int &size) {
		char * vertexEncoded = (char *) calloc(getCompactSize(), sizeof(char));

		int ptr = 0;
		int tmpSize = vertexID.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		/*tmpSize = vertexVal.byteEncode2(&vertexEncoded[ptr + 1]);
		 vertexEncoded[ptr] = ((char) tmpSize);
		 ptr = ptr + tmpSize + 1;*/
		int vertexValSize;
		char * tmpVertexVal = vertexVal.byteEncode(vertexValSize);

		mInt vertexValSizeDec(vertexValSize);
		tmpSize = vertexValSizeDec.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		memcpy(&vertexEncoded[ptr], tmpVertexVal, vertexValSize);
		ptr = ptr + vertexValSize;
		free(tmpVertexVal);

		if (terminated) {
			vertexEncoded[ptr++] = (char) 1;
		} else {
			vertexEncoded[ptr++] = (char) 0;
		}

		mInt css(curSuperStep);
		mDouble rt(ssResTime);

		mInt msgOutLocal(SSOutMsgCntLocal);
		mInt msgOutGlobal(SSOutMsgCntGlobal);

		tmpSize = css.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		tmpSize = rt.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		tmpSize = msgOutLocal.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		tmpSize = msgOutGlobal.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		size = ptr;
		return vertexEncoded;
	}
	char * byteEncode(int &size, bool msg) {

		int maxSize;
		if (msg) {
			maxSize = getByteSizeMsg();
		} else {
			maxSize = getByteSize();
		}

		//std::cout << "vertex "<< vertexID.getValue() << " 1 "<< std::endl;
		char * vertexEncoded = (char *) calloc(maxSize + 1, sizeof(char));

		//std::cout << "vertex "<< vertexID.getValue() << " 2 "<< std::endl;
		int ptr = 0;
		int tmpSize = vertexID.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		/*tmpSize = vertexVal.byteEncode2(&vertexEncoded[ptr + 1]);
		 vertexEncoded[ptr] = ((char) tmpSize);
		 ptr = ptr + tmpSize + 1;*/

		int vertexValSize;
		char * tmpVertexVal = vertexVal.byteEncode(vertexValSize);

		//std::cout << "ptr = " << ptr << std::endl;
		//std::cout << "vertexValSize = " << vertexValSize << std::endl;
		//std::cout << "vertexVal.byteSize(true)" << vertexVal.byteSize() << std::endl;

		mInt vertexValSizeDec(vertexValSize);
		tmpSize = vertexValSizeDec.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		memcpy(&vertexEncoded[ptr], tmpVertexVal, vertexValSize);
		ptr = ptr + vertexValSize;
		free(tmpVertexVal);

		//std::cout << "ptr = " << ptr << std::endl;

		//std::cout << "vertex "<< vertexID.getValue() << " b ptr =  "<< ptr<<std::endl;
		//std::cout << "vertex "<< vertexID.getValue() << " 4 "<< std::endl;
		if (terminated) {
			vertexEncoded[ptr++] = (char) 1;
		} else {
			vertexEncoded[ptr++] = (char) 0;
		}

		mInt css(curSuperStep);
		mInt oe(outEdges.size());
		mInt ie(inEdges.size());

		//std::cout << "vertex "<< vertexID.getValue() << " 5 "<< std::endl;
		tmpSize = css.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " c ptr =  "<< ptr<<std::endl;

		//std::cout << "vertex "<< vertexID.getValue() << " 6 "<< std::endl;
		tmpSize = oe.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " d ptr =  "<< ptr<<std::endl;
		//std::cout << "vertex "<< vertexID.getValue() << " 7 "<< std::endl;
		for (int i = 0; i < outEdges.size(); i++) {
			tmpSize = outEdges[i]->getID().byteEncode2(&vertexEncoded[ptr + 1]);
			vertexEncoded[ptr] = ((char) tmpSize);
			ptr = ptr + tmpSize + 1;
			if (outEdges[i]->hasValue()) {
				vertexEncoded[ptr++] = (char) 1;
				tmpSize = outEdges[i]->getValue().byteEncode2(
						&vertexEncoded[ptr + 1]);
				vertexEncoded[ptr] = ((char) tmpSize);
				ptr = ptr + tmpSize + 1;
			} else {
				vertexEncoded[ptr++] = (char) 0;
			}
		}
		//std::cout << "vertex "<< vertexID.getValue() << " e ptr =  "<< ptr<<std::endl;
		////std::cout << "vertex "<< vertexID.getValue() << " 8 "<< std::endl;
		tmpSize = ie.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " f ptr =  "<< ptr<<std::endl;
		for (int i = 0; i < inEdges.size(); i++) {
			tmpSize = inEdges[i]->getID().byteEncode2(&vertexEncoded[ptr + 1]);
			vertexEncoded[ptr] = ((char) tmpSize);
			ptr = ptr + tmpSize + 1;
			if (inEdges[i]->hasValue()) {
				vertexEncoded[ptr++] = (char) 1;
				tmpSize = inEdges[i]->getValue().byteEncode2(
						&vertexEncoded[ptr + 1]);
				vertexEncoded[ptr] = ((char) tmpSize);
				ptr = ptr + tmpSize + 1;
			} else {
				vertexEncoded[ptr++] = (char) 0;
			}
		}
		if (msg) {
			vertexEncoded[ptr++] = (char) 1;

			mInt evenMsgCnt;
			mInt oddMsgCnt;

			int evenOddTest = (this->curSuperStep + 1) & 1;
			if (evenOddTest == 0) {
				evenMsgCnt.setValue(evenMessageQueue->size());
				oddMsgCnt.setValue(0);
			} else {
				evenMsgCnt.setValue(0);
				oddMsgCnt.setValue(oddMessageQueue->size());
			}
			/*std::cout << "vertex " << vertexID.getValue() << " en even = "
			 << evenMsgCnt.getValue() << " en odd "
			 << oddMsgCnt.getValue() << " outEdges = " << outEdges.size()
			 << " inEdges.size() = " << inEdges.size() << std::endl;*/
			/*std::cout << "vertex " << vertexID.getValue() << " en Message = "
			 << evenOddTest << ":"
			 << (evenMsgCnt.getValue() + oddMsgCnt.getValue())
			 << std::endl;

			 std::cout << "vertex " << vertexID.getValue() << " en queues "
			 << evenMessageQueue->size() << " "
			 << oddMessageQueue->size() << std::endl;*/
			tmpSize = evenMsgCnt.byteEncode2(&vertexEncoded[ptr + 1]);
			vertexEncoded[ptr] = ((char) tmpSize);
			/*std::cout << "vertex " << vertexID.getValue() << " even "
			 << ((int) vertexEncoded[ptr]) << std::endl;*/
			ptr = ptr + tmpSize + 1;
			if (evenMsgCnt.getValue() > 0) {
				while (!evenMessageQueue->empty()) {

					/*tmpSize = evenMessageQueue->front().byteEncode2(
					 &vertexEncoded[ptr + 1]);
					 vertexEncoded[ptr] = ((char) tmpSize);
					 ptr = ptr + tmpSize + 1;*/

					int msgSize;
					char * tmpMsgVal = evenMessageQueue->front().byteEncode(
							msgSize);

					mInt msgSizeDec(msgSize);
					tmpSize = msgSizeDec.byteEncode2(&vertexEncoded[ptr + 1]);
					vertexEncoded[ptr] = ((char) tmpSize);
					ptr = ptr + tmpSize + 1;

					memcpy(&vertexEncoded[ptr], tmpMsgVal, msgSize);
					ptr = ptr + msgSize;
					free(tmpMsgVal);

					evenMessageQueue->pop();
				}
			}

			tmpSize = oddMsgCnt.byteEncode2(&vertexEncoded[ptr + 1]);
			vertexEncoded[ptr] = ((char) tmpSize);
			/*std::cout << "vertex " << vertexID.getValue() << " odd "
			 << ((int) vertexEncoded[ptr]) << std::endl;*/
			ptr = ptr + tmpSize + 1;
			if (oddMsgCnt.getValue() > 0) {
				while (!oddMessageQueue->empty()) {
					/*tmpSize = oddMessageQueue->front().byteEncode2(
					 &vertexEncoded[ptr + 1]);
					 vertexEncoded[ptr] = ((char) tmpSize);
					 ptr = ptr + tmpSize + 1;*/

					int msgSize;
					char * tmpMsgVal = oddMessageQueue->front().byteEncode(
							msgSize);

					mInt msgSizeDec(msgSize);
					tmpSize = msgSizeDec.byteEncode2(&vertexEncoded[ptr + 1]);
					vertexEncoded[ptr] = ((char) tmpSize);
					ptr = ptr + tmpSize + 1;

					memcpy(&vertexEncoded[ptr], tmpMsgVal, msgSize);
					ptr = ptr + msgSize;
					free(tmpMsgVal);

					oddMessageQueue->pop();
				}
			}
		} else {
			vertexEncoded[ptr++] = (char) 0;
		}
		//std::cout << "vertex "<< vertexID.getValue() << " byteSize()/size = " <<maxSize<<"/"<<ptr<<std::endl;
		size = ptr;
		/*if (maxSize < ptr) {
		 std::cout << "decoding ptr = " << ptr << " vs " << maxSize
		 << std::endl;
		 }*/
		return vertexEncoded;
	}
	void byteDecodeCompact(int size, char * data) {
		init();
		int ptr = 0;
		int objPtr = ((int) data[ptr]);
		vertexID.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		/*objPtr = ((int) data[ptr]);
		 vertexVal.byteDecode(objPtr, &data[ptr + 1]);
		 ptr = ptr + objPtr + 1;*/

		mInt vertexValSize;
		objPtr = ((int) data[ptr]);
		vertexValSize.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		vertexVal.byteDecode(vertexValSize.getValue(), &data[ptr]);
		ptr = ptr + vertexValSize.getValue();

		if (data[ptr++] == ((char) 0)) {
			terminated = false;
		} else {
			terminated = true;
		}

		mInt css;
		mDouble rt;

		mInt msgOutLocal;
		mInt msgOutGlobal;

		objPtr = ((int) data[ptr]);
		css.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;
		curSuperStep = css.getValue();

		objPtr = ((int) data[ptr]);
		rt.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;
		ssResTime = rt.getValue();

		objPtr = ((int) data[ptr]);
		msgOutLocal.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;
		SSOutMsgCntLocal = msgOutLocal.getValue();

		objPtr = ((int) data[ptr]);
		msgOutGlobal.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;
		SSOutMsgCntGlobal = msgOutGlobal.getValue();
	}
	void byteDecode(int size, char * data) {

		reset();

		//const clock_t a1 = clock();

		int ptr = 0;
		int objPtr = ((int) data[ptr]);
		vertexID.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " A ptr =  "<< ptr<<std::endl;

		//const clock_t a2 = clock();

		/*objPtr = ((int) data[ptr]);
		 vertexVal.byteDecode(objPtr, &data[ptr + 1]);
		 ptr = ptr + objPtr + 1;*/

		mInt vertexValSize;
		objPtr = ((int) data[ptr]);
		vertexValSize.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		vertexVal.byteDecode(vertexValSize.getValue(), &data[ptr]);
		ptr = ptr + vertexValSize.getValue();

		/*std::cout << "vertex "<< vertexID.getValue() << " vertexVal "<< vertexVal.getValue()<<std::endl;
		 */
		//const clock_t a3 = clock();
		if (data[ptr++] == ((char) 0)) {
			terminated = false;
		} else {
			terminated = true;
		}

		//const clock_t a4 = clock();

		mInt css;
		mInt oe;
		mInt ie;

		//const clock_t a5 = clock();

		objPtr = ((int) data[ptr]);
		css.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " css  =  "<< css.getValue() <<std::endl;

		//const clock_t a6 = clock();

		curSuperStep = css.getValue();

		//const clock_t a7 = clock();

		objPtr = ((int) data[ptr]);
		oe.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " D ptr =  "<< ptr<<std::endl;
		//clock_t a8 = clock();

		bool hasValue;
		for (int i = 0; i < oe.getValue(); i++) {
			K vertex;
			objPtr = ((int) data[ptr]);
			vertex.byteDecode(objPtr, &data[ptr + 1]);
			ptr = ptr + objPtr + 1;

			if (data[ptr++] == ((char) 0)) {
				hasValue = false;
				edge<K, V1> * ed = new edge<K, V1>(vertex);
				outEdges.push_back(ed);
			} else {
				hasValue = true;
				V1 value;
				objPtr = ((int) data[ptr]);
				value.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;
				edge<K, V1> * ed = new edge<K, V1>(vertex, value);
				outEdges.push_back(ed);
			}

		}

		//std::cout << "vertex "<< vertexID.getValue() << " e ptr =  "<< ptr<<std::endl;

		//clock_t a9 = clock();

		objPtr = ((int) data[ptr]);
		ie.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " f ptr =  "<< ptr<<std::endl;
		for (int i = 0; i < ie.getValue(); i++) {
			K vertex;
			objPtr = ((int) data[ptr]);
			vertex.byteDecode(objPtr, &data[ptr + 1]);
			ptr = ptr + objPtr + 1;

			if (data[ptr++] == 0) {
				hasValue = false;
				edge<K, V1> * myEdge = new edge<K, V1>(vertex);
				inEdges.push_back(myEdge);
			} else {
				hasValue = true;
				V1 value;
				objPtr = ((int) data[ptr]);
				value.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;
				edge<K, V1> * myEdge = new edge<K, V1>(vertex, value);
				inEdges.push_back(myEdge);
			}
		}

		//const clock_t a10 = clock();
		/*std::cout << "vertex " << vertexID.getValue() << " inEdges =  "
		 << inEdges.size() << std::endl;
		 std::cout << "vertex " << vertexID.getValue() << " outEdges =  "
		 << outEdges.size() << std::endl;*/

		bool hasMessage = false;
		if (data[ptr++] == 0) {
			hasMessage = false;
		} else {
			hasMessage = true;
		}
		if (hasMessage) {
			mInt evenMsgCnt;
			mInt oddMsgCnt;
			//a8 = clock();
			objPtr = (int) (data[ptr]);
			evenMsgCnt.byteDecode(objPtr, &data[ptr + 1]);
			ptr = ptr + objPtr + 1;

			/*std::cout << "vertex " << vertexID.getValue() << " evenMsgCnt =  "
			 << evenMsgCnt.getValue() << std::endl;*/

			for (int i = 0; i < evenMsgCnt.getValue(); i++) {
				M msg;
				/*objPtr = (int) (data[ptr]);
				 msg.byteDecode(objPtr, &data[ptr + 1]);
				 ptr = ptr + objPtr + 1;*/

				mInt msgSize;
				objPtr = ((int) data[ptr]);
				msgSize.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;

				msg.byteDecode(msgSize.getValue(), &data[ptr]);
				ptr = ptr + msgSize.getValue();

				evenMessageQueue->push(msg);
			}

			//a9 = clock();
			objPtr = (int) (data[ptr]);
			oddMsgCnt.byteDecode(objPtr, &data[ptr + 1]);
			ptr = ptr + objPtr + 1;
			/*std::cout << "vertex " << vertexID.getValue() << " odd =  "
			 << oddMsgCnt.getValue() << std::endl;*/
			for (int i = 0; i < oddMsgCnt.getValue(); i++) {
				M msg;
				/*objPtr = (int) (data[ptr]);
				 msg.byteDecode(objPtr, &data[ptr + 1]);
				 ptr = ptr + objPtr + 1;*/

				mInt msgSize;
				objPtr = ((int) data[ptr]);
				msgSize.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;

				msg.byteDecode(msgSize.getValue(), &data[ptr]);
				ptr = ptr + msgSize.getValue();

				oddMessageQueue->push(msg);
			}
		}
	}
	void setMigrationMark() {
		migrationMark = true;
	}
	void resetMigrationMark() {
		migrationMark = false;
	}
	bool isMigrationMarked() {
		return migrationMark;
	}
	void copyNbrs(mObject<K, V1, M> * vertex) {

		outEdges.clear();
		inEdges.clear();

		std::vector<edge<K, V1> *> * srsOutEdges = vertex->getOutEdges();
		std::vector<edge<K, V1> *> * srcInEdges = vertex->getInEdges();

		for (int i = 0; i < srsOutEdges->size(); i++) {
			outEdges.push_back(srsOutEdges->at(i));
		}
		for (int i = 0; i < srcInEdges->size(); i++) {
			inEdges.push_back(srcInEdges->at(i));
		}
	}
	void swapMessageQueue(mObject<K, V1, M> * vertex) {

		std::queue<M, std::list<M> > * tmpEvenMessageQueue =
				vertex->getMessageQueue(0);
		std::queue<M, std::list<M> > * tmpOddMessageQueue =
				vertex->getMessageQueue(1);

		vertex->setMessageQueue(0, evenMessageQueue);
		vertex->setMessageQueue(1, oddMessageQueue);

		evenMessageQueue = tmpEvenMessageQueue;
		oddMessageQueue = tmpOddMessageQueue;

		vertex->getInLocal();
		vertex->getInGlobal();
	}
}
;

#endif /* OBJECT_H_ */
