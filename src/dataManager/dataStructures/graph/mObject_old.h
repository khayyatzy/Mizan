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
//#include "../../../communication/dataStructures/MyQueue.h"

template<class K, class V1, class M>
class mObject {
private:
	K vertexID;
	V1 vertexVal;
	bool terminated;
	int curSuperStep;
	float ssResTime;

	std::vector<edge<K, V1> > outEdges;
	std::vector<edge<K, V1> > inEdges;
	//MyQueue<V> messageQueue;
//std::queue<M, std::deque<M> > * evenMessageQueue;
//	std::queue<M, std::deque<M> > * oddMessageQueue;
	std::queue<M, std::list<M> > * evenMessageQueue;
	std::queue<M, std::list<M> > * oddMessageQueue;
	//boost::mutex insert_message;
	bool migrationMark;

	//int SSMsgIOOdd;
	//int SSMsgIOEven;
	int SSMsgCntOdd;
	int SSMsgCntEven;
	int SSMsgCntOddLocal;
	int SSMsgCntEvenLocal;

	int SSMsgCntGlobal;
	int SSMsgCntLocal;

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

	void setOutLocal(int inVal) {
		SSMsgCntLocal = inVal;
	}
	void setOutGlobal(int inVal) {
		SSMsgCntGlobal = inVal;
	}
	int getOutLocal() {
		return SSMsgCntLocal;
	}
	int getOutGlobal() {
		return SSMsgCntGlobal;
	}
	int getOutDiff() {
		return (SSMsgCntGlobal-SSMsgCntLocal);
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
		SSMsgCntOdd = 0;
		SSMsgCntEven = 0;
		SSMsgCntOddLocal = 0;
		SSMsgCntEvenLocal = 0;
		//SSMsgIOOdd = 0;
		//SSMsgIOEven = 0;
	}
	virtual ~mObject() {
		delete evenMessageQueue;
		delete oddMessageQueue;
	}
	int getMessageCountGlobal() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			return SSMsgCntEven;

		} else {
			return SSMsgCntOdd;
		}
	}
	int getMessageCountGLDiff() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			return SSMsgCntEven - SSMsgCntEvenLocal;

		} else {
			return SSMsgCntOdd - SSMsgCntOddLocal;
		}
	}
	int getMessageCountLocal() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			return SSMsgCntEvenLocal;

		} else {
			return SSMsgCntOddLocal;
		}
	}
	/*int getIOCount() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			return SSMsgIOEven;

		} else {
			return SSMsgIOOdd;
		}
	}*/
	int getMessageCount() {
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			return SSMsgCntEvenLocal + SSMsgCntEven;

		} else {
			return SSMsgCntOddLocal + SSMsgCntOdd;
		}
	}
	void resetMsgCnt() {
		ssResTime = 0;
		int evenOddTest = this->curSuperStep & 1;
		if (evenOddTest == 0) {
			SSMsgCntEven = 0;
			SSMsgCntEvenLocal = 0;

		} else {
			SSMsgCntOdd = 0;
			SSMsgCntOddLocal = 0;
		}
	}
//vertex methods
	K getVertexID() {
		return vertexID;
	}
	V1 getVertexValue() {
		return vertexVal;
	}
	void setVertexValue(V1 newVal) {
		vertexVal = newVal;
	}

//edges methods
	void addOutEdge(K vertex) {
		edge<K, V1> e(vertex);
		outEdges.push_back(e);
	}
	void addInEdge(K vertex) {
		edge<K, V1> e(vertex);
		inEdges.push_back(e);
	}
	void addOutEdge(K vertex, V1 val) {
		edge<K, V1> e(vertex, val);
		outEdges.push_back(e);
	}
	void addInEdge(K vertex, V1 val) {
		edge<K, V1> e(vertex, val);
		inEdges.push_back(e);
	}
	void deleteOutEdge(K vertex) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (vertex == outEdges[i].getID()) {
				outEdges.erase(outEdges.begin() + i);
				break;
			}
		}
	}
	void deleteInEdge(K vertex) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (vertex == inEdges[i].getID()) {
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

		return outEdges[pos].getID();
	}
	K getInEdgeID(int pos) {
		return inEdges[pos].getID();
	}

//Possible sync problem
	std::vector<edge<K, V1> > * getOutEdges() {
		return &outEdges;
	}
	std::vector<edge<K, V1> > * getInEdges() {
		return &inEdges;
	}

	V1 getOutEdgeValue(int pos) {
		return outEdges[pos].getValue();
	}
	V1 getInEdgeValue(int pos) {
		return inEdges[pos].getValue();
	}
	V1 getOutEdgeValue(K vertex) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (vertex == outEdges[i].getID()) {
				return outEdges[i].getValue();
			}
		}
	}
	V1 getInEdgeValue(K vertex) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (vertex == inEdges[i].getID()) {
				return inEdges[i].getValue();
			}
		}
	}
	void setOutEdgeValue(K vertex, V1 edgeValue) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (vertex == outEdges[i].getID()) {
				outEdges[i].setValue(edgeValue);
				break;
			}
		}
	}
	void setInEdgeValue(K vertex, V1 edgeValue) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (vertex == inEdges[i].getID()) {
				inEdges[i].setValue(edgeValue);
				break;
			}
		}
	}
	bool hasOutEdge(K targetVertexId) {
		for (int i = 0; i < outEdges.size(); i++) {
			if (targetVertexId == outEdges[i].getID()) {
				return true;
			}
		}
		return false;
	}
	bool hasInEdge(K targetVertexId) {
		for (int i = 0; i < inEdges.size(); i++) {
			if (targetVertexId == inEdges[i].getID()) {
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
			SSMsgCntOdd++;
		} else {
			appendMessageInQueue(message, 0);
			SSMsgCntEven++;
		}

	}
	void appendLocalMessage(M message, int inSuperStep) {
		/*boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
		 this->insert);*/
		int evenOddTest = inSuperStep & 1;
		if (evenOddTest == 0) {
			//oddMessageQueue->push(message);
			appendMessageInQueue(message, 1);
			SSMsgCntOddLocal++;
		} else {
			appendMessageInQueue(message, 0);
			//evenMessageQueue->push(message);
			SSMsgCntEvenLocal++;
		}

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
	void setMessageQueue(int evenOdd, std::queue<M> * q) {
		if (evenOdd == 0) {
			evenMessageQueue = q;
		} else {
			oddMessageQueue = q;
		}
	}
	void setMessageQueueCopy(int evenOdd, std::queue<M> * queue) {
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
	void setSSResTime(float inSSResTime) {
		ssResTime = inSSResTime;
	}
	float getSSResTime() {
		return ssResTime;
	}
	int getCompactSize() {
		return vertexID.byteSize() + vertexVal.byteSize() + mLong().byteSize()
				+ mDouble().byteSize() + 5;
	}
	int getByteSize() {
		return vertexID.byteSize() + vertexVal.byteSize() + (9 * sizeof(char))
				+ (5 * sizeof(int)) + (outEdges.size() * vertexID.byteSize())
				+ (outEdges.size() * vertexVal.byteSize())
				+ (2 * outEdges.size()) + (inEdges.size() * vertexID.byteSize())
				+ (inEdges.size() * vertexVal.byteSize()) + (2 * inEdges.size());
	}
	int getByteSizeMsg() {
		return getByteSize() + (2 * sizeof(int))
				+ (evenMessageQueue->size() * M().byteSize()
						+ evenMessageQueue->size())
				+ (oddMessageQueue->size() * M().byteSize()
						+ oddMessageQueue->size());
	}
	char * byteEncodeCompact(int &size) {
		char * vertexEncoded = (char *) calloc(getCompactSize(), sizeof(char));

		int ptr = 0;
		int tmpSize = vertexID.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		tmpSize = vertexVal.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		if (terminated) {
			vertexEncoded[ptr++] = (char) 1;
		} else {
			vertexEncoded[ptr++] = (char) 0;
		}

		mInt css(curSuperStep);
		mDouble rt(ssResTime);

		tmpSize = css.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

		tmpSize = rt.byteEncode2(&vertexEncoded[ptr + 1]);
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

		tmpSize = vertexVal.byteEncode2(&vertexEncoded[ptr + 1]);
		vertexEncoded[ptr] = ((char) tmpSize);
		ptr = ptr + tmpSize + 1;

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
			tmpSize = outEdges[i].getID().byteEncode2(&vertexEncoded[ptr + 1]);
			vertexEncoded[ptr] = ((char) tmpSize);
			ptr = ptr + tmpSize + 1;
			if (outEdges[i].hasValue()) {
				vertexEncoded[ptr++] = (char) 1;
				tmpSize = outEdges[i].getValue().byteEncode2(
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
			tmpSize = inEdges[i].getID().byteEncode2(&vertexEncoded[ptr + 1]);
			vertexEncoded[ptr] = ((char) tmpSize);
			ptr = ptr + tmpSize + 1;
			if (inEdges[i].hasValue()) {
				vertexEncoded[ptr++] = (char) 1;
				tmpSize = inEdges[i].getValue().byteEncode2(
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
					tmpSize = evenMessageQueue->front().byteEncode2(
							&vertexEncoded[ptr + 1]);
					vertexEncoded[ptr] = ((char) tmpSize);
					ptr = ptr + tmpSize + 1;
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
					tmpSize = oddMessageQueue->front().byteEncode2(
							&vertexEncoded[ptr + 1]);
					vertexEncoded[ptr] = ((char) tmpSize);
					ptr = ptr + tmpSize + 1;
					oddMessageQueue->pop();
				}
			}
		} else {
			vertexEncoded[ptr++] = (char) 0;
		}
		//std::cout << "vertex "<< vertexID.getValue() << " byteSize()/size = " <<maxSize<<"/"<<ptr<<std::endl;
		size = ptr;
		return vertexEncoded;
	}
	void byteDecodeCompact(int size, char * data) {
		init();
		int ptr = 0;
		int objPtr = ((int) data[ptr]);
		vertexID.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		objPtr = ((int) data[ptr]);
		vertexVal.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		if (data[ptr++] == ((char) 0)) {
			terminated = false;
		} else {
			terminated = true;
		}

		mInt css;
		mDouble rt;

		objPtr = ((int) data[ptr]);
		css.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;
		curSuperStep = css.getValue();

		objPtr = ((int) data[ptr]);
		rt.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;
		ssResTime = rt.getValue();
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

		objPtr = ((int) data[ptr]);
		vertexVal.byteDecode(objPtr, &data[ptr + 1]);
		ptr = ptr + objPtr + 1;

		//std::cout << "vertex "<< vertexID.getValue() << " B ptr =  "<< ptr<<std::endl;

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

		//std::cout << "vertex "<< vertexID.getValue() << " C ptr =  "<< ptr<<std::endl;

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
				outEdges.push_back(edge<K, V1>(vertex));
			} else {
				hasValue = true;
				V1 value;
				objPtr = ((int) data[ptr]);
				value.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;
				outEdges.push_back(edge<K, V1>(vertex, value));
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
				inEdges.push_back(edge<K, V1>(vertex));
			} else {
				hasValue = true;
				V1 value;
				objPtr = ((int) data[ptr]);
				value.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;
				inEdges.push_back(edge<K, V1>(vertex, value));
			}
		}

		//const clock_t a10 = clock();
		//std::cout << "vertex "<< vertexID.getValue() << " de ptr =  "<< ptr<<std::endl;

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

			for (int i = 0; i < evenMsgCnt.getValue(); i++) {
				M msg;
				objPtr = (int) (data[ptr]);
				msg.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;
				evenMessageQueue->push(msg);
			}

			//a9 = clock();
			objPtr = (int) (data[ptr]);
			oddMsgCnt.byteDecode(objPtr, &data[ptr + 1]);
			ptr = ptr + objPtr + 1;
			for (int i = 0; i < oddMsgCnt.getValue(); i++) {
				M msg;
				objPtr = (int) (data[ptr]);
				msg.byteDecode(objPtr, &data[ptr + 1]);
				ptr = ptr + objPtr + 1;
				oddMessageQueue->push(msg);
			}
//			std::cout << "vertex " << vertexID.getValue() << " out queues "
//					<< evenMsgCnt.getValue() << " " << oddMsgCnt.getValue()
//					<< std::endl;
//			std::cout << "vertex " << vertexID.getValue() << " out size queues "
//					<< evenMessageQueue->size() << " "
//					<< oddMessageQueue->size() << std::endl;

		}
		//const clock_t a11 = clock();

		//std::cout <<
		//" a2-a1" << float(a2 - a1) / CLOCKS_PER_SEC <<
		//" a3-a2" << float(a3 - a2) / CLOCKS_PER_SEC <<
		//" a4-a3" << float(a4 - a3) / CLOCKS_PER_SEC <<
		//" a5-a4" << float(a5 - a4) / CLOCKS_PER_SEC <<
		//" a6-a5" << float(a6 - a5) / CLOCKS_PER_SEC <<
		//" a7-a6" << float(a7 - a6) / CLOCKS_PER_SEC <<
		//" a8-a7" << float(a8 - a7) / CLOCKS_PER_SEC <<
		//" a9-a8 " << float(a9 - a8) / CLOCKS_PER_SEC <<
		//" a11-a9 " << float(a11 - a9) / CLOCKS_PER_SEC <<
		//" a11-a10 " << float(a11 - a10) / CLOCKS_PER_SEC << std::endl;
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
	void swapMessageQueue(mObject<K, V1, M> * vertex) {
		std::queue<M> * tmpEvenMessageQueue = vertex->getMessageQueue(0);
		std::queue<M> * tmpOddMessageQueue = vertex->getMessageQueue(1);

		vertex->setMessageQueue(0, evenMessageQueue);
		vertex->setMessageQueue(1, oddMessageQueue);

		evenMessageQueue = tmpEvenMessageQueue;
		oddMessageQueue = tmpOddMessageQueue;
	}
}
;

#endif /* OBJECT_H_ */
