/*
 * unitTest.cpp
 *
 *  Created on: Jul 18, 2012
 *      Author: refops
 */

#include "unitTest.h"
#include "dataManager/dataStructures/graph/mObject.h"
#include "dataManager/dataStructures/data/mDoubleArray.h"

unitTest::unitTest() {
	/*
	mLong key(101785);
	mDouble * array = new mDouble[2]; //{mDouble(1.1785),mDouble(0)};
	array[0] = mDouble(1.1785);
	array[1] = mDouble(0);
	mDoubleArray value(2, array);

	int mySize = 0;
	char * baba = (char *) calloc(100, sizeof(char));
	mySize = value.byteEncode2(baba);
	mDoubleArray value2;
	value2.byteDecode(mySize, baba);

	std::cout << "value.toString(): " << value.toString() << std::endl;
	std::cout << "value2.toString(): " << value2.toString() << std::endl;

	mObject<mLong, mDoubleArray, mDoubleArray> vertex(key, value);
	vertex.addInEdge(mLong(500), value);
	vertex.addInEdge(mLong(501));
	vertex.addInEdge(mLong(510), value);

	vertex.addOutEdge(mLong(511), value);
	vertex.addInEdge(mLong(521));
	vertex.addInEdge(mLong(530), value);

	vertex.startNewSS();
	vertex.appendLocalMessage(value, 1);
	vertex.appendLocalMessage(value, 1);
	vertex.appendLocalMessage(value, 1);

	vertex.startNewSS();
	vertex.appendLocalMessage(value, 2);
	vertex.appendLocalMessage(value, 2);
	vertex.appendLocalMessage(value, 2);
	vertex.appendLocalMessage(value, 2);

	/*std::cout << "even size = " << vertex.getMessageQueue(0)->size()
	 << std::endl;
	 std::cout << "odd size = " << vertex.getMessageQueue(1)->size()
	 << std::endl;*/

	//int ssize = 0;
	//char * verObj = vertex.byteEncode(ssize, true);

	/*std::cout << "even size after en= " << vertex.getMessageQueue(0)->size()
	 << std::endl;
	 std::cout << "odd size after en= " << vertex.getMessageQueue(1)->size()
	 << std::endl;*/

	mObject<mLong, mDoubleArray, mDoubleArray> vertex2;
	//vertex2.byteDecode(ssize, verObj);

	/*std::cout << vertex2.getVertexID().getValue() << std::endl;
	 std::cout << vertex2.getVertexValue().toString() << std::endl;*/

	/*for (int i = 0; i < vertex2.getInEdgeCount(); i++) {
		std::cout << vertex2.getInEdgeID(i).getValue() << " "
		 << vertex2.getInEdgeValue(i).toString() << std::endl;
	}*/
	/*for (int i = 0; i < vertex2.getOutEdgeCount(); i++) {
		std::cout << vertex2.getOutEdgeID(i).getValue() << " "
		 << vertex2.getOutEdgeValue(i).toString() << std::endl;
	}*/

	/*std::queue<mDoubleArray> * qe = vertex2.getMessageQueue(0);
	while (!qe->empty()) {
		std::cout << "even " << qe->front().toString() << std::endl;
		qe->pop();
	}

	qe = vertex2.getMessageQueue(1);
	while (!qe->empty()) {
		std::cout << "odd " << qe->front().toString() << std::endl;
		qe->pop();
	}*/

	//std::cout << "SS = " << vertex2.getCurrentSS() << std::endl;
}

unitTest::~unitTest() {
	// TODO Auto-generated destructor stub
}

