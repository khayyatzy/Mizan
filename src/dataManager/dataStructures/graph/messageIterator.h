/*
 * messageIterator.h
 *
 *  Created on: May 22, 2012
 *      Author: refops
 */

#ifndef MESSAGEITERATOR_H_
#define MESSAGEITERATOR_H_

#include <queue>
#include <list>

template<class M>
class messageIterator {
private:
	std::queue<M, std::list<M> > * msgPointer;
public:

	messageIterator(std::queue<M, std::list<M> > * inMsgPointer) {
		msgPointer = inMsgPointer;
		//it = msgPointer->begin();
	}
	bool hasNext() {
		//return it < msgPointer->end();
		return !msgPointer->empty();
	}
	M getNext() {
		//V value = *(it);
		//it++;
		//return value;
		M value = msgPointer->front();
		msgPointer->pop();
		return value;
	}
	int getSize() {
		return msgPointer->size();
	}

	virtual ~messageIterator() {
	}
};

#endif /* MESSAGEITERATOR_H_ */
