/*
 * MyQueue.h
 *
 *  Created on: May 19, 2012
 *      Author: awaraka
 */

#ifndef MYQUEUEX_H_
#define MYQUEUEX_H_

#include "general.h"
#include "boost/thread.hpp"

//thread safe queue

template <class T>
class myqueue {
private:

	queue<T> localqueue;
	boost::mutex insert;
	boost::mutex retrieve;

public:
	myqueue(){}
	~myqueue(){}
	void enqueue(T in_value) {
		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
				this->insert);
		localqueue.push(in_value);
		insert_lock.unlock();
	}
	bool dequeue(T& temp) {
		boost::mutex::scoped_lock retrieve_lock = boost::mutex::scoped_lock(
						this->insert);

		if(localqueue.empty())
		{
			return false;
		}
		temp = localqueue.front();
		localqueue.pop();
		retrieve_lock.unlock();
		return true;
	}

	bool isQeueEmpty()
	{
		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
						this->insert);
		bool flag = localqueue.empty();
		insert_lock.unlock();

		return flag;

	}



};

#endif /* MYQUEUE_H_ */
