/*
 * thread_manager.h
 *
 *  Created on: Apr 17, 2012
 *      Author: awaraka
 */

#ifndef THREAD_MANAGER_H_
#define THREAD_MANAGER_H_

#include <boost/threadpool.hpp>
//#include <boost-1_47/boost/threadpool.hpp>



using namespace boost::threadpool;


class thread_manager {

private:

public:
	pool tp;
	void threadpool_init(int thread_count) {
		tp.size_controller().resize(thread_count);
	}

};

#endif /* THREAD_MANAGER_H_ */
