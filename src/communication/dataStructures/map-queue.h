/*
 * map-queue.h
 *
 *  Created on: Jul 15, 2012
 *      Author: awaraka
 */

#ifndef MAP_QUEUE_H_
#define MAP_QUEUE_H_

#include <boost/unordered_map.hpp>
#include "../commManager.h"
#include "boost/thread.hpp"
#include "../../dataManager/dataStructures/data/mLong.h"
#include <queue>
#include"../../dataManager/dataStructures/data/mCharArrayNoCpy.h"

using namespace std;

template<class K, class V1, class M, class A>
class KeyQueMap {

	typedef boost::unordered_map<K, queue<mCharArrayNoCpy>*> map_t;
	typedef typename map_t::iterator iterator_t;
	typedef typename std::pair<iterator_t, bool> pair_t;
	map_t localMap;
	boost::mutex insert_m;

public:

	void insert_KeyVal(K e1, mCharArrayNoCpy obj) {

		iterator_t ret;
		try {
			boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
					this->insert_m);

			iterator_t it = localMap.find(e1);
			if (it != localMap.end()) {
				it->second->push(obj);

			} else {

				queue<mCharArrayNoCpy>* q = new queue<mCharArrayNoCpy>();
				q->push(obj);
				localMap[e1] = q;

			}

			insert_lock.unlock();
		} catch (boost::lock_error & e) {
			cout << "ERROR map queue: BOOST LOCK ERROR" << endl;
			exit(0);
		}

	}

	std::vector<mCharArrayNoCpy> retrieve_val(K e1) {

//		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
//				this->insert_m);
//		iterator_t it = localMap.find(e1);
//		if (it != localMap.end()) {
//			insert_lock.unlock();
//			return it->second;
//		} else {
//			queue<mCharArrayNoCpy>* q = new queue<mCharArrayNoCpy>();
//			insert_lock.unlock();
//			return q;
//		}
		std::vector<mCharArrayNoCpy> p_array;

		try {

			boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
					this->insert_m);
			iterator_t it = localMap.find(e1);
			if (it != localMap.end()) {
				while (!it->second->empty()) {
					p_array.push_back(it->second->front());
					it->second->pop();
				}
			}
			insert_lock.unlock();
			return p_array;

		} catch (boost::lock_error & e) {
			cout << "ERROR map queue: BOOST LOCK ERROR" << endl;
			exit(0);
		}

	}

	std::vector<K> retrieve_LeftoverKeys() {
		vector<K> LeftoverKeys;

		for (iterator_t it = localMap.begin(); it != localMap.end(); ++it)
			LeftoverKeys.push_back(it->first);

	}

};

#endif /* MAP_QUEUE_H_ */
