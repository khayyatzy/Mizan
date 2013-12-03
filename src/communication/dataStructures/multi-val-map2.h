/*
 * multi-val-map2.h
 *
 *  Created on: Jun 27, 2012
 *      Author: awaraka
 */

#ifndef MULTI_VAL_MAP2_H_
#define MULTI_VAL_MAP2_H_

#include <boost/unordered_map.hpp>
#include "../commManager.h"
#include "boost/thread.hpp"

using namespace std;

template<class K, class V1, class M, class A> class comm_manager;
template<class K, class V1, class M, class A>
class MultiValMap2 {

	typedef boost::unordered_multimap<K, int> map_t;
	typedef typename map_t::iterator iterator_t;
	typedef typename std::pair<iterator_t, bool> pair_t;
	map_t localMap;
	comm_manager<K, V1, M, A>* commMang;
	boost::mutex insert_m;

public:

	void MapCommMananger(comm_manager<K, V1,M, A> * comm) {
		commMang = comm;
	}

	void insert_KeyVal(K e1, int PE) {

		iterator_t ret;

		try {
			boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
					this->insert_m);
			ret = localMap.insert(make_pair(e1, PE));
			insert_lock.unlock();
		} catch (boost::lock_error & e) {
			cout << "ERROR map 2: BOOST LOCK ERROR" << endl;
			exit(0);
		}

	}

	std::vector<int> retrieve_val(K e1) {

		std::vector<int> p_array;

		try {
			boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
					this->insert_m);
			iterator_t it = localMap.find(e1);
			while (it != localMap.end() && it->first == e1) {
				p_array.push_back((it->second));
				it++;
			}
			insert_lock.unlock();
			return p_array;
		} catch (boost::lock_error & e) {
			cout << "ERROR map 2: BOOST LOCK ERROR" << endl;
			exit(0);
		}

	}

};

#endif /* MULTI_VAL_MAP2_H_ */
