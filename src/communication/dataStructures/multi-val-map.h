/*
 * multi-val-map.h
 *
 *  Created on: May 14, 2012
 *      Author: awaraka
 */

#ifndef MULTI_VAL_MAP_H_
#define MULTI_VAL_MAP_H_

//include <sparsehash/dense_hash_map>
#include <boost/unordered_map.hpp>
#include "../../dataManager/dataStructures/data/mLong.h"
#include "../commManager.h"
#include "boost/thread.hpp"

using namespace std;

template<class K, class V1, class M, class A> class comm_manager;
template<class K, class V1, class M, class A>
class MultiValMap {

	typedef boost::unordered_multimap<K, std::pair<char*, int> > map_t;
	typedef typename map_t::iterator iterator_t;
	typedef typename std::pair<iterator_t, bool> pair_t;
	map_t localMap;
	comm_manager<K, V1, M, A>* commMang;
	boost::mutex insert_m;

public:

	void MapCommMananger(comm_manager<K, V1,M, A> * comm) {
		commMang = comm;
	}

	void insert_KeyVal(K e1, char* v1, int size) {

		iterator_t ret;
		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
				this->insert_m);
		ret = localMap.insert(make_pair(e1, make_pair(v1, size)));
		insert_lock.unlock();

	}

	void erase_key(const K& e1) {
		boost::mutex::scoped_lock erase_lock = boost::mutex::scoped_lock(
				this->insert_m);

		iterator_t it = localMap.find(e1);
		while (it != localMap.end()) {
			localMap.erase(e1);
			it = localMap.find(e1);
		}

		erase_lock.unlock();
	}

	std::vector<std::pair<char*, int> > retrieve_val(K e1) {

		std::vector<std::pair<char*, int> > p_array;

		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
				this->insert_m);
		iterator_t it = localMap.find(e1);
		while (it != localMap.end() && it->first == e1) {
			p_array.push_back((it->second));
			it++;
		}
		insert_lock.unlock();

		return p_array;

	}

};

#endif /* MULTI_VAL_MAP_H_ */
