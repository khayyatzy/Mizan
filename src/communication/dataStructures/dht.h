/*
 * dht.h
 *
 *  Created on: Apr 23, 2012
 *      Author: awaraka
 */

#ifndef DHT_H_
#define DHT_H_

//#include <sparsehash/dense_hash_map>
#include <boost/unordered_map.hpp>
#include "../../dataManager/dataStructures/data/mLong.h"
#include "../commManager.h"
#include "../sysComm.h"
#include "boost/thread.hpp"

using namespace std;

template<class K, class V1, class M, class A> class userComm;
template<class K, class V1, class M, class A> class sysComm;

template<class K, class V1, class M, class A>
class DHT {

	//typedef google::dense_hash_map<K, int, K, K> map_t;
	typedef boost::unordered_map<K, int> map_t;
	typedef typename map_t::iterator iterator_t;
	typedef typename std::pair<iterator_t, bool> pair_t;
	map_t local_dht;
	sysComm<K, V1, M, A>* mySysComm;
	boost::mutex insert_m;
	boost::mutex retrieve_m;

public:

//	GDHT() {
//			local_dht.set_empty_key(NULL);
//		}

	void MapSysComm(sysComm<K, V1, M, A>* comm) {

		mySysComm = comm;
	}

	void insert_KeyVal(K e1, int v1) {

		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
				this->insert_m);

		pair_t ret;
		local_dht[e1] = v1;
		insert_lock.unlock();

	}

	int retrieve_val(K e1) {
		iterator_t element_it;
		boost::mutex::scoped_lock retrieve_lock = boost::mutex::scoped_lock(
				this->insert_m);
		element_it = local_dht.find(e1);

		if (element_it == local_dht.end()) {
			retrieve_lock.unlock();
			return -1;
		} else {
			retrieve_lock.unlock();
			return element_it->second;
		}
	}

	void erase_key(const K& e1) {
		boost::mutex::scoped_lock erase_lock = boost::mutex::scoped_lock(
				this->insert_m);
		local_dht.erase(e1);
		erase_lock.unlock();
	}


};

#endif /* DHT_H_ */
