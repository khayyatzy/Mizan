/*
 * gdht.h
 *
 *  Created on: May 13, 2012
 *      Author: awaraka
 */

#ifndef GDHT_H_
#define GDHT_H_

/*
#include <sparsehash/dense_hash_map>
#include <boost/unordered_map.hpp>
#include "../../dataManager/dataStructures/data/mLong.h"
#include "../commManager.h"

using namespace std;

template<class K, class V> class comm_manager;

template<class K, class V>
class GDHT {

	typedef google::dense_hash_map<K, int, K, K> map_t;
	typedef typename map_t::iterator iterator_t;
	typedef typename std::pair<iterator_t, bool> pair_t;

	map_t local_dht;

	comm_manager<K, V>* commMang;
	//boost::mutex insert_m;

public:

	GDHT() {
		local_dht.set_empty_key(NULL);
	}

	void MapCommMananger(comm_manager<K, V>* comm) {
		commMang = comm;
	}

	bool insert_KeyVal(K e1, int v1) {

//		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
//				insert_m);

		pair_t ret;
		ret = local_dht.insert(make_pair(e1, v1));
		if (ret.second == false) {
			return false;
		} else {
			return true;
		}

//		insert_lock.unlock();
	}

	int retrieve_val(K e1) {
		iterator_t element_it;
		element_it = local_dht.find(e1);

		if (element_it == local_dht.end()) {
			return -1;
		} else {
			return element_it->second;
		}
	}

	void MAP_LocalVertices(K vertex_id) {

		int size, myrank;
		char* c;
		myrank = commMang->getRank();
		long mod = vertex_id.local_hash_value() % commMang->getPsize();

		if (myrank != mod) {

			c = vertex_id.byteEncode(size);
//			cout << "(" << myrank << ") HASH#: " << vertex_id.local_hash_value()
//					<< " - SIZE: " << size << " - LOC#: " << mod << endl;
//			cout.flush();
			commMang->load_RemoteDHTValue(c, size, mod);
			free(c);
		} else {
			insert_KeyVal(vertex_id, mod);
		}

	}

};
*/
#endif /* GDHT_H_ */
