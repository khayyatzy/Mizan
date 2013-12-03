/*
 * IdataType.cpp
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#include "IdataType.h"

IdataType::IdataType() {
	// TODO Auto-generated constructor stub
}

IdataType::~IdataType() {
	// TODO Auto-generated destructor stub
}

std::size_t IdataType::local_hash_value() const{
	return 0;
}

std::size_t hash_value(const IdataType& p) {

	return p.local_hash_value();
}
