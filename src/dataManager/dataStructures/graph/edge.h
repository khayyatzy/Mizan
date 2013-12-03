/*
 * edge.h
 *
 *  Created on: Mar 29, 2012
 *      Author: refops
 */

#ifndef EDGE_H_
#define EDGE_H_
#include "Ielement.h"

template<class K, class V1>
class edge: public Ielement<K, V1> {

private:
	bool ihaveValue;
public:
	edge() :
			Ielement<K, V1>() {
		ihaveValue = false;
	}
	edge(K vertex) :
			Ielement<K, V1>(vertex) {
		ihaveValue = false;
	}
	edge(K vertex, V1 value) :
			Ielement<K, V1>(vertex, value) {
		ihaveValue = true;
	}
	virtual ~edge() {

	}
	//methods inherited from Ielement
	K getID() {
		return this->id;
	}
	V1 getValue() {
		return this->value;
	}
	void setID(K inId) {
		this->id = inId;
	}
	void setValue(V1 inValue) {
		ihaveValue = true;
		this->value = inValue;
	}

	bool hasValue() {
		return ihaveValue;
	}
};

#endif /* EDGE_H_ */
