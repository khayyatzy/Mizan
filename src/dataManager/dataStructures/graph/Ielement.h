/*
 * element.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef ELEMENT_H_
#define ELEMENT_H_

#include "../data/IdataType.h"

template <class K,class V1>
class Ielement {
protected:
	K	id;
	V1	value;
public:
	Ielement():id(),value(){
	}
	Ielement(K inId):id(inId),value(){
	}
	Ielement(K inId,V1 inValue):id(inId),value(inValue){
	}
	virtual K getID()=0;
	virtual	V1 getValue()=0;
	virtual	void setID(K id)=0;
	virtual	void setValue(V1 value)=0;
	virtual ~Ielement(){

	}
};

#endif /* ELEMENT_H_ */
