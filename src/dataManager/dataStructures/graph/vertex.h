/*
 * vertex.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef VERTEX_H_
#define VERTEX_H_

#include "Ielement.h"

template <class K,class V1>
class vertex : public Ielement<K,V1>{
private:
	V1	oldValue;
	V1	curValue;
public:
	vertex();
	virtual ~vertex();
	//methods inherited from Ielement
	K getID();
	V1 getValue();
	void	  setID(K id);
	void	  setValue(V1 value);
};

#endif /* VERTEX_H_ */

