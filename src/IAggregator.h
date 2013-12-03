/*
 * messageAggregator.h
 *
 *  Created on: May 22, 2012
 *      Author: refops
 */

#ifndef IAGGREGATOR_H_
#define IAGGREGATOR_H_

template <class A>
class IAggregator {
protected:
	A aggValue;
public:
	IAggregator(){}
	virtual void aggregate(A value)=0;
	A getValue(){
		return aggValue;
	}
	void setValue(A value){
		this->aggValue = value;
	}
	virtual ~IAggregator(){}
};

#endif /* IAGGREGATOR_H_ */
