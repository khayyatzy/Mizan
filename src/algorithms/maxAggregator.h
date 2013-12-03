/*
 * maxAggregator.h
 *
 *  Created on: Mar 6, 2013
 *      Author: refops
 */

#ifndef MAXAGGREGATOR_H_
#define MAXAGGREGATOR_H_

#include "../IAggregator.h"

class maxAggregator: public IAggregator<mLong> {
public:
	maxAggregator() {
		aggValue.setValue(0);
	}
	void aggregate(mLong value) {
		if (value > aggValue) {
			aggValue = value;
		}
	}
	mLong getValue() {
		return aggValue;
	}
	void setValue(mLong value) {
		this->aggValue = value;
	}
	virtual ~maxAggregator() {
	}
};

#endif /* MAXAGGREGATOR_H_ */
