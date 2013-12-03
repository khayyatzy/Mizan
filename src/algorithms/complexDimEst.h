/*
 * dimEst.h
 *
 *  Created on: Sep 17, 2012
 *      Author: refops
 */

#ifndef DIMEST_H_
#define DIMEST_H_

#include "../IsuperStep.h"
#include "../dataManager/dataStructures/data/mLongArray.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../dataManager/dataStructures/data/mInt.h"
#include <boost/random/linear_congruential.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
#include <boost/generator_iterator.hpp>
#include <boost/random/mersenne_twister.hpp>

class complexDimEst: public IsuperStep<mLong, mDoubleArray, mDoubleArray, mLong> {
private:
	int maxSuperStep;
	int k;
	boost::mt19937 * generator;
	boost::uniform_real<> * uni_dist;
	boost::variate_generator<boost::mt19937&, boost::uniform_real<> > * uni;
	const static long long v62 = 62;
	const static long long v1 = 1;

public:
	complexDimEst(int inMaxSS) {
		k = 16;
		maxSuperStep = inMaxSS;

		generator = new boost::mt19937(std::time(0));
		uni_dist = new boost::uniform_real<>(0, 1);
		uni = new boost::variate_generator<boost::mt19937&,
				boost::uniform_real<> >(*generator, *uni_dist);
	}
	void initialize(
			userVertexObject<mLong, mDoubleArray, mDoubleArray, mLong> * data) {
		mDouble * value = new mDouble[k];
		double rndVal = 0;
		for (int j = 0; j < k; j++) {
			rndVal =  uni->operator ()();
			value[j].setValue(rndVal);
		}
		mDoubleArray valueArray(k, value);
		data->setVertexValue(valueArray);
	}
	void compute(messageIterator<mLongArray> * messages,
			userVertexObject<mLong, mDoubleArray, mDoubleArray, mLong> * data,
			messageManager<mLong, mDoubleArray, mDoubleArray, mLong> * comm) {

		mDouble * newBitMask = new mDouble[k];
		mDouble * oldBitMask = data->getVertexValue().getArray();

		for (int i = 0; i < k; i++) {
			newBitMask[i].setValue(0);
		}

		mDoubleArray tmpArray;
		mDouble * tmpBitMask;

		double a;
		double b;
		double c;
		while (messages->hasNext()) {
			tmpArray = messages->getNext();
			tmpBitMask = tmpArray.getArray();
			for (int i = 0; i < k; i++) {
				a = oldBitMask[i].getValue();
				b = tmpBitMask[i].getValue();
				c = (a * b) + newBitMask[i].getValue();
				newBitMask[i].setValue(c);
			}
		}

		mDoubleArray outArray(k, newBitMask);
		for (int i = 0; i < data->getInEdgeCount(); i++) {
			comm->sendMessage(data->getInEdgeID(i), outArray);
		}
		/*for (int i = 0; i < data->getOutEdgeCount(); i++) {
		 comm->sendMessage(data->getOutEdgeID(i), outArray);
		 }*/
		if ((outArray == data->getVertexValue() && data->getCurrentSS() != 1) || data->getCurrentSS() > maxSuperStep) {
			data->voteToHalt();
		}
		data->setVertexValue(outArray);
	}
};
#endif /* DIMEST_H_ */
