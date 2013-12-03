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

class dimEst: public IsuperStep<mLong, mLongArray, mLongArray, mLong> {
private:
	int maxSuperStep;
	int k;
	boost::mt19937 * generator;
	boost::uniform_real<> * uni_dist;
	boost::variate_generator<boost::mt19937&, boost::uniform_real<> > * uni;
	const static long long v62 = 62;
	const static long long v1 = 1;

public:
	dimEst(int inMaxSS) {
		k = 16;
		maxSuperStep = inMaxSS;

		generator = new boost::mt19937(std::time(0));
		uni_dist = new boost::uniform_real<>(0, 1);
		uni = new boost::variate_generator<boost::mt19937&,
				boost::uniform_real<> >(*generator, *uni_dist);
	}
	void initialize(
			userVertexObject<mLong, mLongArray, mLongArray, mLong> * data) {
		mLong * value = new mLong[k];
		int finalBitCount = 63;
		long rndVal = 0;
		for (int j = 0; j < k; j++) {
			rndVal = create_random_bm(finalBitCount);
			value[j].setValue((v1 << (v62 - rndVal)));
		}
		mLongArray valueArray(k, value);
		data->setVertexValue(valueArray);
	}
	void compute(messageIterator<mLongArray> * messages,
			userVertexObject<mLong, mLongArray, mLongArray, mLong> * data,
			messageManager<mLong, mLongArray, mLongArray, mLong> * comm) {

		mLong * newBitMask = new mLong[k];
		mLong * oldBitMask = data->getVertexValue().getArray();

		for (int i = 0; i < k; i++) {
			newBitMask[i] = oldBitMask[i];
		}

		mLongArray tmpArray;
		mLong * tmpBitMask;

		long long a;
		long long b;
		long long c;
		while (messages->hasNext()) {
			tmpArray = messages->getNext();
			tmpBitMask = tmpArray.getArray();
			for (int i = 0; i < k; i++) {
				a = newBitMask[i].getValue();
				b = tmpBitMask[i].getValue();
				c = a | b;
				newBitMask[i].setValue(c);
			}
		}

		mLongArray outArray(k, newBitMask);
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

//Src: Pegasus
	int create_random_bm(int size_bitmask) {

		int j;

		// cur_random is between 0 and 1.
		double cur_random = uni->operator ()(); //rand.nextDouble(); //Math.random();
		double threshold = 0;
		for (j = 0; j < size_bitmask - 1; j++) {
			threshold += pow(2.0, -1 * j - 1);

			if (cur_random < threshold) {
				break;
			}
		}

		return j;
	}
};
#endif /* DIMEST_H_ */
