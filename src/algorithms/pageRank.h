/*
 * pageRank.h
 *
 *  Created on: Sep 18, 2012
 *      Author: refops
 */

#ifndef PAGERANK_H_
#define PAGERANK_H_

#include "../IsuperStep.h"
#include "../Icombiner.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../dataManager/dataStructures/data/mDouble.h"

class pageRankCombiner: public Icombiner<mLong, mDouble, mDouble, mLong> {

	void combineMessages(mLong dst, messageIterator<mDouble> * messages,
			messageManager<mLong, mDouble, mDouble, mLong> * mManager) {
		double newVal = 0;
		while (messages->hasNext()) {
			double tmp = messages->getNext().getValue();
			newVal = newVal + tmp;
		}
		mDouble messageOut(newVal);
		mManager->sendMessage(dst, messageOut);
	}
};

class pageRank: public IsuperStep<mLong, mDouble, mDouble, mLong> {
private:
	int vertexTotal;
	double error;
	int maxSuperStep;

public:

	pageRank(int maxSS) {
		vertexTotal = 0;
		error = 0;
		maxSuperStep = maxSS;
	}
	void initialize(userVertexObject<mLong, mDouble, mDouble, mLong> * data) {
		if (vertexTotal == 0) {
			vertexTotal = data->getGlobalVertexCount();
			error = 1.0 / (double) vertexTotal / 10.0;
		}
		vertexTotal++;
		data->setVertexValue(mDouble(1.0 / (double) vertexTotal));
	}
	void compute(messageIterator<mDouble> * messages,
			userVertexObject<mLong, mDouble, mDouble, mLong> * data,
			messageManager<mLong, mDouble, mDouble, mLong> * comm) {

		double currVal = data->getVertexValue().getValue();
		double newVal = 0;
		double c = 0.85;

		if (data->getCurrentSS() > 1) {
			while (messages->hasNext()) {
				double tmp = messages->getNext().getValue();
				newVal = newVal + tmp;
			}
			newVal = newVal * c + (1.0 - c) / ((double) vertexTotal);
			data->setVertexValue(mDouble(newVal));
		} else newVal = currVal;

		if (data->getCurrentSS() <= maxSuperStep) {
			mDouble outVal(newVal / ((double) data->getOutEdgeCount()));
			for (int i = 0; i < data->getOutEdgeCount(); i++) {
				comm->sendMessage(data->getOutEdgeID(i), outVal);
			}
		}else data->voteToHalt();
		if ((abs(currVal - newVal) < error && data->getCurrentSS() > 1)){
			data->voteToHalt();
		}
	}
};
#endif /* PAGERANK_H_ */
