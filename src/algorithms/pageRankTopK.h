/*
 * pageRank.h
 *
 *  Created on: Sep 18, 2012
 *      Author: refops
 */

#ifndef PAGERANKTOPK_H
#define PAGERANKTOPK_H

#include "../IsuperStep.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../dataManager/dataStructures/data/mDouble.h"

class pageRankTopK: public IsuperStep<mLong, mDoubleArray, mDouble, mLong> {
private:
	int vertexTotal; // = 10000;
	double error; // = 1/vertexTotal;
	int prMaxSuperStep;
	int topK;
	int topKMaxSS;

public:
	pageRankTopK(int prMaxSS, int k,int inTopKMaxSS) {
		vertexTotal = 0;
		error = 0;
		prMaxSuperStep = prMaxSS;
		topK = k;
		topKMaxSS = inTopKMaxSS;
	}
	void initialize(
			userVertexObject<mLong, mDoubleArray, mDouble, mLong> * data) {
		if (vertexTotal == 0) {
			vertexTotal = data->getGlobalVertexCount();
			error = 1.0 / (double) vertexTotal / 10.0;
		}
		mDouble * arrayVals = new mDouble[1];
		arrayVals[0].setValue(0);
		mDoubleArray value(1, arrayVals);
		data->setVertexValue(value);
	}
	void compute(messageIterator<mDouble> * messages,
			userVertexObject<mLong, mDoubleArray, mDouble, mLong> * data,
			messageManager<mLong, mDoubleArray, mDouble, mLong> * comm) {

		if (data->getCurrentSS() < prMaxSuperStep) {
			doPageRank(messages, data, comm);
		} else {
			doTopKSearch(messages, data, comm);
		}
	}
	void doTopKSearch(messageIterator<mDouble> * messages,
			userVertexObject<mLong, mDoubleArray, mDouble, mLong> * data,
			messageManager<mLong, mDoubleArray, mDouble, mLong> * comm) {

		bool change = false;

		vector<double> myVec;
		for (int i = 0; i < data->getVertexValue().getSize(); i++) {
			myVec.push_back(data->getVertexValue().getArray()[i].getValue());
		}

		while (messages->hasNext()) {
			double tmp = messages->getNext().getValue();
			if (myVec.size() < topK) {
				myVec.push_back(tmp);
				change = true;
			} else {
				for (int i = 0; i < myVec.size(); i++) {
					if (tmp > myVec[i]) {
						change = true;
						myVec[i] = tmp;
						break;
					}
				}
			}
		}
		if (change) {
			for (int i = 0; i < data->getInEdgeCount(); i++) {
				for (int j = 0; j < myVec.size(); j++) {
					mDouble outVal(myVec[j]);
					comm->sendMessage(data->getInEdgeID(i), outVal);
				}
			}
		}

		mDouble * arrayVVals = new mDouble[myVec.size()];
		for (int i = 0; i < myVec.size(); i++) {
			arrayVVals[i].setValue(myVec[i]);
		}
		mDoubleArray newVVal(myVec.size(), arrayVVals);

		data->setVertexValue(newVVal);

		if (!change || data->getCurrentSS() > topKMaxSS) {
			data->voteToHalt();
		}
	}
	void doPageRank(messageIterator<mDouble> * messages,
			userVertexObject<mLong, mDoubleArray, mDouble, mLong> * data,
			messageManager<mLong, mDoubleArray, mDouble, mLong> * comm) {
		double currVal = data->getVertexValue().getArray()[0].getValue();
		double newVal = 0;
		double c = 0.85;
		while (messages->hasNext()) {
			double tmp = messages->getNext().getValue();
			newVal = newVal + tmp;
		}

		newVal = newVal * c + (1.0 - c) / ((double) vertexTotal);
		mDouble outVal(newVal / ((double) data->getOutEdgeCount()));
		for (int i = 0; i < data->getOutEdgeCount(); i++) {
			comm->sendMessage(data->getOutEdgeID(i), outVal);
		}

		mDouble * arrayVVals = new mDouble[1];
		arrayVVals[0].setValue(newVal);
		mDoubleArray newVVal(1, arrayVVals);

		data->setVertexValue(newVVal);
	}
};
#endif /* PAGERANK_H_ */
