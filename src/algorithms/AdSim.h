#ifndef ADSIM_H_
#define ADSIM_H_
//
#include "../IsuperStep.h"
#include "../dataManager/dataStructures/data/mLongArray.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../dataManager/dataStructures/data/mInt.h"

//synthetic algorithm 1
class AdSim: public IsuperStep<mLong, mLong, mLongArray, mLong> {
public:

	int maxSS;
	bool selected;
	AdSim(int inMaxSS) {
		srand(101785);
		maxSS = inMaxSS;
		selected = false;
	}

	void initialize(userVertexObject<mLong, mLong, mLongArray, mLong> * data) {
		mLong female(+1);
		mLong male(-1);
		mLong ad(2);

		if (!selected) {
			if (data->getOutEdgeCount() > 0) {
				data->setVertexValue(ad);
				selected = true;
			} else if (data->getVertexID().getValue() % 3 == 0) {
				data->setVertexValue(male);
			} else {
				//std::cout << "Yes true!" << std::endl;
				data->setVertexValue(female);
			}
		}
		else if (data->getVertexID().getValue() % 3 == 0) {
			data->setVertexValue(male);
		} else {
			//std::cout << "Yes true!" << std::endl;
			data->setVertexValue(female);
		}

		/*for (int j = 1; j <= 10; j++) {
		 long randval = rand() % 1000;
		 val[j] = randval;
		 }*/

		//mLongArray valueArray(11, val);
	}

	void compute(messageIterator<mLongArray> * messages,
			userVertexObject<mLong, mLong, mLongArray, mLong> * data,
			messageManager<mLong, mLong, mLongArray, mLong> * comm) {

		if (data->getCurrentSS() > maxSS) {
			data->voteToHalt();
		} else if (data->getVertexID().getValue() == 2) {
			if (data->getCurrentSS() % 3 == 1) {
				mLong type(1); // 1: ads or recommended , 2: like, 3: reject,
				mLong msgVal(rand() % 1000);
				mLong *msgArr = new mLong[3];
				msgArr[0] = type;
				msgArr[1] = msgVal;
				msgArr[2].setValue(rand() % 6);
				mLongArray newAds(3, msgArr);

				for (int j = 0; j < data->getOutEdgeCount(); j++) {
					comm->sendMessage(data->getOutEdgeID(j), newAds);
				}
			}
			while (messages->hasNext()) {
				messages->getNext();
			}
		} else if (data->getVertexValue().getValue() > 0) {
			while (messages->getSize() > (data->getOutEdgeCount())) {
				messages->getNext();
			}
			while (messages->hasNext()) {
				mLongArray next = messages->getNext();
				if (next.getArray()[2].getValue() != 0) {
					next.getArray()[2].setValue(
							(next.getArray()[2].getValue() - 1));
					for (int j = 0; j < data->getOutEdgeCount(); j++) {
						comm->sendMessage(data->getOutEdgeID(j), next);
					}
				}
			}
		} else {
			while (messages->hasNext()) {
				messages->getNext();
			}
		}
	}

};
//end algo1
#endif  ADSIM_H_
