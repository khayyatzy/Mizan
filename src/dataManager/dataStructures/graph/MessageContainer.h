/*
 * MessageContainer.h
 *
 *  Created on: Nov 27, 2012
 *      Author: refops
 */

#ifndef MESSAGECONTAINER_H_
#define MESSAGECONTAINER_H_

#include <set>
#include <vector>
#include <queue>
#include <list>
#include <map>

template<class K,class M>
class MessageContainer {
private:
	std::map<K, typename std::queue<M, std::list<M> > *> mStorage;
public:
	MessageContainer(){};
	void put(K dst,M message){
		if((mStorage.find(dst))!=mStorage.end()){
			std::queue<M, std::list<M> > * queue = mStorage.at(dst);
			queue->push(message);
		}
		else{
			std::queue<M, std::list<M> > * queue = new std::queue<M, std::list<M> >();
			queue->push(message);
			mStorage[dst] = queue;
		}
	}
	//the user should delete(ptr)
	std::queue<M, std::list<M> > * getMessageList(K &dst){
		typename std::map<K, std::queue<M, std::list<M> > *>::iterator it = mStorage.begin();
		dst = (*it).first;
		std::queue<M, std::list<M> > * ptr = (*it).second;
		mStorage.erase((*it).first);
		return ptr;
	}
	bool hasMoreKeys(){
		return (!mStorage.empty());
	}
	virtual ~MessageContainer(){};
};

#endif /* MESSAGECONTAINER_H_ */
