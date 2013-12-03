/*
 * systemWideInfo.h
 *
 *  Created on: May 28, 2012
 *      Author: khayyzy
 */

#ifndef SYSTEMWIDEINFO_H_
#define SYSTEMWIDEINFO_H_

#include <map>
#include <stdio.h>
#include <string.h>

template<class K>
class systemWideInfo {
	K maxVertex;
	K minVertex;
	std::map<char *, long> counters;
	std::map<char *, char *> data;
public:
	systemWideInfo() {
	}
	void setMinVertex(K vertex) {
		minVertex = vertex;
	}
	void setMaxVertex(K vertex) {
		maxVertex = vertex;
	}
	K getMaxVertex() {
		return maxVertex;
	}
	K getMinVertex() {
		return minVertex;
	}
	void updateMaxVertex(K vertex) {
		if (vertex > maxVertex) {
			maxVertex = vertex;
		}
	}
	void updateMinVertex(K vertex) {
		if (vertex < minVertex) {
			maxVertex = vertex;
		}
	}
	void addCounter(char * counterName, long value) {
		counters[counterName] = value;
	}
	void incrementCounter(char * counterName) {
		counters[counterName] = counters[counterName] + 1;
	}
	void incrementCounter(char * counterName, long value) {
		counters[counterName] = counters[counterName] + value;
	}
	long getCounter(char * counterName) {
		return counters[counterName];
	}
	void addData(char * dataName, char * value) {
		strcpy(data[dataName], value);
	}
	char * getData(char * dataName) {
		return data[dataName];
	}
	virtual ~systemWideInfo() {
	}
};

#endif /* SYSTEMWIDEINFO_H_ */
