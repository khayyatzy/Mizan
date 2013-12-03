/*
 * mLong.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef MLONG_H_
#define MLONG_H_

#include "IdataType.h"

class mLong: public IdataType {
private:
	long long dataValue;
public:
	mLong();
	mLong(long long value);
	mLong(const mLong & obj);
	virtual ~mLong();
	int byteSize();
	std::string toString();
	void readFromCharArray(char * input);
	char * byteEncode(int &size);
	int byteEncode2(char * buffer);
	void byteDecode(int size, char * input);
	std::size_t local_hash_value() const;
	bool operator==(const IdataType& rhs) const;
	bool operator<(const IdataType& rhs) const;
	bool operator>(const IdataType &rhs) const;
	bool operator<=(const IdataType &rhs) const;
	bool operator>=(const IdataType &rhs) const;

	//Class specific methods
	long long getValue() const;
	void setValue(long long value);
	void cleanUp(){};
};

//
//
//typedef struct{
//  long long operator() (const mLong& v) const {
//	long long tmp = v.getValue();
//    return (tmp*73856);
//  }
//} LongHash;
//
//typedef struct{
//  inline bool operator() (const mLong& lhs, const mLong &rhs) const {
//    return (lhs.getValue()==rhs.getValue());
//  }
//} LongEq;

#endif /* MLONG_H_ */
