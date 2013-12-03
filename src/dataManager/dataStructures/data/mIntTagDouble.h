/*
 * mIntTagDouble.h
 *
 *  Created on: Jun 24, 2012
 *      Author: refops
 */

#ifndef MINTTAGDOUBLE_H_
#define MINTTAGDOUBLE_H_

#include "IdataType.h"
#include "mDouble.h"
#include "mInt.h"

class mIntTagDouble: public IdataType {
private:
	mInt tag;
	mDouble dataValue;
public:
	mIntTagDouble();
	mIntTagDouble(int tag,double value);
	virtual ~mIntTagDouble();
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
	mInt getTag();
	mDouble getValue();
	void setTag(int tag);
	void setValue(double value);void cleanUp(){};
};

#endif /* MINTTAGDOUBLE_H_ */
