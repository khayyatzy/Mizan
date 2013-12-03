/*
 * mIntTagDouble.h
 *
 *  Created on: Jun 24, 2012
 *      Author: refops
 */

#ifndef MINTCHARARRAYPAIR_H_
#define MINTCHARARRAYPAIR_H_

#include "IdataType.h"
#include "mDouble.h"
#include "mInt.h"
#include "mCharArrayNoCpy.h"

class mIntCharArrayPair: public IdataType {
private:
	mInt tag;
	mCharArrayNoCpy dataValue;
public:
	mIntCharArrayPair();
	mIntCharArrayPair(int tag,int size,char * buffer);
	virtual ~mIntCharArrayPair();
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
	mCharArrayNoCpy getValue();
	void setTag(int tag);
	void setValue(int size,char * buffer);
	void cleanUp();
};

#endif /* MINTTAGDOUBLE_H_ */
