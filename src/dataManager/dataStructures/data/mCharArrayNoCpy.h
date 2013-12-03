/*
 * mDouble.h
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#ifndef MCHARARRAYNOCPY_H_
#define MCHARARRAYNOCPY_H_

#include "IdataType.h"

class mCharArrayNoCpy: public IdataType {
private:
	int charSize;
	char * dataValue;
public:
	mCharArrayNoCpy();
	mCharArrayNoCpy(int size, char * array);
	mCharArrayNoCpy(const mCharArrayNoCpy &obj);
	virtual ~mCharArrayNoCpy();
	int byteSize();
	std::string toString();
	void readFromCharArray(char * input);
	char * byteEncode(int &size);
	int byteEncode2(char * buffer);
	void byteDecode(int size, char * input);
	std::size_t local_hash_value() const;
	mCharArrayNoCpy & operator=(const mCharArrayNoCpy& rhs);
	bool operator==(const IdataType& rhs) const;
	bool operator<(const IdataType& rhs) const;
	bool operator>(const IdataType &rhs) const;
	bool operator<=(const IdataType &rhs) const;
	bool operator>=(const IdataType &rhs) const;
	//Class specific methods
	int getSize() const;
	char * getValue() const;
	void setValue(int size, char * data);
	void cleanUp(){};
};

#endif /* MDOUBLE_H_ */
