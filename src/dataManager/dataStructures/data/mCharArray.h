/*
 * mDouble.h
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#ifndef MCHARARRAY_H_
#define MCHARARRAY_H_

#include "IdataType.h"

class mCharArray: public IdataType {
private:
	int charSize;
	char * dataValue;
public:
	mCharArray();
	mCharArray(mCharArray& obj);
	mCharArray(int size,char * array);
	virtual ~mCharArray();
	int byteSize();
	std::string toString();
	void readFromCharArray(char * input);
	char * byteEncode(int &size);
	int byteEncode2(char * buffer);
	void byteDecode(int size, char * input);
	std::size_t local_hash_value() const;
	mCharArray & operator=(const mCharArray& obj);
	bool operator==(const IdataType& rhs) const;
	bool operator<(const IdataType& rhs) const;
	bool operator>(const IdataType &rhs) const;
	bool operator<=(const IdataType &rhs) const;
	bool operator>=(const IdataType &rhs) const;
	//Class specific methods
	void cleanUp();
	int getSize()const;
	char * getValue()const;
	void setValue(int size, char * data);
};

#endif /* MDOUBLE_H_ */
