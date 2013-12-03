/*
 * mLong.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef MDOUBLEARRAY_H_
#define MDOUBLEARRAY_H_

#include "IdataType.h"
#include "mDouble.h"
#include <queue>

class mDoubleArray: public IdataType {
private:
	char delimiter;
	int size;
	mDouble * array;
public:
	mDoubleArray();
	mDoubleArray(const mDoubleArray & obj);
	mDoubleArray(int inSize, mDouble * inArray);
	mDoubleArray(int inSize, mDouble * inArray, char inDelimiter);
	virtual ~mDoubleArray();
	int byteSize();
	std::string toString();
	void readFromCharArray(char * input);
	char * byteEncode(int &size);
	int byteEncode2(char * buffer);
	void byteDecode(int size, char * input);
	std::size_t local_hash_value() const;
	mDoubleArray & operator=(const mDoubleArray& rhs);
	bool operator==(const IdataType& rhs) const;
	bool operator<(const IdataType& rhs) const;
	bool operator>(const IdataType &rhs) const;
	bool operator<=(const IdataType &rhs) const;
	bool operator>=(const IdataType &rhs) const;

	//Class specific methods
	void cleanUp();
	mDouble * getArray() const;
	int getSize() const;
	void setArray(int inSize, mDouble * value);
	void setArray(int inSize, mDouble * value, char inDelimiter);
	void setDelimiter(char inDelimiter);

};
#endif /* MLONG_H_ */
