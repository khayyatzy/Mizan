/*
 * mLong.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef MLONGARRAY_H_
#define MLONGARRAY_H_

#include "IdataType.h"
#include <queue>
#include "mLong.h"

class mLongArray: public IdataType {
private:
	char delimiter;
	int size;
	 mLong * array;
public:
	mLongArray();
	mLongArray(const mLongArray & obj);
	mLongArray(int inSize, mLong * inArray);
	mLongArray(int inSize, mLong * inArray, char inDelimiter);
	virtual ~mLongArray();
	int byteSize();
	std::string toString();
	void readFromCharArray(char * input);
	char * byteEncode(int &size);
	int byteEncode2(char * buffer);
	void byteDecode(int size, char * input);
	std::size_t local_hash_value() const;
	mLongArray & operator=(const mLongArray& rhs);
	bool operator==(const IdataType& rhs) const;
	bool operator<(const IdataType& rhs) const;
	bool operator>(const IdataType &rhs) const;
	bool operator<=(const IdataType &rhs) const;
	bool operator>=(const IdataType &rhs) const;

	//Class specific methods
	void cleanUp();
	mLong * getArray() const;
	int getSize() const;
	void setArray(int inSize, mLong * value);
	void setArray(int inSize, mLong * value,char inDelimiter);
	void setDelimiter(char inDelimiter);

};
#endif /* MLONG_H_ */
