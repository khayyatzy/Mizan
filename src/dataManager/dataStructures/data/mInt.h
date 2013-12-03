/*
 * mInt.h
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#ifndef MINT_H_
#define MINT_H_

#include "IdataType.h"

class mInt: public IdataType {
private:
	int dataValue;
public:
	mInt();
	mInt(int value);
	virtual ~mInt();
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
	int getValue();
	void setValue(int value);
	void cleanUp(){};
};

#endif /* MINT_H_ */
