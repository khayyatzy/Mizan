/*
 * mDouble.h
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#ifndef MDOUBLE_H_
#define MDOUBLE_H_

#include "IdataType.h"

class mDouble: public IdataType {
private:
	double dataValue;
public:
	mDouble();
	mDouble(double value);
	mDouble(const mDouble & obj);
	virtual ~mDouble();
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
	double getValue()const;
	void setValue(double value);
	void cleanUp(){};
};

#endif /* MDOUBLE_H_ */
