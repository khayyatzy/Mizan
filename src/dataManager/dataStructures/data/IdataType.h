/*
 * IdataType.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef IDATATYPE_H_
#define IDATATYPE_H_

#include <iostream>
#include <string>
#include <cstdio>
#include <stdlib.h>
#include <boost/functional/hash.hpp>
#include <cstring>

class IdataType {

public:
	IdataType();
	virtual ~IdataType();
	virtual int byteSize()=0;
	virtual std::string toString()=0;
	virtual void readFromCharArray(char * input)=0;
	virtual char * byteEncode(int &size)=0;
	virtual int byteEncode2(char * buffer)=0;
	virtual void byteDecode(int size, char * buffer)=0;
	virtual std::size_t local_hash_value() const=0;
	virtual bool operator==(const IdataType &val2) const=0;
	virtual bool operator<(const IdataType &val2) const=0;
	virtual bool operator> (const IdataType &val2) const =0;
	virtual bool operator<= (const IdataType &val2)const=0;
	virtual bool operator>= (const IdataType &val2)const=0;
	//virtual void setValue (void * input)=0;
	//virtual void getValue ()=0;
	friend std::size_t hash_value(const IdataType& p);
	virtual void cleanUp() = 0;
	//virtual int compare(IdataType &val2)=0;
	//virtual bool operator> (const IdataType &val2) const =0;
	//virtual bool operator<= (const IdataType &val2)const=0;
	//virtual bool operator>= (const IdataType &val2)const=0;

};

#endif /* IDATATYPE_H_ */
