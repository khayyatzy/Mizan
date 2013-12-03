/*
 * mDouble.cpp
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#include "mDouble.h"
#include <iostream>
#include <stdlib.h>

mDouble::mDouble() :
		IdataType(), dataValue() {
	// TODO Auto-generated constructor stub
}
mDouble::mDouble(const mDouble & obj) :
		IdataType(){
	dataValue = obj.getValue();
}

mDouble::mDouble(double value) :
		IdataType(), dataValue(value) {
	// TODO Auto-generated constructor stub
}
mDouble::~mDouble() {
	// TODO Auto-generated destructor stub
}
int mDouble::byteSize() {
	return sizeof(double);
}
std::string mDouble::toString() {
	char outArray[30];
	sprintf(outArray, "%f", dataValue);
	std::string output(outArray);
	return output;
}
void mDouble::readFromCharArray(char * input) {
	dataValue = atof(input);
}
char * mDouble::byteEncode(int &size) {
	char * output = (char *) calloc(sizeof(double), sizeof(char));
	size = byteEncode2(output);
	return output;
}
int mDouble::byteEncode2(char * buffer) {
	unsigned long long tmpDataValue = *((unsigned long long*) &dataValue);

	int j = 0;
	char tmpOutput;
	bool hasData = false;
	for (int i = ((sizeof(double) - 1) * 8); i > -1; i = i - 8) {
		buffer[j] = 0;
		tmpOutput = (char) ((tmpDataValue >> i) & 0xFF);
		if (tmpOutput != ((char) 0) || hasData) {
			hasData = true;
			buffer[j] = tmpOutput;
			++j;
		}
	}
	if (j == 0) {
		return 1;
	} else {
		return j;
	}
}

void mDouble::byteDecode(int size, char * input) {
	//dataValue = atoll(input);
	unsigned long long tmpDataValue = 0;

	int tmp = 0;
	for (int i = 0; i < size; i++) {
		tmp = 0;
		tmp = (((int) input[i]) & 0x00ff);

		tmpDataValue = tmpDataValue << 8;
		tmpDataValue = tmpDataValue | tmp;
	}

	dataValue = *((double*) &tmpDataValue);
}
std::size_t mDouble::local_hash_value() const {
	//errors: NAN & -0
	return (size_t) *(const unsigned long long *) &dataValue;
}
bool mDouble::operator==(const IdataType& rhs) const {
	return (dataValue == ((mDouble&) rhs).getValue());
}
bool mDouble::operator<(const IdataType& rhs) const {
	return (dataValue < ((mDouble&) rhs).getValue());
}
bool mDouble::operator>(const IdataType& rhs) const {
	return (dataValue > ((mDouble&) rhs).getValue());
}
bool mDouble::operator<=(const IdataType& rhs) const {
	return (dataValue <= ((mDouble&) rhs).getValue());
}
bool mDouble::operator>=(const IdataType& rhs) const {
	return (dataValue >= ((mDouble&) rhs).getValue());
}

//Class specific methods
double mDouble::getValue() const{
	return dataValue;
}
void mDouble::setValue(double value) {
	dataValue = value;
}

