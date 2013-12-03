/*
 * mInt.cpp
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#include "mInt.h"
#include <iostream>
#include <stdlib.h>

mInt::mInt() :
		IdataType(), dataValue(0) {
	// TODO Auto-generated constructor stub
}
mInt::mInt(int value) :
		IdataType(), dataValue(value) {
	// TODO Auto-generated constructor stub
}
mInt::~mInt() {
	// TODO Auto-generated destructor stub
}
int mInt::byteSize() {
	return sizeof(int);
}
std::string mInt::toString() {
	char outArray[11];
	sprintf(outArray, "%d", dataValue);
	std::string output(outArray);
	return output;
}

void mInt::readFromCharArray(char * input) {
	dataValue = atoi(input);
}

char * mInt::byteEncode(int &size) {
	char * output = (char *) calloc(sizeof(int), sizeof(char));

	size = byteEncode2(output);

	return output;
}
int mInt::byteEncode2(char * buffer) {
	//char * output = (char *) calloc(sizeof(int), sizeof(char));
	int j = 0;
	bool hasData = false;
	char tmpOutput;
	for (int i = ((sizeof(int) - 1) * 8); i > -1; i = i - 8) {
		tmpOutput = (char) ((dataValue >> i) & 0xFF);
		buffer[j] = 0;
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

void mInt::byteDecode(int size, char * input) {
	//dataValue = atoll(input);
	dataValue = 0;
	int tmp = 0;
	for (int i = 0; i < size; i++) {
		tmp = 0;
		tmp = (((int) input[i]) & 0x00ff);

		dataValue = dataValue << 8;
		dataValue = dataValue | tmp;
	}
}
std::size_t mInt::local_hash_value() const {
	return dataValue;
}

bool mInt::operator==(const IdataType& rhs) const {
	return (dataValue == ((mInt&) rhs).getValue());
}
bool mInt::operator<(const IdataType& rhs) const {
	return (dataValue < ((mInt&) rhs).getValue());
}
bool mInt::operator>(const IdataType& rhs) const {
	return (dataValue > ((mInt&) rhs).getValue());
}
bool mInt::operator<=(const IdataType& rhs) const {
	return (dataValue <= ((mInt&) rhs).getValue());
}
bool mInt::operator>=(const IdataType& rhs) const {
	return (dataValue >= ((mInt&) rhs).getValue());
}

//Class specific methods
int mInt::getValue() {
	return dataValue;
}
void mInt::setValue(int value) {
	dataValue = value;
}
