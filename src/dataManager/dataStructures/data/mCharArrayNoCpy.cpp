/*
 * mDouble.cpp
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#include "mCharArrayNoCpy.h"
#include <iostream>
#include <stdlib.h>

mCharArrayNoCpy::mCharArrayNoCpy() :
		IdataType(), charSize(0) {
	// TODO Auto-generated constructor stub
}
mCharArrayNoCpy::mCharArrayNoCpy(int size, char * array) :
		IdataType(), charSize(size), dataValue(array) {
	// TODO Auto-generated constructor stub
}
mCharArrayNoCpy::mCharArrayNoCpy(const mCharArrayNoCpy &obj) {
	charSize = obj.getSize();
	dataValue = obj.getValue();
}
mCharArrayNoCpy::~mCharArrayNoCpy() {
}
int mCharArrayNoCpy::byteSize() {
	return charSize;
}
std::string mCharArrayNoCpy::toString() {
	std::string output(dataValue);
	return output;
}
void mCharArrayNoCpy::readFromCharArray(char * input) {
	charSize = strlen(input);
	dataValue = (char *) calloc(charSize, sizeof(char));
	memcpy(dataValue, input, charSize);
}
char * mCharArrayNoCpy::byteEncode(int &size) {
	size = charSize;
	return dataValue;
}
int mCharArrayNoCpy::byteEncode2(char * buffer) {
	memcpy(buffer, dataValue, charSize);
	return charSize;
}

void mCharArrayNoCpy::byteDecode(int size, char * input) {
	charSize = size;
	dataValue = input;
}
std::size_t mCharArrayNoCpy::local_hash_value() const {
	if (charSize > 0) {
		return (size_t) dataValue[0];
	}
	return 0;
}
mCharArrayNoCpy & mCharArrayNoCpy::operator=(const mCharArrayNoCpy& rhs){
	charSize = ((mCharArrayNoCpy)rhs).getSize();
	dataValue = ((mCharArrayNoCpy)rhs).getValue();
	return *this;
}
bool mCharArrayNoCpy::operator==(const IdataType& rhs) const {
	int size2 = ((mCharArrayNoCpy&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArrayNoCpy&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (!(dataValue[i] == array2[i])) {
			return false;
		}
	}
	return true;
}
bool mCharArrayNoCpy::operator<(const IdataType& rhs) const {
	int size2 = ((mCharArrayNoCpy&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArrayNoCpy&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] >= array2[i]) {
			return false;
		}
	}
	return true;
}
bool mCharArrayNoCpy::operator>(const IdataType& rhs) const {
	int size2 = ((mCharArrayNoCpy&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArrayNoCpy&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] <= array2[i]) {
			return false;
		}
	}
	return true;
}
bool mCharArrayNoCpy::operator<=(const IdataType& rhs) const {
	int size2 = ((mCharArrayNoCpy&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArrayNoCpy&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] > array2[i]) {
			return false;
		}
	}
	return true;
}
bool mCharArrayNoCpy::operator>=(const IdataType& rhs) const {
	int size2 = ((mCharArrayNoCpy&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArrayNoCpy&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] < array2[i]) {
			return false;
		}
	}
	return true;
}

//Class specific methods
int mCharArrayNoCpy::getSize() const {
	return charSize;
}
char * mCharArrayNoCpy::getValue() const {
	return dataValue;
}
void mCharArrayNoCpy::setValue(int size, char * data) {
	charSize = size;
	dataValue = data;
}

