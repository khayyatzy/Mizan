/*
 * mDouble.cpp
 *
 *  Created on: Apr 8, 2012
 *      Author: refops
 */

#include "mCharArray.h"
#include <iostream>
#include <stdlib.h>

mCharArray::mCharArray() :
		IdataType(), charSize(0) {
	// TODO Auto-generated constructor stub
}
mCharArray::mCharArray(mCharArray& obj) {
	charSize = obj.getSize();
	dataValue = (char*) calloc(charSize, sizeof(char));
	memcpy(dataValue, obj.getValue(), charSize);
}
mCharArray::mCharArray(int size, char * array) :
		IdataType(), charSize(size), dataValue(array) {
	//dataValue = (char *) calloc(size,sizeof(char));
	//memcpy(dataValue,array,size);
	// TODO Auto-generated constructor stub
}
mCharArray::~mCharArray() {
	// TODO Auto-generated destructor stub
	cleanUp();
}
int mCharArray::byteSize() {
	return charSize;
}
std::string mCharArray::toString() {
	std::string output(dataValue);
	return output;
}
void mCharArray::readFromCharArray(char * input) {
	if (strlen(input) != charSize) {
		cleanUp();
	}
	charSize = strlen(input);
	dataValue = (char *) calloc(charSize, sizeof(char));
	memcpy(dataValue, input, charSize);
}
char * mCharArray::byteEncode(int &size) {
	size = charSize;
	char * output = (char *) calloc(charSize, sizeof(char));
	memcpy(output, dataValue, charSize);
	return output;
}
int mCharArray::byteEncode2(char * buffer) {
	memcpy(buffer, dataValue, charSize);
	return charSize;
}

void mCharArray::byteDecode(int size, char * input) {
	cleanUp();
	charSize = size;
	dataValue = (char *) calloc(size, sizeof(char));
	memcpy(dataValue, input, size);
}
std::size_t mCharArray::local_hash_value() const {
	if (charSize > 0) {
		return (size_t) dataValue[0];
	}
	return 0;
}
mCharArray & mCharArray::operator=(const mCharArray& obj) {
	charSize = obj.getSize();
	dataValue = (char*) calloc(charSize, sizeof(char));
	memcpy(dataValue, obj.getValue(), charSize);
}
bool mCharArray::operator==(const IdataType& rhs) const {
	int size2 = ((mCharArray&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArray&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (!(dataValue[i] == array2[i])) {
			return false;
		}
	}
	return true;
}
bool mCharArray::operator<(const IdataType& rhs) const {
	int size2 = ((mCharArray&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArray&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] >= array2[i]) {
			return false;
		}
	}
	return true;
}
bool mCharArray::operator>(const IdataType& rhs) const {
	int size2 = ((mCharArray&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArray&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] <= array2[i]) {
			return false;
		}
	}
	return true;
}
bool mCharArray::operator<=(const IdataType& rhs) const {
	int size2 = ((mCharArray&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArray&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] > array2[i]) {
			return false;
		}
	}
	return true;
}
bool mCharArray::operator>=(const IdataType& rhs) const {
	int size2 = ((mCharArray&) rhs).getSize();
	if (charSize != size2) {
		return false;
	}
	char * array2 = ((mCharArray&) rhs).getValue();
	for (int i = 0; i < charSize; i++) {
		if (dataValue[i] < array2[i]) {
			return false;
		}
	}
	return true;
}

//Class specific methods
void mCharArray::cleanUp() {
	if (charSize != 0) {
		free(dataValue);
		charSize = 0;
	}
}
int mCharArray::getSize() const{
	return charSize;
}
char * mCharArray::getValue() const{
	return dataValue;
}
void mCharArray::setValue(int size, char * data) {
	cleanUp();
	charSize = size;
	dataValue = (char *) calloc(charSize, sizeof(char));
	memcpy(dataValue, data, charSize);
}

