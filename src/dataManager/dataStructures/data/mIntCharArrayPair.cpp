/*
 * mIntTagDouble.cpp
 *
 *  Created on: Jun 24, 2012
 *      Author: refops
 */

#include "mIntCharArrayPair.h"
#include <iostream>
#include <stdlib.h>

mIntCharArrayPair::mIntCharArrayPair() :
		IdataType(), dataValue() {
	// TODO Auto-generated constructor stub
}
mIntCharArrayPair::mIntCharArrayPair(int inTag, int size, char * buffer) :
		IdataType(), tag(inTag), dataValue(size, buffer) {
	// TODO Auto-generated constructor stub
}
mIntCharArrayPair::~mIntCharArrayPair() {
	// TODO Auto-generated destructor stub
}
int mIntCharArrayPair::byteSize() {
	return mInt().byteSize() * 2 + dataValue.byteSize();
}
std::string mIntCharArrayPair::toString() {
	char outArray[42];
	strcpy(outArray, tag.toString().c_str());
	strcat(outArray, ":");
	strcat(outArray, dataValue.toString().c_str());
	std::string output(outArray);
	return output;
}
void mIntCharArrayPair::readFromCharArray(char * input) {
	std::string myInput(input);
	int length = strlen(input);
	size_t found = myInput.find(':');

	char outArray1[11];
	strncpy(outArray1, input, found);
	outArray1[found] = 0;
	tag.readFromCharArray(outArray1);

	char outArray2[30];
	strncpy(outArray2, &input[found + 1], (length - found));
	outArray2[(length - found)] = 0;
	dataValue.readFromCharArray(outArray2);
}
char * mIntCharArrayPair::byteEncode(int &size) {

	char * output = (char *) calloc(byteSize() + 2, sizeof(char));
	size = byteEncode2(output);

	return output;
}
int mIntCharArrayPair::byteEncode2(char * buffer) {
	int ptr = 0;
	int tmp;
	tmp = tag.byteEncode2(&buffer[ptr + 1]);
	buffer[ptr] = ((char) tmp);
	ptr = ptr + tmp + 1;

	mInt charSize(dataValue.getSize());
	tmp = charSize.byteEncode2(&buffer[ptr + 1]);
	buffer[ptr] = ((char) tmp);
	ptr = ptr + tmp + 1;

	tmp = dataValue.byteEncode2(&buffer[ptr]);
	ptr = ptr + tmp;

	return ptr;
}

void mIntCharArrayPair::byteDecode(int size, char * input) {
	int ptr = 0;
	int objSize = 0;

	objSize = ((int) input[ptr]);
	tag.byteDecode(objSize, &input[ptr + 1]);
	ptr = ptr + objSize + 1;

	mInt charSize;
	objSize = ((int) input[ptr]);
	charSize.byteDecode(objSize, &input[ptr + 1]);
	ptr = ptr + objSize + 1;

	dataValue.byteDecode(charSize.getValue(), &input[ptr]);
}
std::size_t mIntCharArrayPair::local_hash_value() const {
	return tag.local_hash_value();
}
bool mIntCharArrayPair::operator==(const IdataType& rhs) const {
	return (tag == ((mIntCharArrayPair&) rhs).getTag()
			&& dataValue == ((mIntCharArrayPair&) rhs).getValue());
}
bool mIntCharArrayPair::operator<(const IdataType& rhs) const {
	return (tag < ((mIntCharArrayPair&) rhs).getTag()
			&& dataValue < ((mIntCharArrayPair&) rhs).getValue());
}
bool mIntCharArrayPair::operator>(const IdataType& rhs) const {
	return (tag > ((mIntCharArrayPair&) rhs).getTag()
			&& dataValue > ((mIntCharArrayPair&) rhs).getValue());
}
bool mIntCharArrayPair::operator<=(const IdataType& rhs) const {
	return (tag <= ((mIntCharArrayPair&) rhs).getTag()
			&& dataValue <= ((mIntCharArrayPair&) rhs).getValue());
}
bool mIntCharArrayPair::operator>=(const IdataType& rhs) const {
	return (tag >= ((mIntCharArrayPair&) rhs).getTag()
			&& dataValue >= ((mIntCharArrayPair&) rhs).getValue());
}

//Class specific methods
mInt mIntCharArrayPair::getTag() {
	return tag;
}
mCharArrayNoCpy mIntCharArrayPair::getValue() {
	return dataValue;
}
void mIntCharArrayPair::setTag(int inTag) {
	tag = inTag;
}
void mIntCharArrayPair::setValue(int size, char * buffer) {
	dataValue.byteDecode(size, buffer);
}
void mIntCharArrayPair::cleanUp() {
	dataValue.cleanUp();
}
