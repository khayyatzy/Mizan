/*
 * mIntTagDouble.cpp
 *
 *  Created on: Jun 24, 2012
 *      Author: refops
 */

#include "mIntTagDouble.h"
#include <iostream>
#include <stdlib.h>

mIntTagDouble::mIntTagDouble() :
		IdataType(), dataValue() {
	// TODO Auto-generated constructor stub
}
mIntTagDouble::mIntTagDouble(int inTag, double value) :
		IdataType(), tag(inTag), dataValue(value) {
	// TODO Auto-generated constructor stub
}
mIntTagDouble::~mIntTagDouble() {
	// TODO Auto-generated destructor stub
}
int mIntTagDouble::byteSize() {
	return sizeof(int) + sizeof(double);
}
std::string mIntTagDouble::toString() {
	char outArray[42];
	strcpy(outArray, tag.toString().c_str());
	strcat(outArray, ":");
	strcat(outArray, dataValue.toString().c_str());
	std::string output(outArray);
	return output;
}
void mIntTagDouble::readFromCharArray(char * input) {
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
char * mIntTagDouble::byteEncode(int &size) {

	char * output = (char *) calloc(sizeof(int) + sizeof(double), sizeof(char));
	int j = tag.byteEncode2(&output[1]);
	output[0] = ((char) j);
	j++;

	int k = dataValue.byteEncode2(&output[j + 1]);
	output[j] = ((char) k);
	k++;
	size = j + k;

	return output;
}
int mIntTagDouble::byteEncode2(char * buffer) {
	int j = tag.byteEncode2(&buffer[1]);
	buffer[0] = ((char) j);
	j++;

	unsigned long long tmpDataValue = *((unsigned long long*) &dataValue);

	int k = dataValue.byteEncode2(&buffer[j + 1]);
	buffer[j] = ((char) k);
	k++;
	return j + k;
}

void mIntTagDouble::byteDecode(int size, char * input) {
	int tagSize = ((int)input[0]);
	tag.byteDecode(tagSize,&input[1]);
	int dataSize = ((int)input[tagSize+1]);
	dataValue.byteDecode(dataSize,&input[tagSize+2]);
}
std::size_t mIntTagDouble::local_hash_value() const {
	return tag.local_hash_value();
}
bool mIntTagDouble::operator==(const IdataType& rhs) const {
	return (tag == ((mIntTagDouble&) rhs).getTag()
			&& dataValue == ((mIntTagDouble&) rhs).getValue());
}
bool mIntTagDouble::operator<(const IdataType& rhs) const {
	return (tag < ((mIntTagDouble&) rhs).getTag()
			&& dataValue < ((mIntTagDouble&) rhs).getValue());
}
bool mIntTagDouble::operator>(const IdataType& rhs) const {
	return (tag > ((mIntTagDouble&) rhs).getTag()
			&& dataValue > ((mIntTagDouble&) rhs).getValue());
}
bool mIntTagDouble::operator<=(const IdataType& rhs) const {
	return (tag <= ((mIntTagDouble&) rhs).getTag()
			&& dataValue <= ((mIntTagDouble&) rhs).getValue());
}
bool mIntTagDouble::operator>=(const IdataType& rhs) const {
	return (tag >= ((mIntTagDouble&) rhs).getTag()
			&& dataValue >= ((mIntTagDouble&) rhs).getValue());
}

//Class specific methods
mInt mIntTagDouble::getTag() {
	return tag;
}
mDouble mIntTagDouble::getValue() {
	return dataValue;
}
void mIntTagDouble::setTag(int inTag) {
	tag.setValue(inTag);
}
void mIntTagDouble::setValue(double value) {
	dataValue = value;
}
