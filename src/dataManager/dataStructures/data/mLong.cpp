/*
 * mLong.cpp
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#include "mLong.h"

mLong::mLong() :
		IdataType(), dataValue() {
	// TODO Auto-generated constructor stub
}
mLong::mLong(const mLong & obj) :
		IdataType(){
	dataValue = obj.getValue();
}
mLong::mLong(long long value) :
		IdataType(), dataValue(value) {
	// TODO Auto-generated constructor stub
}
mLong::~mLong() {
	// TODO Auto-generated destructor stub
}
int mLong::byteSize() {
	return sizeof(long long);
}
std::string mLong::toString() {
	char outArray[30];
	sprintf(outArray, "%lld", dataValue);
	std::string output(outArray);
	return output;
}

void mLong::readFromCharArray(char * input) {
	dataValue = atoll(input);
}

char * mLong::byteEncode(int &size) {
	char * output = (char *) calloc(sizeof(long long), sizeof(char));

	size = byteEncode2(output);
	return output;
}
int mLong::byteEncode2(char * buffer){
	//char * output = (char *) calloc(sizeof(long long), sizeof(char));
	int j = 0;
	bool hasData = false;
	char tmpOutput;
	for (int i = ((sizeof(long long) - 1) * 8); i > -1; i = i - 8) {
		buffer[j] = 0;
		tmpOutput = (char) ((dataValue >> i) & 0xFF);
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

void mLong::byteDecode(int size, char * input) {
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
std::size_t mLong::local_hash_value() const {
	return dataValue;
}
bool mLong::operator==(const IdataType& rhs) const {
	return (dataValue == ((mLong&) rhs).getValue());
}
bool mLong::operator<(const IdataType& rhs) const {
	return (dataValue < ((mLong&) rhs).getValue());
}
bool mLong::operator>(const IdataType& rhs) const {
	return (dataValue > ((mLong&) rhs).getValue());
}
bool mLong::operator<=(const IdataType& rhs) const {
	return (dataValue <= ((mLong&) rhs).getValue());
}
bool mLong::operator>=(const IdataType& rhs) const {
	return (dataValue >= ((mLong&) rhs).getValue());
}

//Class specific methods
long long mLong::getValue() const{
	return dataValue;
}
void mLong::setValue(long long value) {
	dataValue = value;
}

