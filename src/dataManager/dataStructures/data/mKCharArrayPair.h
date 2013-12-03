/*
 * mKTagDouble.h
 *
 *  Created on: Jun 24, 2012
 *      Author: refops
 */

#ifndef MKCHARARRAYPAIR_H_
#define MKCHARARRAYPAIR_H_

#include "IdataType.h"
#include "mCharArrayNoCpy.h"

template<class K>
class mKCharArrayPair: public IdataType {
private:
	K tag;
	mCharArrayNoCpy dataValue;
public:

	mKCharArrayPair() :
			IdataType(), dataValue() {
		// TODO Auto-generated constructor stub
	}
	mKCharArrayPair(K inTag, int size, char * buffer) :
			IdataType(), tag(inTag), dataValue(size, buffer) {
		// TODO Auto-generated constructor stub
	}
	~mKCharArrayPair() {
		// TODO Auto-generated destructor stub
	}
	int byteSize() {
		return K().byteSize() * 2 + dataValue.byteSize()+3;
	}
	std::string toString() {
		char outArray[42];
		strcpy(outArray, tag.toString().c_str());
		strcat(outArray, ":");
		strcat(outArray, dataValue.toString().c_str());
		std::string output(outArray);
		return output;
	}
	void readFromCharArray(char * input) {
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
	char * byteEncode(int &size) {

		char * output = (char *) calloc(byteSize() + 2, sizeof(char));
		size = byteEncode2(output);

		return output;
	}
	int byteEncode2(char * buffer) {
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

	void byteDecode(int size, char * input) {
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
	std::size_t local_hash_value() const {
		return tag.local_hash_value();
	}
	bool operator==(const IdataType& rhs) const {
		return (tag == ((mKCharArrayPair&) rhs).getTag()
				&& dataValue == ((mKCharArrayPair&) rhs).getValue());
	}
	bool operator<(const IdataType& rhs) const {
		return (tag < ((mKCharArrayPair&) rhs).getTag()
				&& dataValue < ((mKCharArrayPair&) rhs).getValue());
	}
	bool operator>(const IdataType& rhs) const {
		return (tag > ((mKCharArrayPair&) rhs).getTag()
				&& dataValue > ((mKCharArrayPair&) rhs).getValue());
	}
	bool operator<=(const IdataType& rhs) const {
		return (tag <= ((mKCharArrayPair&) rhs).getTag()
				&& dataValue <= ((mKCharArrayPair&) rhs).getValue());
	}
	bool operator>=(const IdataType& rhs) const {
		return (tag >= ((mKCharArrayPair&) rhs).getTag()
				&& dataValue >= ((mKCharArrayPair&) rhs).getValue());
	}

	//Class specific methods
	K getTag() {
		return tag;
	}
	mCharArrayNoCpy getValue() {
		return dataValue;
	}
	void setTag(K inTag) {
		tag = inTag;
	}
	void setValue(int size, char * buffer) {
		dataValue.byteDecode(size, buffer);
	}
	void cleanUp(){};

};

#endif /* MINTTAGDOUBLE_H_ */
