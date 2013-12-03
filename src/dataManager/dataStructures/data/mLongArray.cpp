/*
 * mLong.cpp
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#include "mLongArray.h"

mLongArray::mLongArray() :
		size(0) {
	delimiter = ':';
	// TODO Auto-generated constructor stub
}
mLongArray::mLongArray(const mLongArray & obj) {
	//cleanUp();
	size = obj.getSize();
	array = new mLong[size];
	mLong * dummy = obj.getArray();
	for (int i = 0; i < size; i++) {
		array[i] = dummy[i];
	}

}
mLongArray::mLongArray(int inSize, mLong * inArray) :
		IdataType(), size(inSize), array(inArray) {
	delimiter = ':';
	// TODO Auto-generated constructor stub
}
mLongArray::mLongArray(int inSize, mLong * inArray, char inDelimiter) :
		IdataType(), size(inSize), array(inArray), delimiter(inDelimiter) {
	// TODO Auto-generated constructor stub
}
void mLongArray::cleanUp() {
	if (size != 0) {
		delete[] array;
		size = 0;
	}
}
mLongArray::~mLongArray() {
	// TODO Auto-generated destructor stub
	cleanUp();
}
int mLongArray::byteSize() {
	return mLong().byteSize() * size + size;
}
std::string mLongArray::toString() {
	char outArray[31 * size];
	outArray[0] = 0;
	char myDelimiter[2];
	myDelimiter[0] = delimiter;
	myDelimiter[1] = 0;
	char tmpArray[30];
	for (int i = 0; i < size; i++) {
		std::strcat(outArray, array[i].toString().c_str());
		std::strcat(outArray, myDelimiter);
	}
	std::string output(outArray);
	return output;
}

void mLongArray::readFromCharArray(char * input) {
	int i = 0;
	int mySize = 0;
	while (input[i] != 0) {
		if (input[i] == delimiter) {
			mySize++;
		}
		i++;
	}
	if (mySize != size) {
		if (size != 0) {
			cleanUp();
		}
		size = mySize;
		array = new mLong[mySize];
	}
	int startPtr = 0;
	int endPtr = 0;
	for (int i = 0; i < mySize; i++) {
		char tmpArray[30];
		while (input[endPtr] != delimiter) {
			endPtr++;
		}
		//12345:668512:999831
		strncpy(tmpArray, &input[startPtr], (endPtr - startPtr));
		tmpArray[endPtr - startPtr] = 0;
		array[i].readFromCharArray(tmpArray);
		//std::cout << array[i].getValue() << std::endl;
		endPtr++;
		startPtr = endPtr;
	}
}

char * mLongArray::byteEncode(int &inSize) {
	char * output = (char *) calloc(byteSize(), sizeof(char));
	int j = 0;
	int tmpSize = 0;
	for (int i = 0; i < size; i++) {
		tmpSize = array[i].byteEncode2(&output[j + 1]);
		output[j] = ((char) tmpSize);
		j = j + tmpSize + 1;
	}
	inSize = j;
	return output;
}
int mLongArray::byteEncode2(char * buffer) {
	int j = 0;
	int tmpSize = 0;
	for (int i = 0; i < size; i++) {
		tmpSize = array[i].byteEncode2(&buffer[j + 1]);
		buffer[j] = ((char) tmpSize);
		j = j + tmpSize + 1;
	}
	return j;
}
void mLongArray::byteDecode(int inSize, char * input) {
	//dataValue = atoll(input);
	std::queue<mLong> list;
	delimiter=':';
	int j = 0;
	int objSize = 0;
	while (j < inSize) {
		objSize = ((int) input[j]);
		mLong obj;
		obj.byteDecode(objSize, &input[j + 1]);
		j = j + objSize + 1;
		list.push(obj);
	}

	int realSize = list.size();
	if (realSize != size) {
		if (size != 0) {
			cleanUp();
		}
		size = realSize;
		array = new mLong[realSize];
	}
	for (int i = 0; i < realSize; i++) {
		array[i] = list.front();
		list.pop();
	}
}
std::size_t mLongArray::local_hash_value() const {
	if (size > 0) {
		return array[0].local_hash_value();
	}
	return 0;
}
mLongArray & mLongArray::operator=(const mLongArray& rhs) {
	cleanUp();
	size = rhs.getSize();
	array = new mLong[size];
	mLong * dummy = rhs.getArray();
	for (int i = 0; i < size; i++) {
		array[i] = dummy[i];
	}
	return *this;
}
bool mLongArray::operator==(const IdataType& rhs) const {
	int size2 = ((mLongArray&) rhs).getSize();
	if (size != size2) {
		return false;
	}
	mLong * array2 = ((mLongArray&) rhs).getArray();
	for (int i = 0; i < size; i++) {
		if (!(array[i] == array2[i])) {
			return false;
		}
	}
	return true;
}
bool mLongArray::operator<(const IdataType& rhs) const {
	int size2 = ((mLongArray&) rhs).getSize();
	mLong * array2 = ((mLongArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] >= array2[i])) {
			return false;
		}
	}
	return true;
}
bool mLongArray::operator>(const IdataType& rhs) const {
	int size2 = ((mLongArray&) rhs).getSize();
	mLong * array2 = ((mLongArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] <= array2[i])) {
			return false;
		}
	}
	return true;
}
bool mLongArray::operator<=(const IdataType& rhs) const {
	int size2 = ((mLongArray&) rhs).getSize();
	mLong * array2 = ((mLongArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] > array2[i])) {
			return false;
		}
	}
	return true;
}
bool mLongArray::operator>=(const IdataType& rhs) const {
	int size2 = ((mLongArray&) rhs).getSize();
	mLong * array2 = ((mLongArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] < array2[i])) {
			return false;
		}
	}
	return true;
}

//Class specific methods
mLong * mLongArray::getArray() const {
	return array;
}
int mLongArray::getSize() const {
	return size;
}
void mLongArray::setArray(int inSize, mLong * value) {
	cleanUp();
	size = inSize;
	array = value;
}
void mLongArray::setArray(int inSize, mLong * value, char inDelimiter) {
	cleanUp();
	size = inSize;
	array = value;
	delimiter = inDelimiter;
}
