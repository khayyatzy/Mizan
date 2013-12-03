/*
 * mLong.cpp
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#include "mDoubleArray.h"

mDoubleArray::mDoubleArray() :
		size(0) {
	delimiter = ':';
	// TODO Auto-generated constructor stub
}
mDoubleArray::mDoubleArray(const mDoubleArray & obj) {
	//cleanUp();
	size = obj.getSize();
	array = new mDouble[size];
	delimiter = ':';
	mDouble * dummy = obj.getArray();
	for (int i = 0; i < size; i++) {
		array[i] = dummy[i].getValue();
	}

}
mDoubleArray::mDoubleArray(int inSize, mDouble * inArray) :
		IdataType(), size(inSize), array(inArray) {
	delimiter = ':';
	// TODO Auto-generated constructor stub
}
mDoubleArray::mDoubleArray(int inSize, mDouble * inArray, char inDelimiter) :
		IdataType(), size(inSize), array(inArray), delimiter(inDelimiter) {
	// TODO Auto-generated constructor stub
}
void mDoubleArray::cleanUp() {
	if (size != 0) {
		delete[] array;
		size = 0;
	}
}
mDoubleArray::~mDoubleArray() {
	// TODO Auto-generated destructor stub
	cleanUp();
}
int mDoubleArray::byteSize() {
	return mDouble().byteSize() * size + size;
}
std::string mDoubleArray::toString() {
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

void mDoubleArray::readFromCharArray(char * input) {
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
		array = new mDouble[mySize];
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

char * mDoubleArray::byteEncode(int &inSize) {
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
int mDoubleArray::byteEncode2(char * buffer) {
	int j = 0;
	int tmpSize = 0;
	for (int i = 0; i < size; i++) {
		tmpSize = array[i].byteEncode2(&buffer[j + 1]);
		buffer[j] = ((char) tmpSize);
		j = j + tmpSize + 1;
	}
	return j;
}
void mDoubleArray::byteDecode(int inSize, char * input) {
	std::queue<mDouble> list;
	delimiter=':';
	int j = 0;
	int objSize = 0;
	while (j < inSize) {
		objSize = ((int) input[j]);
		mDouble obj;
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
		array = new mDouble[realSize];
	}
	for (int i = 0; i < realSize; i++) {
		array[i] = list.front();
		list.pop();
	}
}
std::size_t mDoubleArray::local_hash_value() const {
	if (size > 0) {
		return array[0].local_hash_value();
	}
	return 0;
}
mDoubleArray & mDoubleArray::operator=(const mDoubleArray& rhs) {
	//cleanUp();
	delimiter=':';
	size = ((mDoubleArray)rhs).getSize();
	array = new mDouble[size];
	mDouble * dummy = ((mDoubleArray)rhs).getArray();
	for (int i = 0; i < size; i++) {
		array[i] = dummy[i].getValue();
	}
	return *this;
}
bool mDoubleArray::operator==(const IdataType& rhs) const {
	int size2 = ((mDoubleArray&) rhs).getSize();
	if (size != size2) {
		return false;
	}
	mDouble * array2 = ((mDoubleArray&) rhs).getArray();
	for (int i = 0; i < size; i++) {
		if (!(array[i] == array2[i])) {
			return false;
		}
	}
	return true;
}
bool mDoubleArray::operator<(const IdataType& rhs) const {
	int size2 = ((mDoubleArray&) rhs).getSize();
	mDouble * array2 = ((mDoubleArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] >= array2[i])) {
			return false;
		}
	}
	return true;
}
bool mDoubleArray::operator>(const IdataType& rhs) const {
	int size2 = ((mDoubleArray&) rhs).getSize();
	mDouble * array2 = ((mDoubleArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] <= array2[i])) {
			return false;
		}
	}
	return true;
}
bool mDoubleArray::operator<=(const IdataType& rhs) const {
	int size2 = ((mDoubleArray&) rhs).getSize();
	mDouble * array2 = ((mDoubleArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] > array2[i])) {
			return false;
		}
	}
	return true;
}
bool mDoubleArray::operator>=(const IdataType& rhs) const {
	int size2 = ((mDoubleArray&) rhs).getSize();
	mDouble * array2 = ((mDoubleArray&) rhs).getArray();
	for (int i = 0; i < size && i < size2; i++) {
		if ((array[i] < array2[i])) {
			return false;
		}
	}
	return true;
}

//Class specific methods
mDouble * mDoubleArray::getArray() const {
	return array;
}
int mDoubleArray::getSize() const {
	return size;
}
void mDoubleArray::setArray(int inSize, mDouble * value) {
	cleanUp();
	size = inSize;
	array = value;
}
void mDoubleArray::setArray(int inSize, mDouble * value, char inDelimiter) {
	cleanUp();
	size = inSize;
	array = value;
	delimiter = inDelimiter;
}

