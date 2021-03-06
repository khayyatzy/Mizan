/*
 * K.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef MKARRAY_H_
#define MKARRAY_H_

#include "IdataType.h"
#include <queue>

template<class K>
class mKArray: public IdataType {
private:
	char delimiter;
	int size;
	K * array;
public:
	mKArray() :
			size(0) {
		delimiter = ':';
		// TODO Auto-generated constructor stub
	}
	mKArray(const mKArray & obj) {
		//cleanUp();
		size = obj.getSize();
		array = new K[size];
		K * dummy = obj.getArray();
		for (int i = 0; i < size; i++) {
			array[i] = dummy[i];
		}

	}
	mKArray(int inSize, K * inArray) :
			IdataType(), size(inSize), array(inArray) {
		delimiter = ':';
		// TODO Auto-generated constructor stub
	}
	mKArray(int inSize, K * inArray, char inDelimiter) :
			IdataType(), size(inSize), array(inArray), delimiter(inDelimiter) {
		// TODO Auto-generated constructor stub
	}
	void cleanUp() {
		if (size != 0) {
			delete[] array;
			size = 0;
		}
	}
	~mKArray() {
		// TODO Auto-generated destructor stub
		cleanUp();
	}
	int byteSize() {
		return K().byteSize() * size;
	}
	std::string toString() {
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

	void readFromCharArray(char * input) {
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
			array = new K[mySize];
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

	char * byteEncode(int &inSize) {
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
	int byteEncode2(char * buffer) {
		int j = 0;
		int tmpSize = 0;
		for (int i = 0; i < size; i++) {
			tmpSize = array[i].byteEncode2(&buffer[j + 1]);
			buffer[j] = ((char) tmpSize);
			j = j + tmpSize + 1;
		}
		return j;
	}
	void byteDecode(int inSize, char * input) {
		//dataValue = atoll(input);
		std::queue<K> list;
		delimiter = ':';
		int j = 0;
		int objSize = 0;
		while (j < inSize) {
			objSize = ((int) input[j]);
			K obj;
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
			array = new K[realSize];
		}
		for (int i = 0; i < realSize; i++) {
			array[i] = list.front();
			list.pop();
		}
	}
	std::size_t local_hash_value() const {
		if (size > 0) {
			return array[0].local_hash_value();
		}
		return 0;
	}
	mKArray & operator=(const mKArray& rhs) {
		//cleanUp();
		size = rhs.getSize();
		array = new K[size];
		K * dummy = rhs.getArray();
		for (int i = 0; i < size; i++) {
			array[i] = dummy[i];
		}
		return *this;
	}
	bool operator==(const IdataType& rhs) const {
		int size2 = ((mKArray&) rhs).getSize();
		if (size != size2) {
			return false;
		}
		K * array2 = ((mKArray&) rhs).getArray();
		for (int i = 0; i < size; i++) {
			if (!(array[i] == array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator<(const IdataType& rhs) const {
		int size2 = ((mKArray&) rhs).getSize();
		K * array2 = ((mKArray&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] >= array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator>(const IdataType& rhs) const {
		int size2 = ((mKArray&) rhs).getSize();
		K * array2 = ((mKArray&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] <= array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator<=(const IdataType& rhs) const {
		int size2 = ((mKArray&) rhs).getSize();
		K * array2 = ((mKArray&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] > array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator>=(const IdataType& rhs) const {
		int size2 = ((mKArray&) rhs).getSize();
		K * array2 = ((mKArray&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] < array2[i])) {
				return false;
			}
		}
		return true;
	}

	//Class specific methods
	K * getArray() const {
		return array;
	}
	int getSize() const {
		return size;
	}
	void setArray(int inSize, K * value) {
		cleanUp();
		size = inSize;
		array = value;
	}
	void setArray(int inSize, K * value, char inDelimiter) {
		cleanUp();
		size = inSize;
		array = value;
		delimiter = inDelimiter;
	}

};
#endif /* K_H_ */
