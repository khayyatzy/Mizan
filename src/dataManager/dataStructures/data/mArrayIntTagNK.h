/*
 * mLong.h
 *
 *  Created on: Mar 27, 2012
 *      Author: refops
 */

#ifndef MARRAYINTTAGNK_H_
#define MARRAYINTTAGNK_H_

#include "IdataType.h"
#include "mInt.h"
#include <queue>

template<class K>
class mArrayIntTagNK: public IdataType {
private:
	char delimiter;
	int size;
	mInt tag;
	K * array;
public:
	mArrayIntTagNK(mArrayIntTagNK& obj) {
		delimiter = obj.getDelimiter();
		tag = obj.getTag();
		//cleanUp();
		size = obj.getSize();
		array = new K[size];
		K * tmpArray = obj.getArray();
		for (int i = 0; i < size; i++) {
			array[i] = tmpArray[i];
		}
	}
	mArrayIntTagNK() :
			size(0) {
		delimiter = ':';
	}
	mArrayIntTagNK(int inSize, int inTag, K * inArray) :
			IdataType(), size(inSize), tag(inTag), array(inArray) {
		delimiter = ':';
		// TODO Auto-generated constructor stub
	}
	mArrayIntTagNK(int inSize, int inTag, K * inArray, char inDelimiter) :
			IdataType(), size(inSize), tag(inTag), array(inArray), delimiter(
					inDelimiter) {
		// TODO Auto-generated constructor stub
	}
	virtual ~mArrayIntTagNK() {
		// TODO Auto-generated destructor stub
		cleanUp();
	}
	int byteSize() {
		int byteSize = mInt().byteSize();
		for (int i = 0; i < size; i++) {
			byteSize = byteSize + array[i].byteSize();
		}
		return byteSize+size;
	}
	std::string toString() {
		char outArray[31 * size + 12];
		outArray[0] = 0;
		char myDelimiter[2];
		myDelimiter[0] = delimiter;
		myDelimiter[1] = 0;
		std::strcat(outArray, tag.toString().c_str());
		std::strcat(outArray, myDelimiter);
		for (int i = 0; i < size; i++) {
			std::strcat(outArray, array[i].toString().c_str());
			std::strcat(outArray, myDelimiter);
		}
		std::string output(outArray);
		return output;
	}

	//todo
	void readFromCharArray(char * input) {
		std::string myInput(input);
		int length = strlen(input);
		size_t found = myInput.find(':');

		char outArray1[11];
		strncpy(outArray1, input, found);
		outArray1[found] = 0;
		tag.readFromCharArray(outArray1);

		int i = found + 1;
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
		int startPtr = found + 1;
		int endPtr = found + 1;
		for (int i = startPtr; i < mySize; i++) {
			char tmpArray[30];
			while (input[endPtr] != delimiter) {
				endPtr++;
			}
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
		inSize = byteEncode2(output);
		return output;
	}
	int byteEncode2(char * buffer) {
		int j = tag.byteEncode2(&buffer[1]);
		buffer[0] = ((char) j);
		j++;

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
		delimiter=':';
		int tagSize = ((int) input[0]);
		tag.byteDecode(tagSize, &input[1]);

		std::queue<K> localList;

		int j = tagSize + 1;
		int objSize = 0;
		while (j < inSize) {
			objSize = ((int) input[j]);
			K obj;
			obj.byteDecode(objSize, &input[j + 1]);
			j = j + objSize + 1;
			localList.push(obj);
		}

		int realSize = localList.size();
		if (realSize != size) {
			if (size != 0) {
				cleanUp();
			}
			size = realSize;
			array = new K[realSize];
		}
		for (int i = 0; i < realSize; i++) {
			array[i] = localList.front();
			localList.pop();
		}
	}
	std::size_t local_hash_value() const {
		if (size > 0) {
			return array[0].local_hash_value();
		}
		return 0;
	}

	mArrayIntTagNK & operator=(const mArrayIntTagNK& rhs) {
		delimiter = rhs.getDelimiter();
		tag = rhs.getTag();
		//cleanUp();
		size = rhs.getSize();
		array = new K[size];
		K * tmpArray = rhs.getArray();
		for (int i = 0; i < size; i++) {
			array[i] = tmpArray[i];
		}
		return *this;
	}
	bool operator==(const IdataType& rhs) const {
		int size2 = ((mArrayIntTagNK&) rhs).getSize();
		if (size != size2) {
			return false;
		}
		K * array2 = ((mArrayIntTagNK&) rhs).getArray();
		for (int i = 0; i < size; i++) {
			if (!(array[i] == array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator<(const IdataType& rhs) const {
		int size2 = ((mArrayIntTagNK&) rhs).getSize();
		K * array2 = ((mArrayIntTagNK&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] >= array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator>(const IdataType& rhs) const {
		int size2 = ((mArrayIntTagNK&) rhs).getSize();
		K * array2 = ((mArrayIntTagNK&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] <= array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator<=(const IdataType& rhs) const {
		int size2 = ((mArrayIntTagNK&) rhs).getSize();
		K * array2 = ((mArrayIntTagNK&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] > array2[i])) {
				return false;
			}
		}
		return true;
	}
	bool operator>=(const IdataType& rhs) const {
		int size2 = ((mArrayIntTagNK&) rhs).getSize();
		K * array2 = ((mArrayIntTagNK&) rhs).getArray();
		for (int i = 0; i < size && i < size2; i++) {
			if ((array[i] < array2[i])) {
				return false;
			}
		}
		return true;
	}

	//Class specific methods
	void cleanUp() {
		if (size != 0) {
			delete[] array;
			size = 0;
		}
	}
	K * getArray() const{
		return array;
	}
	int getSize() const{
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
	void setDelimiter(char inDelimiter) {
		delimiter = inDelimiter;
	}
	char getDelimiter() const{
		return delimiter;
	}
	mInt getTag() const{
		return tag;
	}

};
#endif /* MLONG_H_ */
