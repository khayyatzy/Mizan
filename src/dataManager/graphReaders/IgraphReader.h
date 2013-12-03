/*
 * IgraphReader.h
 *
 *  Created on: Mar 25, 2012
 *      Author: khayyzy
 */

#ifndef IGRAPHREADER_H_
#define IGRAPHREADER_H_

#include "../dataStructures/general.h"


class IgraphReader {
protected:
	int IOBufferSize;
public:
	IgraphReader(int inputIOBuffer);
	virtual fileStatus readBlock(char * inputBuffer, int &retBufferSize,int reqBufferSize)=0;
	virtual fileStatus isFileReadable()=0;
	virtual int getDefaultIBlockSize()=0;
	virtual int getSuggBlockSize()=0;
	virtual void closeTheFile()=0;

	int getConfBlockSize(){
		return IOBufferSize;
	}
	void setBlockSize(int blockSize){
		IOBufferSize = blockSize;
	}

	virtual ~IgraphReader();
};

#endif /* IGRAPHREADER_H_ */
