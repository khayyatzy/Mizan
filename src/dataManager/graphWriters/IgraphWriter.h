#ifndef IGRAPHWRITER_H_
#define IGRAPHWRITER_H_

#include "../dataStructures/general.h"

class IgraphWriter {
protected:
	int IOBufferSize;
public:
	IgraphWriter(int IOBufferSize);
	virtual ~IgraphWriter();

	virtual void writeBlock(int size, char * array)=0;
	virtual fileStatus isFileWritable()=0;
	virtual int getDefaultIBlockSize()=0;
	virtual void closeTheFile()=0;

	int getConfBlockSize() {
		return IOBufferSize;
	}
	void setBlockSize(int blockSize) {
		IOBufferSize = blockSize;
	}

};

#endif /* IGRAPHREADER_H_ */
