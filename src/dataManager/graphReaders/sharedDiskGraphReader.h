/*
 * hdfsGraphReader.h
 *
 *  Created on: Mar 25, 2012
 *      Author: khayyzy
 */

#ifndef SHAREDDISKGRAPHREADER_H_
#define SHAREDDISKGRAPHREADER_H_
#include <iostream>
#include <fstream>

#include "IgraphReader.h"

class sharedDiskGraphReader: public IgraphReader {
private:
	const static int defIOBuffer = 4096;
	const char* intputFile;
	std::ifstream myfile;

	void openFile();

public:
	sharedDiskGraphReader(char * filePath, int bufferSize);

	//inherited methods
	fileStatus readBlock(char * inputBuffer, int &retBufferSize,
			int reqBufferSize);
	fileStatus isFileReadable();
	int getDefaultIBlockSize();
	int getSuggBlockSize();
	void closeTheFile();

	~sharedDiskGraphReader();
};

#endif /* HDFSGRAPHREADER_H_ */
