/*
 * hdfsGraphReader.cpp
 *
 *  Created on: Mar 25, 2012
 *      Author: khayyzy
 */

#include "IgraphReader.h"
#include "sharedDiskGraphReader.h"

sharedDiskGraphReader::sharedDiskGraphReader(char * filePath, int bufferSize) :
		IgraphReader(bufferSize), intputFile(filePath) {
	if (bufferSize < 0) {
		setBlockSize(defIOBuffer);
	}
	openFile();
}

void sharedDiskGraphReader::openFile() {
	myfile.open(intputFile, std::ios::in);
}

fileStatus sharedDiskGraphReader::readBlock(char * inputBuffer,
		int &retBufferSize, int reqBufferSize) {
	fileStatus status = isFileReadable();
	if (status == success) {
		//retBufferSize = hdfsRead(fs, hdfs_file, inputBuffer, reqBufferSize);
		myfile.read(inputBuffer, reqBufferSize);
		retBufferSize = myfile.gcount();
	}
	return status;
}
fileStatus sharedDiskGraphReader::isFileReadable() {
	if (myfile.is_open()) {
		return success;
	} else if (myfile.bad()) {
		return fail_fileSystemNotReachable;
	} else {
		return fail_unknown;
	}
}

int sharedDiskGraphReader::getDefaultIBlockSize() {
	return defIOBuffer;
}

int sharedDiskGraphReader::getSuggBlockSize() {
	return defIOBuffer;
}
void sharedDiskGraphReader::closeTheFile() {
	myfile.close();
}
sharedDiskGraphReader::~sharedDiskGraphReader() {
	//hdfsCloseFile(fs, hdfs_file);
	//hdfsDisconnect(fs);
}

