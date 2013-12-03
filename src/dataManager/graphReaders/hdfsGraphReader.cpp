/*
 * hdfsGraphReader.cpp
 *
 *  Created on: Mar 25, 2012
 *      Author: khayyzy
 */

#include "IgraphReader.h"
#include "hdfsGraphReader.h"
#include <iostream>

const char *hdfsGraphReader::host = "default";

hdfsGraphReader::hdfsGraphReader(char* filePath, int bufferSize, char* host,
		tPort port) :
		IgraphReader(bufferSize), hdfs_host(host), hdfs_port(port), intputFile(
				filePath) {
	if (bufferSize < 0) {
		setBlockSize(HDFSdefIOBuffer);
	}
	openFile();
}

hdfsGraphReader::hdfsGraphReader(char* filePath, int bufferSize) :
		IgraphReader(bufferSize), hdfs_host(host), hdfs_port(port), intputFile(
				filePath) {
	if (bufferSize < 0) {
		setBlockSize(HDFSdefIOBuffer);
	}
	openFile();
}

void hdfsGraphReader::openFile() {
	//std::cout << "\t-connecting to hdfs" << std::endl << std::flush;
	fs = hdfsConnect(hdfs_host, hdfs_port);
	if (fs != 0) {
		//std::cout << "\t-opening HDFS file" << std::endl << std::flush;
		hdfs_file = hdfsOpenFile(fs, intputFile, O_APPEND, 0, 0, 0);
	}
}

fileStatus hdfsGraphReader::readBlock(char * inputBuffer, int &retBufferSize,
		int reqBufferSize) {
	fileStatus status = isFileReadable();
	if (status == success) {
		retBufferSize = hdfsRead(fs, hdfs_file, inputBuffer, reqBufferSize);
	}
	return status;
}
fileStatus hdfsGraphReader::isFileReadable() {
	if (fs != 0 && hdfs_file != 0) {
		return success;
	} else if (fs == 0) {
		return fail_fileSystemNotReachable;
	} else if (hdfsExists(fs, intputFile) != 0) {
		return fail_fileNotFound;
	} else {
		return fail_unknown;
	}
}

int hdfsGraphReader::getDefaultIBlockSize() {
	return HDFSdefIOBuffer;
}

int hdfsGraphReader::getSuggBlockSize() {
	int buffer = hdfsAvailable(fs, hdfs_file);
	if (buffer > 0) {
		return buffer;
	} else {
		return getConfBlockSize();
	}
}
void hdfsGraphReader::closeTheFile() {
	hdfsCloseFile(fs, hdfs_file);
	hdfsDisconnect(fs);
}
hdfsGraphReader::~hdfsGraphReader() {
	//hdfsCloseFile(fs, hdfs_file);
	//hdfsDisconnect(fs);
}

