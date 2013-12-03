/*
 * hdfsGraphWriter.cpp
 *
 *  Created on: Jul 24, 2012
 *      Author: refops
 */

#include "hdfsGraphWriter.h"
const char *hdfsGraphWriter::host = "default";
hdfsGraphWriter::hdfsGraphWriter(char* filePath, int bufferSize, char* host,
		tPort port) :
		IgraphWriter(bufferSize), hdfs_host(host), hdfs_port(port), intputFile(
				filePath) {
	if (bufferSize < 0) {
		setBlockSize(HDFSdefIOBuffer);
	}
	openFile();
}
hdfsGraphWriter::hdfsGraphWriter(char* filePath, int bufferSize) :
		IgraphWriter(bufferSize), hdfs_host(host), hdfs_port(port), intputFile(
				filePath) {
	if (bufferSize < 0) {
		setBlockSize(HDFSdefIOBuffer);
	}
	openFile();
}
hdfsGraphWriter::~hdfsGraphWriter() {
	// TODO Auto-generated destructor stub
}
void hdfsGraphWriter::openFile() {
	fs = hdfsConnect(hdfs_host, hdfs_port);
	if (fs != 0) {
		//std::cout << "\t-opening HDFS file" << std::endl << std::flush;
		hdfs_file = hdfsOpenFile(fs, intputFile, O_WRONLY, 0, 0, 0);
	}
}
void hdfsGraphWriter::writeBlock(int size, char * array) {

	int wSize = hdfsWrite(fs, hdfs_file, array, size);
	int status = hdfsFlush(fs,hdfs_file);

}
fileStatus hdfsGraphWriter::isFileWritable() {
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
int hdfsGraphWriter::getDefaultIBlockSize() {
	return HDFSdefIOBuffer;
}
void hdfsGraphWriter::closeTheFile() {
	hdfsCloseFile(fs, hdfs_file);
	hdfsDisconnect(fs);
}

int hdfsGraphWriter::HDFSAvailable() {

}
