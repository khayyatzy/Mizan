/*
 * hdfsGraphReader.h
 *
 *  Created on: Mar 25, 2012
 *      Author: khayyzy
 */

#ifndef HDFSGRAPHREADER_H_
#define HDFSGRAPHREADER_H_

#include "IgraphReader.h"
#include "hdfs.h"

class hdfsGraphReader: public IgraphReader {
private:
	const static int HDFSdefIOBuffer = 4096;
	const static char * host;
	const static tPort port = 0;

	//HDFS variables
	hdfsFS fs;
	hdfsFile hdfs_file;
	const char* hdfs_host;
	tPort hdfs_port;
	//file variables
	const char* intputFile;

	void openFile();

public:
	hdfsGraphReader(char* filePath, int bufferSize, char* host,
			tPort port);
	hdfsGraphReader(char* filePath, int bufferSize);

	//inherited methods
	fileStatus readBlock(char * inputBuffer, int &retBufferSize,int reqBufferSize);
	fileStatus isFileReadable();
	int getDefaultIBlockSize();
	int getSuggBlockSize();
	void closeTheFile();

	//Own methods
	int HDFSAvailable();

	~hdfsGraphReader();
};

#endif /* HDFSGRAPHREADER_H_ */
