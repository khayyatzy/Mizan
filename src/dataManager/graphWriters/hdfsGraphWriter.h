/*
 * hdfsGraphWriter.h
 *
 *  Created on: Jul 24, 2012
 *      Author: refops
 */

#ifndef HDFSGRAPHWRITER_H_
#define HDFSGRAPHWRITER_H_

#include "IgraphWriter.h"
#include "hdfs.h"

class hdfsGraphWriter: public IgraphWriter {
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
	hdfsGraphWriter(char* filePath, int bufferSize, char* host, tPort port);
	hdfsGraphWriter(char* filePath, int bufferSize);
	virtual ~hdfsGraphWriter();

	//inherited methods
	void writeBlock(int size, char * array);
	fileStatus isFileWritable();
	int getDefaultIBlockSize();
	void closeTheFile();

	int HDFSAvailable();
};

#endif /* HDFSGRAPHWRITER_H_ */
