/*
 * general.h
 *
 *  Created on: Apr 2, 2012
 *      Author: refops
 */

#ifndef GENERAL_H_
#define GENERAL_H_

#include <string.h>
#include <iostream>
#include <queue>
#include "map"
#include <stdio.h>
#include <stdlib.h>

using namespace std;

#include "boost/thread/mutex.hpp"
#include "boost/thread/exceptions.hpp"
#include "mpi.h"

static int KB = 1024;
static int MB = 1024 * 1024;
static int data_msgsize = 4 * 1024;
static int buffer_msgsize = 4 * KB; //* KB
static queue<char*> SYS_cmdQueue;
static queue<char*> DATA_cmdQueue;

enum messageStatus {
	m_success, m_fail,
};

enum messageCode {
	DM, BCast, AllNB,
};

enum communicationType {
	_pt2pt, _ring, _pt2ptb,
};

enum msgHeader {
	_SYS, _DATA, _EXIT_PE,
};
enum SYS_CMDS {
	DHT_I, //dht_insert
	DHT_U, //dht_update
	DHT_A, //dht_ask
	DHT_R, //dht_response
	InitVertexCount,
	FinishInit,
	EndofSS,
	StartSS,
	Terminate,
	ENDMSG,
	VertexMigrate,
	SendSoftVertex,
	SendHardVertex,
	StealVertex,
	SendStolenVertex,
	StolenVertexResult,
	GraphMutation,
	LateStats,LateStatsTerminate,
	StealBarrier,
	Aggregator,
	MigrateBarrier,
};

enum DATA_CMDS {
	SSdata, InNbrs, OutNbrs, ALLVTX, ENDDMSG,
};
enum SYS_CMDS_PRIORITY {
	NO_PRIORITY, AFTER_DATABUFFER_PRIORITY, INSTANT_PRIORITY
};

enum block_type {
	INT, DOUBLE, CHAR, LONG_LONG,
};

static const char* msgHeader_strings[] = { "_SYS", "_DATA", "_EXIT_PE" };
static const char* DATA_CMDS_strings[] = { "SSdata", "InNbrs", "OutNbrs", "ALLVTX", "ENDDMSG" };
static const char* SYS_CMDS_strings[] = { "DHT_I", "DHT_U", "DHT_A", "DHT_R",
		"InitVertexCount", "FinishInit", "EndofSS", "StartSS", "Terminate",
		"ENDMSG", "SSExecTime", "VertexMigrate", "SendSoftVertex",
		"SendHardVertex", "StealVertex", "SendStolenVertex",
		"StolenVertexResult" };

#endif /* GENERAL_H_ */
