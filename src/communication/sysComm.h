/*
 * sysComm.h
 *
 *  Created on: May 19, 2012
 *      Author: awaraka
 */

#ifndef SYSCOMM_H_
#define SYSCOMM_H_

#include "commManager.h"
#include "dataStructures/dht.h"
#include "dataStructures/gdht.h"
#include "dataStructures/multi-val-map.h"
#include "dataStructures/bufferpool.h"
#include "pt2pt.h"
#include "thread_manager.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../computation/computeManager.h"
#include "../Mizan.h"

template<class K, class V1, class M, class A> class comm_manager;
template<class K, class V1, class M, class A>
class sysComm {
private:

	comm_manager<K, V1, M, A>* myCommMang;

	void append_ENDMSG_tail(char* &buf) {
		SYS_CMDS sys = ENDMSG;
		memcpy(buf, &sys, sizeof(int));
		buf += sizeof(int);
	}

public:

	sysComm(comm_manager<K, V1, M, A>* x) {
		myCommMang = x;
		init();
	}

	void init() {

		communicationType myCommType;
		myCommType = myCommMang->getCommType();

	}

	void append_int_val(char*& buf, int val) {
		memcpy(buf, &val, sizeof(int));
		buf += sizeof(int);
	}

	void append_IdataType(char*& recvbuf, IdataType &value) {
		int idsize;
		char* c = value.byteEncode(idsize);

		//we insert the string length
		memcpy(recvbuf, &idsize, sizeof(int));
		recvbuf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(recvbuf, c, idsize);
		recvbuf += idsize;

		free(c);

	}


	void BroadcastSysMessage(SYS_CMDS command, SYS_CMDS_PRIORITY pri) {
		/* PLEASE REMOVE ALL cout OF DHT!! */
		//send a direct sys message to all PE's, invoke processSysCommand(SYS_CMDS message) in Mizan
		if (pri == AFTER_DATABUFFER_PRIORITY) {
			myCommMang->sendDataImmediate();
		}

		for (int i = 0; i < getPsize(); i++) {

//			cout<<"("<<getRank()<<") to ["<<i<<"] Broadcast SYS CMD: "<<command<<endl;
//					cout.flush();

			int index;
			char* buf = (char*) malloc((6 * sizeof(int)) * sizeof(char)); //myCommMang->request_emptyBuff(index);
			char *orig_start = buf;
			msgHeader mCode = _SYS;
			append_int_val(buf, mCode);
			append_int_val(buf, command);
			int count = 0;
			append_int_val(buf, count);
			append_ENDMSG_tail(buf);

			int buf_size = 0;
			buf_size = buf - orig_start;

			myCommMang->sendMessage(orig_start, buf_size, i);

			free(orig_start);

		}

	}

	void BroadcastSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri) {

		myCommMang->BroadcastSysMessageValue(command, value, pri);
	}

	void sendSysMessage(SYS_CMDS command, SYS_CMDS_PRIORITY pri, int dst) {
		//send a direct sys message to dst, invoke rocessSysCommand(SYS_CMDS message) in Mizan

		if (pri == AFTER_DATABUFFER_PRIORITY) {
			myCommMang->sendDataImmediate();

		}


//		cout<<"("<<getRank()<<") to ["<<dst<<"] Sending SYS CMD: "<<command<<endl;
//		cout.flush();
		int index;
		char* buf = (char*) malloc((10 * sizeof(int)) ); // myCommMang->request_emptyBuff(index);
		char *orig_start = buf;
		msgHeader mCode = _SYS;
		append_int_val(buf, mCode);
		append_int_val(buf, command);
		int count = 0;
		append_int_val(buf, count);
		append_ENDMSG_tail(buf);

		int buf_size = buf - orig_start;

		myCommMang->sendMessage(orig_start, buf_size, dst);

		free(orig_start);

	}
	bool sendSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri, int dst) {

		if (pri == AFTER_DATABUFFER_PRIORITY) {
			myCommMang->sendDataImmediate();
		}
		int idsize;
		char* c = value.byteEncode(idsize);

		int allocation_size = (sizeof(int) * 10) + idsize;

		if (allocation_size > buffer_msgsize) {
			cout << "(" << getRank() << ") ALLOCATION BUFFER EXCEEDED LIMIT: "
					<< allocation_size << endl;
			cout.flush();

			free(c);
			return false;
		} else {


//			cout<<"("<<getRank()<<") to ["<<dst<<"] SendingVAL SYS CMD: "<<command<<endl;
//					cout.flush();

			char* buf = (char*) malloc(allocation_size * sizeof(char));
			char *orig_start = buf;
			msgHeader mCode = _SYS;
			append_int_val(buf, mCode);
			append_int_val(buf, command);
			int count = 1;
			append_int_val(buf, count);
			append_int_val(buf, idsize);

			//we insert the actual ID encoded in char
			memcpy(buf, c, idsize);
			buf += idsize;

			SYS_CMDS sCode = ENDMSG;
			append_int_val(buf, sCode);

			int buf_size = buf - orig_start;

			myCommMang->sendMessage(orig_start, buf_size, dst);

			free(c);
			free(orig_start);

			return true;
		}

	}

	bool check_for_correctness(char* sent_buf, SYS_CMDS command) {
		bool correct = true;
		msgHeader mCode;

		memcpy(&mCode, sent_buf, sizeof(int));
		sent_buf += sizeof(int);
		if (mCode != _SYS)
			correct = false;

		SYS_CMDS cCode;
		memcpy(&cCode, sent_buf, sizeof(int));
		sent_buf += sizeof(int);
		if (cCode != command)
			correct = false;


		return correct;

	}

	bool sendSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri, K& Vertex_obj) {

		int idsize;
		char* c = value.byteEncode(idsize);

		int allocation_size = (sizeof(int) * 5) + idsize;
		if (allocation_size > buffer_msgsize) {
			cout << "(" << getRank() << ") ALLOCATION BUFFER EXCEEDED LIMIT "
					<< allocation_size << endl;
			cout.flush();
			free(c);
			return false;
		} else {

			char* buf = (char*) malloc(allocation_size * sizeof(char));
			char *orig_start = buf;

			msgHeader mCode = _SYS;
			append_int_val(buf, mCode);

			append_int_val(buf, command);
			int count = 1;
			append_int_val(buf, count);

			memcpy(buf, &idsize, sizeof(int));
			buf += sizeof(int);

			memcpy(buf, c, idsize);
			buf += idsize;

			SYS_CMDS dCode = ENDMSG;
			append_int_val(buf, dCode);

			int dst; // not defined yet ...
			int buf_size = buf - orig_start;
			myCommMang->sendSYSMessageBYID(Vertex_obj, orig_start, buf_size);

			free(c);
			free(orig_start);

			return true;
		}

	}

	void InformOwnership(const K & vertex_id) {
		myCommMang->InformOwnership(vertex_id);
	}

	void sendSelfExit() {

		//This method should send to Icommunicate receiver EXIT command
		msgHeader syscmd = _EXIT_PE;
		int index;
		char* buf = myCommMang->request_emptyBuff(index);
		char *orig_start = buf;
		append_int_val(buf, syscmd);
		append_ENDMSG_tail(buf);
		int buf_size = buf - orig_start;
		myCommMang->send_NON_Blocking_Message(orig_start, index, buf_size,
				getRank());
		myCommMang->free_bpSendBuff(index);
	}

	int getRank() {
		return myCommMang->getRank();
	}

	int getPsize() {
		return myCommMang->getPsize();
	}

};

#endif /* SYSCOMM_H_ */
