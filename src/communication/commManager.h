/*
 * communication_manager.h
 *
 *  Created on: Apr 23, 2012
 *      Author: awaraka
 */

#ifndef COMMMANAGER_H_
#define COMMMANAGER_H_

#include "dataStructures/dht.h"
#include "dataStructures/gdht.h"
#include "dataStructures/multi-val-map.h"
#include "dataStructures/bufferpool.h"
#include "ring.h"
#include "pt2ptb.h"
#include "thread_manager.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../computation/computeManager.h"
#include "../Mizan.h"

template<class K, class V1, class M, class A> class DHT;
template<class K, class V1, class M, class A> class Mizan;

template<class K, class V1, class M, class A>
class comm_manager {
private:
	thread_manager* tm;
	Mizan<K, V1, M, A> * mizan;
	DHT<K, V1, M, A>* dht_comm;

	communicationType commType;
	Icommunicate<K, V1, M, A> *dxxx;

public:

	comm_manager() {
		//cout << "Communication Manager is up..." << endl;

	}
	comm_manager(communicationType t) {
		this->commType = t;
	}
	comm_manager(communicationType t, int argc, char** argv) {
		this->commType = t;
		init(argc, argv);
	}
	bool isLocal(K id) {
		return mizan->vertexExists(id);
	}
	void init(int argc, char** argv) {

		if (this->commType == _pt2ptb) {
			dxxx = new pt2ptb<K, V1, M, A>(argc, argv);
		} else {
			if (this->commType == _ring) {
				//dxxx = new ring<K, V1, M, A>(argc, argv);
			}

		}

	}

	void MapThreadMananger(thread_manager* x) {
		tm = x;

	}

	void MapInstances(Mizan<K, V1, M, A> * x, DHT<K, V1, M, A> * y,
			thread_manager* z) {
		mizan = x;
		dht_comm = y;
		tm = z;

		dxxx->MapMizan(x);
		dxxx->MapDHT(y);
	}

	void listen_4cmds() {

		dxxx->listen_4cmds();
	}

	void sendDataImmediate() {
		dxxx->SendDataImmediately();
	}

	void send_NON_Blocking_Message(char* the_buf, int buf_size, int new_dest) {

		dxxx->SendNONBlock(the_buf, buf_size, new_dest);
	}

	void send_NON_Blocking_Message(char* the_buf, int buff_index, int buf_size,
			int new_dest) {

		dxxx->SendNONBlock(the_buf, buff_index, buf_size, new_dest);
	}

	char* request_emptyBuff(int & index) {
		return dxxx->RequestBuff(index);
	}

	void free_bpSendBuff(int & index) {
		dxxx->free_bpSendBuff(index);
	}

	void free_bpBuff(int index) {
		dxxx->free_bpBuff(index);
	}

	void InformOwnership(const K &vertex_id) {
		dxxx->InformOwnership(vertex_id);
	}

	void process_queueCmds() {

		dxxx->process_queueCmds();
	}

	void sendMessage(char* msg, int buf_size, int dest) {
		dxxx->sendMPICommand(msg, buf_size, dest);
	}

	bool sendMessage(K &id, M &THE_MSG) {
		return dxxx->sendMessage(id, THE_MSG);

	}
	void sendSYSMessageBYID(K &id, char* THE_MSG, int its_size) {

		dxxx->sendSYSMessageBYID(id, THE_MSG, its_size);

	}

	void sendMessageToInNbrs(K &srcId, M &msg) {
		dxxx->sendMessageToInNbrs(srcId, msg);

	}
	void sendMessageOutNbrs(K &srcId, M &msg) {

		dxxx->sendMessageOutNbrs(srcId, msg);

	}
	void sendMessageToAll(M &msg) {
		dxxx->sendMessageToAll(msg);
	}

	void sendSyncMessage(char* sendbuf, int buf_size, int dest_rank) {

		dxxx->sendSyncMessage(sendbuf, buf_size, dest_rank);
	}

	void BroadcastSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri) {
		dxxx->BroadcastSysMessageValue(command, value, pri);
	}

	void clearComm() {
		MPI_Finalize();
	}

	void setCommType(communicationType t) {
		this->commType = t;
	}
	communicationType getCommType() {
		return commType;
	}

	int getRank() {
		return dxxx->get_rank();
	}

	int getPsize() {
		return dxxx->get_psize();
	}

};

#endif /* COMMUNICATION_MANAGER_H_ */
