/*
 * Icommunicate.h
 *
 *  Created on: Apr 2, 2012
 *      Author: refops
 */

#ifndef ICOMMUNICATE_H_
#define ICOMMUNICATE_H_

#include "dataStructures/general.h"
#include "thread_manager.h"
#include "dataStructures/bufferpool.h"
#include "dataStructures/dht.h"

template<class K, class V1, class M, class A> class DHT;
template<class K, class V, class M, class A> class Mizan;

template<class K, class V1, class M, class A>
class Icommunicate {
private:
public:
	//Icommunicate(){}
	//virtual ~Icommunicate(){}
	virtual void BroadcastSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri)=0;
	virtual void process_queueCmds() = 0;
	virtual void InformOwnership(K vertex_id)=0;
	virtual void sendMPICommand(char*, int, int)=0;
	virtual void RecvMPICommand(char*, int&, int&)=0;
	virtual char* RequestBuff(int&)=0;
	virtual void free_bpSendBuff(int)=0;
	virtual void free_bpBuff(int)=0;
	virtual void SendNONBlock(char*, int, int, int)=0;
	virtual void SendNONBlock(char*, int, int)=0;
	virtual void sendSyncMessage(char*, int, int)=0;
	virtual void sendSYSMessageBYID(K &id, char* THE_MSG, int its_size)=0;
	virtual void AppendBuffMsg(char*, int, int)=0;
	virtual void SendDataImmediately()=0;
	virtual void listen_4cmds()=0;
	virtual bool sendMessage(K & id, M & THE_MSG)=0;
	virtual void sendMessageToInNbrs(K & srcId, M & msg)=0;
	virtual void sendMessageOutNbrs(K & srcId, M & msg) = 0;
	virtual void sendMessageToAll(M &) = 0;
	virtual void MapDHT(DHT<K, V1, M, A> * x)=0;
	virtual void MapMizan(Mizan<K, V1, M, A> * x)=0;
	virtual int get_rank()=0;
	virtual int get_psize()=0;
};

#endif /* ICOMMUNICATE_H_ */
