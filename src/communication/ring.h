/*

 * ring.h
 *
 *  Created on: Jun 27, 2012
 *      Author: awaraka


#ifndef RING_H_
#define RING_H_

#include "Icommunicate.h"
#include "dataStructures/dht.h"
#include "dataStructures/gdht.h"
#include "dataStructures/multi-val-map.h"
#include "dataStructures/multi-val-map2.h"
#include "dataStructures/bufferpool.h"
#include "pt2pt.h"
#include "pt2ptb.h"
#include "thread_manager.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../computation/computeManager.h"
#include "../Mizan.h"

template<class K, class V1, class M, class A> class DHT;
template<class K, class V1, class M, class A> class Mizan;
template<class K, class V1, class M, class A> class Icommunicate;

template<class K, class V1, class M, class A>
class ring: public Icommunicate<K, V1, M, A> {

private:

	bufferpool bp;
	Mizan<K, V1, M, A> * mizan;
	DHT<K, V1, M, A>* dumb_dht;
	int rank, psize, successive_rank, predecessor_rank;
	char* DATA_buffer;
	char* DATA_buffer_cursor;
	int DATA_buffer_counter;
	int DATA_bp_buffer_index;
	boost::mutex DATAbuff;

	void execute_DataCmd(char*& recv_buf, int from_rank) {
		DATA_CMDS dCode;

		char* x_interval = recv_buf;
		memcpy(&dCode, recv_buf, sizeof(int));
		recv_buf += sizeof(int);

		while (dCode != ENDDMSG) {

//			cout << "(" << rank << ") Received -- " << DATA_CMDS_strings[dCode]
//					<< " message from " << from_rank << endl;
//			cout.flush();

			int id_size;
			memcpy(&id_size, recv_buf, sizeof(int));
			recv_buf += sizeof(int);

			char *dht_VID = (char*) malloc(id_size * sizeof(char));
			memcpy(dht_VID, recv_buf, id_size);
			recv_buf += id_size;

			K dht_VID_val;
			dht_VID_val.byteDecode(id_size, dht_VID);

			int msg_size;
			memcpy(&msg_size, recv_buf, sizeof(int));
			recv_buf += sizeof(int);

			//we insert msg data
			char * recv_msg = (char*) malloc(msg_size * sizeof(char));
			memcpy(recv_msg, recv_buf, msg_size);
			recv_buf += msg_size;

			M myMessage;
			myMessage.byteDecode(msg_size, recv_msg);

			if (mizan->vertexExists(dht_VID_val)) {
				mizan->appendIncomeQueue(dht_VID_val, myMessage, from_rank);
			} else {
				int indexbuf;
				char * temp_buf = (char*) malloc(data_msgsize * sizeof(char)); //= bp.request_buffer(indexbuf);
				int buf_count = (recv_buf - x_interval);
				memcpy(temp_buf, x_interval, buf_count);
				AppendBuffMsg(temp_buf, buf_count, -1);
				//bp.set_freeNotSend_buff(indexbuf);
				free(temp_buf);
			}

			x_interval = recv_buf;
			memcpy(&dCode, recv_buf, sizeof(int));
			recv_buf += sizeof(int);

			free(dht_VID);
			free(recv_msg);

		}
	}

	void execute_SYSCmds(char* recvbuf, int from_rank) {
		SYS_CMDS sysCode;
		memcpy(&sysCode, recvbuf, sizeof(int));
		recvbuf += sizeof(int);

		while (sysCode != ENDMSG) {
			//
			cout << "(" << rank << ") parsed a " << SYS_CMDS_strings[sysCode]
					<< " code from " << from_rank << endl;
			cout.flush();

			if (sysCode == InitVertexCount || sysCode == FinishInit
					|| sysCode == EndofSS || sysCode == Terminate
					|| sysCode == StartSS || sysCode == VertexMigrate
					|| sysCode == SendSoftVertex || sysCode == SendHardVertex
					|| sysCode == StealVertex || sysCode == SendStolenVertex
					|| sysCode == StolenVertexResult) {
				//							cout << "(" << getRank() << ") Received "
				//									<< SYS_CMDS_strings[sysCode] << " from ["
				//									<< from_rank << "]" << endl;
				//							cout.flush();

				int count = -1;
				memcpy(&count, recvbuf, sizeof(int));
				recvbuf += sizeof(int);

				if (count == 0) {
					mizan->processSysCommand(sysCode);
				} else {
					for (int k = 0; k < count; k++) {

						int idsize;
						memcpy(&idsize, recvbuf, sizeof(int));
						recvbuf += sizeof(int);

						char* c = (char*) malloc(idsize * sizeof(char));
						//we insert the actual ID encoded in char
						memcpy(c, recvbuf, idsize);
						recvbuf += idsize;

						mizan->processSysCommand(sysCode, idsize, c);
						free(c);
					}

				}

			} else {

				cout << "(" << rank
						<< ") ############# ERROR: UNKNOWN COMMAND ############## "
						<< endl;

			}

			memcpy(&sysCode, recvbuf, sizeof(int));
			recvbuf += sizeof(int);

		}

	}

	void append_data_content(K id, char* & recvbuf, char* msg, int size) {

		int idsize;
		char* c = id.byteEncode(idsize);

		//we insert the string length
		memcpy(recvbuf, &idsize, sizeof(int));
		recvbuf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(recvbuf, c, idsize);
		recvbuf += idsize;

		//we insert the msg size
		memcpy(recvbuf, &size, sizeof(int));
		recvbuf += sizeof(int);

		//we insert msg data
		memcpy(recvbuf, msg, size);
		recvbuf += size;

		free(c);
	}

	void append_int_val(char*& buf, int val) {
		memcpy(buf, &val, sizeof(int));
		buf += sizeof(int);
	}

	bool validate_DATAbuffer_insertion(const int databuf_size) {
		int count = (DATA_buffer_counter + databuf_size);
		return (count < (data_msgsize - (sizeof(int) * 2)));
	}

	void reset_databuffer() {

		DATA_buffer = bp.request_buffer(DATA_bp_buffer_index);
		DATA_buffer_cursor = DATA_buffer;
		DATA_buffer_counter = 0;
		msgHeader mCode = _DATA;
		append_int_val(DATA_buffer_cursor, mCode);
		DATA_buffer_counter += sizeof(int);

	}

	void set_rank(int val) {
		this->rank = val;
	}

	void set_psize(int val) {
		this->psize = val;
	}

	void set_successive_rank(int val) {

		if (val != psize - 1)
			this->successive_rank = val + 1;
		else
			this->successive_rank = 0;
	}

	void set_predecessor_rank(int val) {

		if (val != 0)
			this->predecessor_rank = val - 1;
		else
			this->predecessor_rank = psize - 1;
	}

public:

	ring(int argc, char** argv) {
		int val;
		int provided;
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

		if (provided != 3) {
			cout << "ERROR: Thread MPI not enabled..." << endl;
			exit(0);
		}

		//set up values of the compute node
		MPI_Comm_rank(MPI_COMM_WORLD, &val);
		set_rank(val);
		MPI_Comm_size(MPI_COMM_WORLD, &val);
		set_psize(val);

		set_successive_rank(rank);
		set_predecessor_rank(rank);

		reset_databuffer();

		cout << "Rank " << this->rank << " started Successfully..." << endl;

	}

	void InformOwnership(const K & vertex_id) {

	}

	void FinishedComputation() {

	}

	void AppendBuffMsg(char* buf, int buf_len, int PEdest) {

		boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock(
				this->DATAbuff);
		bool can_insert = validate_DATAbuffer_insertion(buf_len);
		if (can_insert) {

			memcpy(DATA_buffer_cursor, buf, buf_len);
			DATA_buffer_cursor += buf_len;
			DATA_buffer_counter += buf_len;

//			cout << "(" << rank << ") data buf is NOT FULL("<<DATA_bp_buffer_index<<")..." << endl;
//			cout.flush();

		} else {
			DATA_CMDS dsys = ENDDMSG;
			append_int_val(DATA_buffer_cursor, dsys);

			int buf_size = DATA_buffer_cursor - &(DATA_buffer[0]);

//			cout << "(" << rank << ") data buf is FULL(" << DATA_bp_buffer_index
//					<< ")..." << endl;
//			cout.flush();

			SendNONBlock(DATA_buffer, DATA_bp_buffer_index, buf_size,
					successive_rank);
			free_bpSendBuff(DATA_bp_buffer_index);
			reset_databuffer();
//			cout << "(" << rank << ") data buf reset(" << DATA_bp_buffer_index
//					<< ")..." << endl;
//			cout.flush();

			memcpy(DATA_buffer_cursor, buf, buf_len);
			DATA_buffer_cursor += buf_len;
			DATA_buffer_counter += buf_len;

		}

		test_lock.unlock();
	}

	void BroadcastSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri) {
	}

	void free_bpBuff(int index) {
		bp.set_freeNotSend_buff(index);
	}

	void SendDataImmediately() {

		DATA_CMDS dsys = ENDDMSG;
		int buf_size;

		if (DATA_buffer_counter > sizeof(int)) {

			append_int_val(DATA_buffer_cursor, dsys);

			buf_size = DATA_buffer_cursor - &(DATA_buffer[0]);
			SendNONBlock(DATA_buffer, DATA_bp_buffer_index, buf_size,
					successive_rank);
			free_bpSendBuff(DATA_bp_buffer_index);
			reset_databuffer();
		}

	}
	void sendMPICommand(char * sendbuf, int buf_size, int dest) {
		MPI_Request send_req;
		MPI_Status status;
		MPI_Isend(sendbuf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
				&send_req);
		MPI_Wait(&send_req, &status);
	}

	void SendNONBlock(char* buf, int buff_index, int buf_size, int dest) {

		if (dest != -1)
			MPI_Isend(buf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
					(bp.mpi_reqs[buff_index]));
		else {
			cout << "(" << rank << ") ERRRRROOOOOOORRRRRR: dest=-1" << endl;
			cout.flush();
		}

	}

	void SendNONBlock(char* buf, int buf_size, int dest) {

		MPI_Request req;
		MPI_Isend(buf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD, &req);

	}

	void sendSYSMessageBYID(K &id, char* THE_MSG, int its_size) {

	}

	void sendMessageToInNbrs(K &srcId, M msg) {

	}
	void sendMessageOutNbrs(K &srcId, M msg) {

	}
	void sendMessageToAll(M &msg) {

	}
	void sendSyncMessage(char* sendbuf, int buf_size, int dest) {

		MPI_Ssend(sendbuf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD);
	}

	bool sendMessage(K &id, M &THE_MSG) {

		int msglen = 0;
		int buf_size = 0;
		char * msg = THE_MSG.byteEncode(msglen);
		bool exist = mizan->vertexExists(id);

		if (!exist) {

			int dataMsg_count = 0;
			int buf_index;
			char * Sendbuf = bp.request_buffer(buf_index);(char*) malloc(
					data_msgsize * sizeof(char));
			char *buf_start = Sendbuf;
			DATA_CMDS dsys = SSdata;
			append_int_val(Sendbuf, dsys);

			append_data_content(id, Sendbuf, msg, msglen);
			dataMsg_count = Sendbuf - buf_start;
			AppendBuffMsg(buf_start, dataMsg_count, -1);
			//bp.set_freeNotSend_buff(buf_index);
			free(buf_start);

		} else {

			mizan->appendLocalIncomeQueue(id, THE_MSG);
		}

		free(msg);
		return exist;
	}

	void RecvMPICommand(char* recvbuf, int& from_rank,
			int & actual_received_size) {
		//recvbuf = new char[data_msgsize];
		MPI_Request recv_req;
		MPI_Status status;
		MPI_Recv(recvbuf, buffer_msgsize, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,
				MPI_COMM_WORLD, &status);
		from_rank = status.MPI_SOURCE;
		MPI_Get_count(&status, MPI_CHAR, &actual_received_size);
	}

	void process_queueCmds() {
	}

	void listen_4cmds() {

		msgHeader mCode;
		int from_rank;
		bool exit = false;
		char* recv_buf = (char*) malloc(buffer_msgsize * sizeof(char));
		char *recvbuf_cursor = recv_buf;
		int x;
		while (!exit) {

			RecvMPICommand(recv_buf, from_rank, x);

			memcpy(&mCode, recv_buf, sizeof(int));
			recv_buf += sizeof(int);

//			cout << "(" << rank << ") Received a "
//					<< msgHeader_strings[mCode] << " message from " << from_rank
//					<< endl;

			if (mCode == _SYS) {

				execute_SYSCmds(recv_buf, from_rank);

			} else {

				if (mCode == _DATA) {
					execute_DataCmd(recv_buf, from_rank);
				} else {
					if (mCode == _EXIT_PE) {
						exit = true;
					}
				}

			}

			recv_buf = recvbuf_cursor;
		}

	}

	char* RequestBuff(int & buff_index) {
		char * buf = bp.request_buffer(buff_index);
		return buf;
	}

	void free_bpSendBuff(int indx) {
		bp.set_free_buff(indx);
	}

	void MapMizan(Mizan<K, V1, M, A> * x) {
		mizan = x;
	}

	void MapDHT(DHT<K, V1, M, A> * x) {
		dumb_dht = x;
	}

	void finalize() {
		MPI_Finalize();
	}
	int get_rank() {
		return rank;
	}
	int get_psize() {
		return psize;
	}

};

#endif  RING_H_
*/
