/*
 * pt2ptb.h
 *
 *  Created on: Jun 9, 2012
 *      Author: awaraka
 */

#ifndef PT2PTB_H_
#define PT2PTB_H_

#include "Icommunicate.h"
#include "dataStructures/dht.h"
#include "dataStructures/gdht.h"
#include "dataStructures/map-queue.h"
#include "dataStructures/multi-val-map.h"
#include "dataStructures/multi-val-map2.h"
#include "dataStructures/bufferpool.h"
#include "dataStructures/MyQueue.h"
#include "pt2ptb.h"
#include "thread_manager.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../dataManager/dataStructures/graph/edge.h"
#include "../computation/computeManager.h"
#include "../Mizan.h"
#include"../dataManager/dataStructures/data/mCharArrayNoCpy.h"

template<class K, class V1, class M, class A> class DHT;
template<class K, class V1, class M, class A> class Mizan;
template<class K, class V1, class M, class A> class Icommunicate;
template<class T> class myqueue;

template<class K, class V1, class M, class A>
class pt2ptb: public Icommunicate<K, V1, M, A> {

private:
	bufferpool bp;
	Mizan<K, V1, M, A> * mizan;
	DHT<K, V1, M, A>* dht_comm;
	DHT<K, V1, M, A>* discovered_maps;
	KeyQueMap<K, V1, M, A> k_DataMsgs;
	KeyQueMap<K, V1, M, A> k_SYSMsgs;
	MultiValMap2<K, V1, M, A> V_PEs;
	int rank;
	int psize;
	vector<char*> DATA_buffers;
	vector<char *> DATA_buffers_cursors;
	vector<int> DATA_buffers_counters;
	vector<int> DATA_buffers_indexes;
	boost::mutex DATAbuff;

	int EndofSS_count;
	int k_dataMsgsCount;
	boost::mutex k_dataMsgsCount_lock;
	boost::mutex hamza;

	myqueue<pair<char*, int> > CMDSQueue;

	void initialize_DATABuffers() {
		DATA_buffers.resize(psize);
		DATA_buffers_cursors.resize(psize);
		DATA_buffers_counters.resize(psize);
		DATA_buffers_indexes.resize(psize);

		for (int i = 0; i < psize; i++) {
			reset_databuffer(i);
		}

	}

	void append_int_val(char*& buf, int val) {
		memcpy(buf, &val, sizeof(int));
		buf += sizeof(int);
	}

	void reset_databuffer(int PEdest) {

//		DATA_buffers[PEdest] = bp.request_buffer(DATA_buffers_indexes[PEdest]);
//		DATA_buffers_cursors[PEdest] = DATA_buffers[PEdest];
//		DATA_buffers_counters[PEdest] = 0;
//		msgHeader mCode = _DATA;
//		append_int_val(DATA_buffers_cursors[PEdest], mCode);
//		DATA_buffers_counters[PEdest] += sizeof(int);

		DATA_buffers[PEdest] = (char*) malloc(data_msgsize * sizeof(char));
		DATA_buffers_cursors[PEdest] = DATA_buffers[PEdest];
		DATA_buffers_counters[PEdest] = 0;
		msgHeader mCode = _DATA;
		append_int_val(DATA_buffers_cursors[PEdest], mCode);
		DATA_buffers_counters[PEdest] += sizeof(int);

	}

	bool validate_DATAbuffer_insertion(const int databuf_size, const int dest) {
		//int for char size integer + original char value size + the destination integer

		int count = (DATA_buffers_counters[dest] + databuf_size);

		//cout << "(" << getRank() << ") Buf: "<<count<<">? "<<data_msgsize<<endl;

		return (count < (data_msgsize - (sizeof(int) * 2)));
	}

	void execute_DataCmd(char*& recv_buf, int from_rank) {
		DATA_CMDS dCode;
		memcpy(&dCode, recv_buf, sizeof(int));
		recv_buf += sizeof(int);
				cout << "(" << rank << ") Received -- " << DATA_CMDS_strings[dCode]
						<< " message from " << from_rank << endl;
		while (dCode != ENDDMSG) {

			if (dCode == SSdata) {
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
				/*
				 cout << "(" << this->getRank() << ") MSG SIZE: " << msg_size

				 << endl;
				 cout.flush();*/

				//we insert msg data
				char * recv_msg = (char*) malloc(msg_size * sizeof(char));
				memcpy(recv_msg, recv_buf, msg_size);
				recv_buf += msg_size;

				M myMessage;
				myMessage.byteDecode(msg_size, recv_msg);

				mizan->appendIncomeQueue(dht_VID_val, myMessage, from_rank);
				free(dht_VID);
				free(recv_msg);
			} else {
				if (dCode == InNbrs) {
					int id_size;
					memcpy(&id_size, recv_buf, sizeof(int));
					recv_buf += sizeof(int);

					char *dht_VID = (char*) malloc(id_size * sizeof(char));
					memcpy(dht_VID, recv_buf, id_size);
					recv_buf += id_size;

					K vertex_id;
					vertex_id.byteDecode(id_size, dht_VID);

					int msg_size;
					memcpy(&msg_size, recv_buf, sizeof(int));
					recv_buf += sizeof(int);
					/*
					 cout << "(" << this->getRank() << ") MSG SIZE: " << msg_size

					 << endl;
					 cout.flush();*/

					//we insert msg data
					char * recv_msg = (char*) malloc(msg_size * sizeof(char));
					memcpy(recv_msg, recv_buf, msg_size);
					recv_buf += msg_size;

					M myMessage;
					myMessage.byteDecode(msg_size, recv_msg);

					mizan->appendIncomeQueueNbr(vertex_id, myMessage, InNbrs);
					free(dht_VID);

					free(recv_msg);

				} else {
					if (dCode == OutNbrs) {

						int id_size;
						memcpy(&id_size, recv_buf, sizeof(int));
						recv_buf += sizeof(int);

						char *dht_VID = (char*) malloc(id_size * sizeof(char));
						memcpy(dht_VID, recv_buf, id_size);
						recv_buf += id_size;

						K vertex_id;
						vertex_id.byteDecode(id_size, dht_VID);

						int msg_size;
						memcpy(&msg_size, recv_buf, sizeof(int));
						recv_buf += sizeof(int);
						/*
						 cout << "(" << this->getRank() << ") MSG SIZE: " << msg_size

						 << endl;
						 cout.flush();*/

						//we insert msg data
						char * recv_msg = (char*) malloc(
								msg_size * sizeof(char));
						memcpy(recv_msg, recv_buf, msg_size);
						recv_buf += msg_size;

						M myMessage;
						myMessage.byteDecode(msg_size, recv_msg);

						mizan->appendIncomeQueueNbr(vertex_id, myMessage,
								OutNbrs);
						free(dht_VID);
						free(recv_msg);

					} else {
						if (dCode == ALLVTX) {

							int msg_size;
							memcpy(&msg_size, recv_buf, sizeof(int));
							recv_buf += sizeof(int);

							char * recv_msg = (char*) malloc(
									msg_size * sizeof(char));
							memcpy(recv_msg, recv_buf, msg_size);
							recv_buf += msg_size;

							M myMessage;
							myMessage.byteDecode(msg_size, recv_msg);

							mizan->appendIncomeQueueAll(myMessage);
							free(recv_msg);

						} else {
							cout << "(" << rank
									<< ") UNKNOWN COMMAND IN DATA MSG" << endl;
							cout.flush();

							throw(210);
						}
					}

				}

			}

			memcpy(&dCode, recv_buf, sizeof(int));
			recv_buf += sizeof(int);

		}
	}

	void execute_SYSCmds(char* recvbuf, int from_rank) {
		SYS_CMDS sysCode;
		memcpy(&sysCode, recvbuf, sizeof(int));
		recvbuf += sizeof(int);

		while (sysCode != ENDMSG) {
			//
			/*cout << "(" << getRank() << ") parsed a "
			 << SYS_CMDS_strings[sysCode] << " code from " << from_rank
			 << endl;
			 cout.flush();*/

			if (sysCode == DHT_I) {
				//msg format: id size, id, dest
				int id_size, dest_val;
				memcpy(&id_size, recvbuf, sizeof(int));
				recvbuf += sizeof(int);

				//cout << "(" << getRank() << ") Size: " << id_size << endl;
				char *dht_VID = (char*) malloc(id_size * sizeof(char));
				memcpy(dht_VID, recvbuf, id_size);
				recvbuf += id_size;

				memcpy(&dest_val, recvbuf, sizeof(int));
				recvbuf += sizeof(int);

				K dht_VID_value;
				dht_VID_value.byteDecode(id_size, dht_VID);

//				cout << "(" << rank << ") Remoted DHT("
//						<< dht_VID_value.getValue() << "," << dest_val << ")"
//						<< endl;
//				cout.flush();

				int dest_dht = dht_comm->retrieve_val(dht_VID_value);
				dht_comm->insert_KeyVal(dht_VID_value, dest_val);

				if (dest_dht != -1) {

//					cout << "###(" << rank
//							<< ") THIS IS A MIGRATED VERTEX new DHT["
//							<< ((mLong) dht_VID_value).getValue() << ","
//							<< dest_val << "]" << endl;
//					cout.flush();

					vector<int> PEs_list = V_PEs.retrieve_val(dht_VID_value);

//					cout << "\t ###(" << rank << ") ITS Associated PEs: ";
//					for (int i = 0; i < PEs_list.size(); i++) {
//						cout << PEs_list[i] << ",";
//					}
//					cout << endl;
//					cout.flush();

					for (int i = 0; i < PEs_list.size(); i++) {
						int buf_idx;
						char* buf = (char*) malloc(
								(6 * sizeof(int) + id_size) * sizeof(char)); // bp.request_buffer(buf_idx);
						char* buf_start = buf;

						msgHeader x = _SYS;
						append_int_val(buf, x);
						SYS_CMDS y = DHT_U;
						append_int_val(buf, y);

						memcpy(buf, &id_size, sizeof(int));
						buf += sizeof(int);

						//we insert the actual ID encoded in char
						memcpy(buf, dht_VID, id_size);
						buf += id_size;

						//we insert dest
						memcpy(buf, &dest_val, sizeof(int));
						buf += sizeof(int);

						y = ENDMSG;
						append_int_val(buf, y);

						int buf_count = buf - buf_start;

						sendMPICommand(buf_start, buf_count, PEs_list[i]);
						free(buf_start);
						//SendNONBlock(buf_start, buf_idx, buf_count,
						//	PEs_list[i]);
						//bp.set_free_buff(buf_idx);
					}

				}

				free(dht_VID);
			} else {
				if (sysCode == DHT_A) {

					//					cout << "(" << getRank() << ") Received "
					//							<< SYS_CMDS_strings[sysCode] << " from ["
					//							<< from_rank << "]" << endl;
					//					cout.flush();
					//size of id , id char encoded
					int id_size;
					memcpy(&id_size, recvbuf, sizeof(int));
					recvbuf += sizeof(int);

					char *dht_VID = (char*) malloc(id_size * sizeof(char));
					memcpy(dht_VID, recvbuf, id_size);
					recvbuf += id_size;

					K dht_VID_value;
					dht_VID_value.byteDecode(id_size, dht_VID);

					int desired_dest = dht_comm->retrieve_val(dht_VID_value);

//					cout<<"PE"<<rank<<" saving interval["<<dht_VID_value.getValue()<<","<<from_rank<<"]"<<endl;
//					cout.flush();
					if (mizan->isDynamicPartEnabled()) {
						V_PEs.insert_KeyVal(dht_VID_value, from_rank);
					}
					if (desired_dest != -1) {
						int buff_index;
						char* the_buf = (char*) malloc(
								((6 * sizeof(int)) + id_size) * sizeof(char)); //RequestBuff(buff_index);
						char* responsebuf_start = the_buf;
						msgHeader mCode = _SYS;
						append_int_val(responsebuf_start, mCode);
						append_DHT_RSP(dht_VID, id_size, desired_dest,
								responsebuf_start);
						//						cout << "(" << getRank() << ") The answer of Val: "
						//								<< dht_VID_value.getValue() << "'s location is "
						//								<< desired_dest << " - forwarding to: "
						//								<< from_rank << endl;
						//						cout.flush();
						SYS_CMDS sys = ENDMSG;
						append_int_val(responsebuf_start, sys);
						int buf_size = responsebuf_start - the_buf;
						sendMPICommand(the_buf, buf_size, from_rank);
						free(the_buf);
//						SendNONBlock(the_buf, buff_index, buf_size, from_rank);
//						bp.set_free_buff(buff_index);

					} else {
						cout << "(" << rank
								<< ")  ERROR: INVALID DHT_A command: Vertex ID "
								<< dht_VID_value.getValue()
								<< " does not exist in the DHT" << endl;
						cout.flush();

						throw 208;
					}

					free(dht_VID);

				} else {
					if (sysCode == DHT_R) {

						int id_size;
						int discovered_dest;
						memcpy(&id_size, recvbuf, sizeof(int));
						recvbuf += sizeof(int);

						char *dht_VID = (char*) malloc(id_size * sizeof(char));
						memcpy(dht_VID, recvbuf, id_size);
						recvbuf += id_size;

						K dht_VID_value;
						dht_VID_value.byteDecode(id_size, dht_VID);

						memcpy(&discovered_dest, recvbuf, sizeof(int));
						recvbuf += sizeof(int);

						boost::mutex::scoped_lock test_lock =
								boost::mutex::scoped_lock(
										this->k_dataMsgsCount_lock);

						if (!mizan->isSteelComputeMode()) {
							discovered_maps->erase_key(dht_VID_value);
							discovered_maps->insert_KeyVal(dht_VID_value,
									discovered_dest);
						}

						ConstructbMsg_of_PendingData(dht_VID_value,
								discovered_dest, dht_VID, id_size);

						k_dataMsgsCount--;
						if (k_dataMsgsCount == 0) {
							hamza.unlock();
						}

						test_lock.unlock();

						Check_SYS_MSGS(dht_VID_value, discovered_dest);

						free(dht_VID);

					} else {

						if (sysCode == DHT_U) {

							int id_size, dest_val;
							memcpy(&id_size, recvbuf, sizeof(int));
							recvbuf += sizeof(int);

							//cout << "(" << getRank() << ") Size: " << id_size << endl;
							char *dht_VID = (char*) malloc(
									id_size * sizeof(char));
							memcpy(dht_VID, recvbuf, id_size);
							recvbuf += id_size;

							memcpy(&dest_val, recvbuf, sizeof(int));
							recvbuf += sizeof(int);

							K dht_VID_value;
							dht_VID_value.byteDecode(id_size, dht_VID);

//							cout << "(" << rank << ") UPDATED DHT("
//									<< dht_VID_value.getValue() << ","
//									<< dest_val << ")" << endl;
//							cout.flush();

							//

							discovered_maps->insert_KeyVal(dht_VID_value,
									dest_val);
							int test_dest = discovered_maps->retrieve_val(
									dht_VID_value);
//							cout << "(" << rank << ") UPDATED MAP("
//									<< dht_VID_value.getValue() << ","
//									<< test_dest << ")" << endl;
//							cout.flush();
							free(dht_VID);

						} else {

							if (sysCode == EndofSS) {

								EndofSS_count++;
//								cout << "(" << rank << ") EndofSS count="
//										<< EndofSS_count << endl;
//								cout.flush();

//								if (EndofSS_count == psize) {
//									bool isempty = CMDSQueue.isQeueEmpty();
//									cout << "(" << rank
//											<< ") after the all 16 ENDOFSS: QUEUE is "
//											<< isempty << endl;
//									cout.flush();
//								}

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

										char* c = (char*) malloc(
												idsize * sizeof(char));
										//we insert the actual ID encoded in char
										memcpy(c, recvbuf, idsize);
										recvbuf += idsize;

										mizan->processSysCommand(sysCode,
												idsize, c);
										free(c);
									}

								}

							} else {

								/*if (sysCode == InitVertexCount
								 || sysCode == FinishInit
								 || sysCode == EndofSS
								 || sysCode == Terminate
								 || sysCode == StartSS
								 || sysCode == SSExecTime
								 || sysCode == VertexMigrate) {*/

//							cout << "(" << rank << ") in sys  "<<endl;
//							cout << "(" << rank << ") Received "
//									<< SYS_CMDS_strings[sysCode] << " from ["
//									<< from_rank << "]" << endl;
//							cout.flush();
								if (sysCode == StartSS) {
									k_dataMsgsCount = 0;
									EndofSS_count = 0;
								}

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

										char* c = (char*) malloc(
												idsize * sizeof(char));
										//we insert the actual ID encoded in char
										memcpy(c, recvbuf, idsize);
										recvbuf += idsize;

										mizan->processSysCommand(sysCode,
												idsize, c);
										free(c);
									}

								}
							}

							/*} else {

							 cout << "(" << rank
							 << ") ############# ERROR: UNKNOWN COMMAND ############## "
							 << endl;

							 }*/

						}
					}

				}
			}

			memcpy(&sysCode, recvbuf, sizeof(int));
			recvbuf += sizeof(int);

		}

	}

	void Check_SYS_MSGS(K &dht_VID_value, int new_dest) {
		std::vector<mCharArrayNoCpy> p_array;
		p_array = k_SYSMsgs.retrieve_val(dht_VID_value);
		for (int i = 0; i < p_array.size(); i++) {
			int buf_index;
			char * buf = (char*) malloc(p_array[i].getSize() * sizeof(char)); // bp.request_buffer(buf_index);

			memcpy(buf, p_array[i].getValue(), p_array[i].getSize());
			free(p_array[i].getValue());

			sendMPICommand(buf, p_array[i].getSize(), new_dest);
			free(buf);
			//SendNONBlock(buf, buf_index, p_array[i].getSize(), new_dest);
			//bp.set_free_buff(buf_index);
		}
	}

	void append_IdataType(char*& recvbuf, int idsize, char* c) {
//		int idsize;
//		char* c = value.byteEncode(idsize);

		//we insert the string length
		memcpy(recvbuf, &idsize, sizeof(int));
		recvbuf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(recvbuf, c, idsize);
		recvbuf += idsize;

//		free(c);

	}

	void ConstructbMsg_of_PendingData(K & e1, int new_dest, char* encoded_id,
			int id_size) {
		std::vector<mCharArrayNoCpy> p_array;

		//		cout << "(" << getRank() << ") retrieving msg values for VID["
		//				<< e1.getValue() << "] " << endl;
		//		cout.flush();
		p_array = k_DataMsgs.retrieve_val(e1);

		//append to msg

		int data_msg_count = 0;
		char * temp;

		for (int i = 0; i < p_array.size(); i++) {
			char* the_buf = (char*) malloc(data_msgsize * sizeof(char));
			char* responsebuf_start = the_buf;

			//msgHeader HeaderM = _DATA;
			DATA_CMDS dcode = SSdata;
			data_msg_count = 0;
			responsebuf_start = the_buf;
			//append_int_val(responsebuf_start, HeaderM);

			append_int_val(responsebuf_start, dcode);
			MsgContent_PendingData(responsebuf_start, encoded_id, id_size,
					p_array[i].getValue(), p_array[i].getSize());

			//dcode = ENDDMSG;
			//append_int_val(responsebuf_start, dcode);
			data_msg_count = responsebuf_start - the_buf;
			AppendBuffMsg(the_buf, data_msg_count, new_dest);
			//sendMPICommand(the_buf, data_msg_count, new_dest);
			free(the_buf);

		}

		//free(the_buf);

	}

	void test_msgPair(K &e1, mCharArrayNoCpy &x) {
		if (e1.getValue() == 5) {

			int temp_int;
			char* temp_x_val = x.getValue();
			memcpy(&temp_int, temp_x_val, sizeof(int));
			temp_x_val += sizeof(int);

			char* tmp_char = (char*) malloc(temp_int * sizeof(char));
			memcpy(tmp_char, temp_x_val, temp_int);

			M myMessage;
			myMessage.byteDecode(temp_int, tmp_char);
			cout << "*********** (" << rank << ") Test Sent Msg: "
					<< (myMessage.getValue()) << endl;
		}

	}

	void MsgContent_PendingData(char* & buf, char* encoded_id, int id_size,
			char* msg, int msg_size) {

		//we insert the string length
		memcpy(buf, &id_size, sizeof(int));
		buf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(buf, encoded_id, id_size);
		buf += id_size;

		//we insert the msg size + data
		memcpy(buf, msg, msg_size);
		buf += msg_size;

	}

	void append_DHT_RSP(char* dht_VID, int id_size, int desired_dest,
			char* & buf) {
		SYS_CMDS sys = DHT_R;

		memcpy(buf, &sys, sizeof(int));
		buf += sizeof(int);

		//we insert the string length
		memcpy(buf, &id_size, sizeof(int));
		buf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(buf, dht_VID, id_size);
		buf += id_size;

		//add msg content
		memcpy(buf, &desired_dest, sizeof(int));
		buf += sizeof(int);

	}
	void append_DHT_Ask(K &id, char* & buf) {
		SYS_CMDS sys = DHT_A;
		int size;

		char* c = id.byteEncode(size);

		memcpy(buf, &sys, sizeof(int));
		buf += sizeof(int);

		//we insert the string length
		memcpy(buf, &size, sizeof(int));
		buf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(buf, c, size);
		buf += size;

		free(c);

	}

	void append_data_content(K &id, char* & recvbuf, char* msg, int size) {

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

	void set_rank(int val) {
		this->rank = val;
	}
	void set_psize(int val) {
		this->psize = val;
	}

public:

	pt2ptb(int argc, char** argv) {
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

		k_dataMsgsCount = 0;
		EndofSS_count = 0;
		initialize_DATABuffers();
		discovered_maps = new DHT<K, V1, M, A>();

		cout << "Rank " << this->rank << " started Successfully..." << endl;

	}

	void MapMizan(Mizan<K, V1, M, A> * x) {
		mizan = x;
	}

	void MapDHT(DHT<K, V1, M, A> * x) {
		dht_comm = x;
	}

	bool sendMessage(K &id, M &THE_MSG) {

		int msglen = 0;
		int buf_size = 0;
		char * msg = THE_MSG.byteEncode(msglen);
		bool exist = mizan->vertexExists(id);

		if (!exist) {

//				cout << "(" << rank << ") Vertex ID: " << id.getValue()
//						<< " does not exist locally" << endl;
//				cout.flush();

			int loc = /*id.getValue() % psize ;*/dht_comm->retrieve_val(id);

			boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock(
					this->k_dataMsgsCount_lock);

			if (loc == -1) {
				int extra_info = discovered_maps->retrieve_val(id);
				if (extra_info == -1) {

					//ask remote dht
					char * Sendbuf = (char*) malloc(
							data_msgsize * sizeof(char));

					char *buf_start = Sendbuf;

					msgHeader mCode = _SYS;
					append_int_val(buf_start, mCode);

					append_DHT_Ask(id, buf_start);
					if (k_dataMsgsCount == 0) {
						hamza.lock();
					}
					k_dataMsgsCount++;

					SYS_CMDS sys = ENDMSG;
					append_int_val(buf_start, sys);

					int dest = id.local_hash_value() % psize;

					//keep statistics for later use
					if (!mizan->isSteelComputeMode()) {
						discovered_maps->insert_KeyVal(id, -2);
					}
					char * temp_buf = (char*) malloc(
							(sizeof(int) + msglen) * sizeof(char));
					char* temp_buf_start = temp_buf;
					memcpy(temp_buf, &msglen, sizeof(int));
					temp_buf += sizeof(int);
					memcpy(temp_buf, msg, msglen);
					temp_buf += msglen;

					mCharArrayNoCpy tmp_obj((sizeof(int) + msglen),
							temp_buf_start);
					k_DataMsgs.insert_KeyVal(id, tmp_obj);

					//k_msgs.insert_KeyVal(id, msg, msglen);

					buf_size = buf_start - Sendbuf;
					sendMPICommand(Sendbuf, buf_size, dest);
					free(Sendbuf);

				} else {

					if (extra_info != -2) {

//							cout << "(" << rank << ") Vertex ID: "
//									<< id.getValue()
//									<< " exists in Discovered DHT" << endl;
//							cout.flush();

						int dataMsg_count = 0;
						char * Sendbuf = (char*) malloc(
								data_msgsize * sizeof(char));
						char *buf_start = Sendbuf;

						//msgHeader HeadCode = _DATA;
						//append_int_val(buf_start, HeadCode);
						DATA_CMDS dsys = SSdata;
						append_int_val(buf_start, dsys);

						append_data_content(id, buf_start, msg, msglen);

						//dsys = ENDDMSG;
						//append_int_val(buf_start, dsys);
						dataMsg_count = buf_start - Sendbuf;
						AppendBuffMsg(Sendbuf, dataMsg_count, extra_info);
						//sendMPICommand(Sendbuf, dataMsg_count, extra_info);

						free(Sendbuf);
						free(msg);

					} else {

						char * temp_buf = (char*) malloc(
								(sizeof(int) + msglen) * sizeof(char));
						char* temp_buf_start = temp_buf;
						memcpy(temp_buf, &msglen, sizeof(int));
						temp_buf += sizeof(int);
						memcpy(temp_buf, msg, msglen);
						temp_buf += msglen;

						mCharArrayNoCpy tmp_obj((sizeof(int) + msglen),
								temp_buf_start);
						k_DataMsgs.insert_KeyVal(id, tmp_obj);
//						boost::mutex::scoped_lock test_lock =
//								boost::mutex::scoped_lock(
//										this->k_dataMsgsCount_lock);
//						if (k_dataMsgsCount == 0) {
//							hamza.lock();
//						}
//						k_dataMsgsCount++;
//						test_lock.unlock();

					}

				}

				test_lock.unlock();

				//
			} else {
				//
				int dataMsg_count = 0;
				char * Sendbuf = (char*) malloc(data_msgsize * sizeof(char));
				char *buf_start = Sendbuf;
				//msgHeader HeadCode = _DATA;
				//append_int_val(buf_start, HeadCode);
				DATA_CMDS dsys = SSdata;
				append_int_val(buf_start, dsys);

				append_data_content(id, buf_start, msg, msglen);
				//dsys = ENDDMSG;
				//append_int_val(buf_start, dsys);
				dataMsg_count = buf_start - Sendbuf;
				AppendBuffMsg(Sendbuf, dataMsg_count, loc);
				//sendMPICommand(Sendbuf, dataMsg_count, loc);
				free(Sendbuf);
				free(msg);
			}

		} else {
			//message is local
			//			cout << "(" << getRank() << ") Vertex ID: " << id.getValue()
			//					<< " exists locally" << endl;
			//			cout.flush();
			mizan->appendLocalIncomeQueue(id, THE_MSG);
			free(msg);
		}
		return exist;
	}

	void sendSYSMessageBYID(K &id, char* THE_MSG, int its_size) {

		bool exist = mizan->vertexExists(id);

		if (!exist) {

			int loc = id.getValue() % psize; //dht_comm->retrieve_val(id);
			if (loc == -1) {
				int extra_info = discovered_maps->retrieve_val(id);
				if (extra_info == -1) {
					//ask remote dht
					char * Sendbuf = (char*) malloc(
							data_msgsize * sizeof(char));

					char *buf_start = Sendbuf;

					msgHeader mCode = _SYS;
					append_int_val(buf_start, mCode);

					append_DHT_Ask(id, buf_start);
					SYS_CMDS sys = ENDMSG;
					append_int_val(buf_start, sys);

					long dest = id.local_hash_value() % psize;

					//keep statistics for later use
					if (!mizan->isSteelComputeMode()) {
						discovered_maps->insert_KeyVal(id, -2);
					}
					mCharArrayNoCpy tmp_obj(its_size, THE_MSG);
					k_SYSMsgs.insert_KeyVal(id, tmp_obj);

					//k_msgs.insert_KeyVal(id, msg, msglen);

					int buf_size = buf_start - Sendbuf;
					sendMPICommand(Sendbuf, buf_size, dest);
					free(Sendbuf);

				} else {

					if (extra_info != -2) {

						int buf_index;
						char * buf = (char*) malloc(
								(its_size + 1) * sizeof(char)); //bp.request_buffer(buf_index);
						memcpy(buf, THE_MSG, its_size);

						sendMPICommand(buf, its_size, extra_info);
						free(buf);
						//SendNONBlock(buf, buf_index, its_size, extra_info);
						//bp.set_free_buff(buf_index);

						free(THE_MSG);

					} else {

						mCharArrayNoCpy tmp_obj(its_size, THE_MSG);
						k_SYSMsgs.insert_KeyVal(id, tmp_obj);

					}

				}

				//
			} else {
				//

				int buf_index;
				char * buf = (char*) malloc(its_size * sizeof(char)); // bp.request_buffer(buf_index);
				memcpy(buf, THE_MSG, its_size);

				sendMPICommand(buf, its_size, loc);
				free(buf);
				//SendNONBlock(buf, buf_index, its_size, loc);
				//bp.set_free_buff(buf_index);

				free(THE_MSG);

			}

		} else {

			int buf_index;
			char * buf = (char*) malloc(
					(its_size + sizeof(int)) * sizeof(char)); //bp.request_buffer(buf_index);
			memcpy(buf, THE_MSG, its_size);

			sendMPICommand(buf, its_size, rank);
			free(buf);
			//SendNONBlock(buf, buf_index, its_size, rank);
			//bp.set_free_buff(buf_index);
			free(THE_MSG);
		}

	}

	void free_bpBuff(int index) {
		bp.set_freeNotSend_buff(index);
	}

	void SendDataImmediately() {

		DATA_CMDS dsys = ENDDMSG;
		int buf_size;
		for (int i = 0; i < psize; i++) {

			boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock(
								this->DATAbuff);
			if (DATA_buffers_counters[i] > sizeof(int)) {

				append_int_val(DATA_buffers_cursors[i], dsys);

				buf_size = DATA_buffers_cursors[i] - DATA_buffers[i];
				sendMPICommand(DATA_buffers[i], buf_size, i);
				free(DATA_buffers[i]);
				//SendNONBlock(DATA_buffers[i], DATA_buffers_indexes[i], buf_size,
				//		i);
				//free_bpSendBuff(DATA_buffers_indexes[i]);
				reset_databuffer(i);
			}
			test_lock.unlock();
		}

		//clearing the k_msgs map
//		vector<K> LeftOver_keys = k_msgs.retrieve_LeftoverKeys();
//
//		for (int i = 0; i < LeftOver_keys.size(); i++) {
//			int idsize;
//			char* encoded_id = LeftOver_keys[i].byteEncode(idsize);
//			int new_dest = discovered_maps->retrieve_val( LeftOver_keys[i]);
//			if(new_dest < 0)
//			{
//				cout<<"******* ("<<rank<<") ERRORRR IN DISCOVERED MAPS: INVALID RANK"<<endl;
//			}
//			ConstructbMsg_of_PendingData(LeftOver_keys[i], new_dest, encoded_id,
//					idsize);
//			free(encoded_id);
//		}

	}

	void AppendBuffMsg(char* buf, int buf_len, int PEdest) {


		try {
			boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock(
					this->DATAbuff);
			bool can_insert = validate_DATAbuffer_insertion(buf_len, PEdest);
			
			cout << "(" << rank << ") can_insert:" <<can_insert << endl;
			cout.flush();
			
			if (can_insert) {
				memcpy(DATA_buffers_cursors[PEdest], buf, buf_len);
				DATA_buffers_cursors[PEdest] += buf_len;

				DATA_buffers_counters[PEdest] += buf_len;

			} else {
				DATA_CMDS dsys = ENDDMSG;
				append_int_val(DATA_buffers_cursors[PEdest], dsys);

				int buf_size = DATA_buffers_cursors[PEdest]
						- DATA_buffers[PEdest];

				sendMPICommand(DATA_buffers[PEdest], buf_size, PEdest);
				free(DATA_buffers[PEdest]);
				//SendNONBlock(DATA_buffers[PEdest], DATA_buffers_indexes[PEdest],
				//		buf_size, PEdest);
				//bp.set_free_buff(DATA_buffers_indexes[PEdest]);
				reset_databuffer(PEdest);

				memcpy(DATA_buffers_cursors[PEdest], buf, buf_len);
				DATA_buffers_cursors[PEdest] += buf_len;
				DATA_buffers_counters[PEdest] += buf_len;

			}

			test_lock.unlock();

		} catch (boost::lock_error & e) {
			cout << "ERROR AppendBuffMsg: BOOST LOCK ERROR" << endl;
			exit(0);
		}
	}

	void sendSyncMessage(char* sendbuf, int buf_size, int dest) {

//		cout<<"("<<rank<<") Sync Send to process["<<dest<<"]"<<endl;
//		cout.flush();
		MPI_Ssend(sendbuf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD);
	}

	void sendMPICommand(char * sendbuf, int buf_size, int dest) {
		MPI_Request send_req;
		MPI_Status status;
		MPI_Isend(sendbuf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
				&send_req);
		MPI_Wait(&send_req, &status);
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

	char* RequestBuff(int & buff_index) {
		char * buf = bp.request_buffer(buff_index);
		return buf;
	}

	void free_bpSendBuff(int indx) {
		bp.set_free_buff(indx);
	}
	void SendNONBlock(char* buf, int buf_size, int dest) {

		MPI_Request req;
		MPI_Isend(buf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD, &req);

	}

	void SendNONBlock(char* buf, int buff_index, int buf_size, int dest) {

		if (dest != -1)
			MPI_Isend(buf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
					(bp.mpi_reqs[buff_index]));

		//bp.set_free_buff(buff_index);

		//MPI_Test(&(bp.mpi_reqs[buff_index]), &flag, &current_status);
	}

	void sendMessageToInNbrs(K &srcId, M &msg) {

		int msglen = 0;
		char *the_msg = msg.byteEncode(msglen);

		vector<edge<K, V1> *> * dest_VIds = mizan->getInEdges(srcId);
		std::set<int> dest_ranks;
		std::set<int>::iterator it = dest_ranks.begin();
		bool found_ranks = true;
		for (int i = 0; i < dest_VIds->size() && found_ranks; i++) {
			K dstVer = dest_VIds->operator [](i)->getID();

			bool exist = mizan->vertexExists(dstVer);

			if (!exist) {
				int loc = dht_comm->retrieve_val(dstVer);
				if (loc == -1) {
					int extra_info = discovered_maps->retrieve_val(dstVer);
					if (extra_info < 0) {
						found_ranks = false;
					} else {
						dest_ranks.insert(extra_info);
					}
				} else {
					dest_ranks.insert(loc);
				}
			} else {
				dest_ranks.insert(rank);
			}

		} //end of vertices

		if (found_ranks) {
			it = dest_ranks.begin();
			for (; it != dest_ranks.end(); it++) {
				int dst_pe = *it;
				if (dst_pe != rank) {
					int index;
					char* buf = (char*) malloc(data_msgsize * sizeof(char));
					char *orig_start = buf;

					msgHeader HeadCode = _DATA;
					append_int_val(buf, HeadCode);
					DATA_CMDS dsys = InNbrs;
					append_int_val(buf, dsys);

					append_K_ID(buf, srcId);
					append_char_itsSize(buf, the_msg, msglen);

					dsys = ENDDMSG;
					append_int_val(buf, dsys);
					int buf_size = 0;
					buf_size = buf - orig_start;

					//AppendBuffMsg(orig_start, buf_size, dst_pe);
					sendMPICommand(orig_start, buf_size, dst_pe);

					free(orig_start);
				} else {
					mizan->appendIncomeQueueNbr(srcId, msg, InNbrs);
				}
			}

		} else {
			it = dest_ranks.begin();
			dest_ranks.erase(it, dest_ranks.end());

			for (int i = 0; i < dest_VIds->size(); i++) {
				K dstVer = dest_VIds->operator [](i)->getID();
				sendMessage(dstVer, msg);
			}

		}

		free(the_msg);

	}

	void sendMessageOutNbrs(K &srcId, M &msg) {

	
		int msglen = 0;
		char *the_msg = msg.byteEncode(msglen);

		vector<edge<K, V1> *> * dest_VIds = mizan->getOutEdges(srcId);
		std::set<int> dest_ranks;
		std::set<int>::iterator it = dest_ranks.begin();
		bool found_ranks = true;
		for (int i = 0; i < dest_VIds->size() && found_ranks; i++) {
			K dstVer = dest_VIds->operator [](i)->getID();

			bool exist = mizan->vertexExists(dstVer);

			if (!exist) {
				int loc = dht_comm->retrieve_val(dstVer);
				if (loc == -1) {
					int extra_info = discovered_maps->retrieve_val(dstVer);
					if (extra_info < 0) {
						found_ranks = false;
					} else {
						dest_ranks.insert(extra_info);
					}
				} else {
					dest_ranks.insert(loc);
				}
			} else {
				dest_ranks.insert(rank);
			}

		} //end of vertices

		//found_ranks = false;
		if (found_ranks) {
			cout << "(" << rank << ") ALL RANKS FOUND for ID#"
					<< srcId.getValue() << endl;
			cout.flush();
			it = dest_ranks.begin();
			for (; it != dest_ranks.end(); it++) {
				int dst_pe = *it;

				if (dst_pe != rank) {
					int index;
					char* buf = (char*) malloc(data_msgsize * sizeof(char)); //bp.request_buffer(index);
					char *orig_start = buf;

					//msgHeader HeadCode = _DATA;
					//append_int_val(buf, HeadCode);

					DATA_CMDS dsys = OutNbrs;
					append_int_val(buf, dsys);

					append_K_ID(buf, srcId);
					append_char_itsSize(buf, the_msg, msglen);

					//dsys = ENDDMSG;
					//append_int_val(buf, dsys);

					int buf_size = 0;
					buf_size = buf - orig_start;

					AppendBuffMsg(orig_start, buf_size, dst_pe);
					//sendMPICommand(orig_start, buf_size, dst_pe);

					free(orig_start);
					//bp.set_freeNotSend_buff(index);
				} else {
					mizan->appendIncomeQueueNbr(srcId, msg, OutNbrs);
				}
			}

		} else {

//			cout << "(" << rank << ") NOT FOUND RANKS for ID#"
//					<< srcId.getValue() << endl;
//			cout.flush();
			it = dest_ranks.begin();
			dest_ranks.erase(it, dest_ranks.end());

			for (int i = 0; i < dest_VIds->size(); i++) {
				K dstVer = dest_VIds->operator [](i)->getID();
				sendMessage(dstVer, msg);
			}

		}

		free(the_msg);

	}

	void sendMessageToAll(M &msg) {

		
	}


	void BroadcastSysMessageValue(SYS_CMDS command, IdataType &value,
			SYS_CMDS_PRIORITY pri) {


		if ( pri == AFTER_DATABUFFER_PRIORITY /* command == EndofSS*/) {

			hamza.lock();
//			cout << "(" << rank << ") k_dataMsgsCount= " << k_dataMsgsCount
//					<< endl;
//			cout.flush();
			hamza.unlock();
			SendDataImmediately();
		}
//			bool exit_loop = false;
//			while (!exit_loop) {
//				boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock(
//						this->k_dataMsgsCount_lock);
//				if (k_dataMsgsCount == 0) {
//					exit_loop = true;
//				}
//				test_lock.unlock();
//			}

		for (int i = 0; i < psize; i++) {
			int index;

			int idsize;
			char* c = value.byteEncode(idsize);
			int allocation_size = idsize + sizeof(int) * 7;

			if (allocation_size > buffer_msgsize) {
				cout << "(" << rank
						<< ") ERROR: ALLOCATION BUFFER EXCEEDED LIMIT "
						<< allocation_size << endl;
				cout.flush();
			}

			char* buf = (char*) malloc(allocation_size * sizeof(char)); //myCommMang->request_emptyBuff(index);
			char *orig_start = buf;
			msgHeader mHead = _SYS;
			append_int_val(buf, mHead);
			append_int_val(buf, command);
			int count = 1;
			append_int_val(buf, count);
			append_IdataType(buf, idsize, c);
			free(c);
			SYS_CMDS sCode = ENDMSG;
			append_int_val(buf, sCode);

			int buf_size = 0;

			buf_size = buf - orig_start;

			sendMPICommand(orig_start, buf_size, i);

			free(orig_start);

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

	void append_K_ID(char* & buf, K & id) {
		int idsize;
		char* c = id.byteEncode(idsize);

		//we insert the string length
		memcpy(buf, &idsize, sizeof(int));
		buf += sizeof(int);

		//we insert the actual ID encoded in char
		memcpy(buf, c, idsize);
		buf += idsize;
	}

	void append_char_itsSize(char*& recvbuf, char* msg, int size) {
		memcpy(recvbuf, &size, sizeof(int));
		recvbuf += sizeof(int);

		//we insert msg data
		memcpy(recvbuf, msg, size);
		recvbuf += size;
	}

	void InformOwnership(K vertex_id) {

		long mod = vertex_id.local_hash_value() % psize;
		if (rank != mod) {
//			cout << "\t &&&&&&&& (" << rank << ") Inform ownership to dht["<<mod<<"]"
//					<< ((mLong) vertex_id).getValue() << endl;
//			cout.flush();
			int idsize;
			char * c = vertex_id.byteEncode(idsize);

			int allocation_size = (sizeof(int) * 5) + idsize;
			if (allocation_size > buffer_msgsize) {
				cout << "(" << rank
						<< ") ALLOCATION BUFFER EXCEEDED LIMIT in DHT_I "
						<< allocation_size << endl;
				cout.flush();
			}

			char* send_buf = (char*) malloc(allocation_size * sizeof(char));
			char * send_buf_start = send_buf;

			msgHeader mCode = _SYS;
			append_int_val(send_buf, mCode);
			SYS_CMDS dCode = DHT_I;
			append_int_val(send_buf, dCode);
			append_int_val(send_buf, idsize);

			memcpy(send_buf, c, idsize);
			send_buf += idsize;

			append_int_val(send_buf, rank);
			dCode = ENDMSG;
			append_int_val(send_buf, dCode);
			free(c);

			int buf_size = send_buf - send_buf_start;
			sendMPICommand(send_buf_start, buf_size, mod);

			free(send_buf_start);
		} else {
//			cout << "\t ######^^^^####(" << rank << ") Inform ownership to dht (local)"
//					<< ((mLong) vertex_id).getValue() << endl;
//			cout.flush();

			int v_loc = dht_comm->retrieve_val(vertex_id);
			if (v_loc == -1) {
				dht_comm->insert_KeyVal(vertex_id, rank);
			} else {
//				cout << "\t ###^^^^####(" << rank
//						<< ") THIS IS A MIGRATED VERTEX - old DHT["
//						<< ((mLong) vertex_id).getValue() << "," << v_loc
//						<< endl;
//				cout.flush();

				dht_comm->insert_KeyVal(vertex_id, rank);
//				int new_freaking_dest = dht_comm->retrieve_val(vertex_id);
//				cout << "\t ###^^^^####(" << rank
//						<< ") THIS IS A MIGRATED VERTEX - new DHT["
//						<< ((mLong) vertex_id).getValue() << ","
//						<< new_freaking_dest << endl;
//				cout.flush();

				vector<int> PEs_list = V_PEs.retrieve_val(vertex_id);

//				cout << "\t ###(" << rank << ") ITS Associated PEs: ";
//				for (int i = 0; i < PEs_list.size(); i++) {
//					cout << PEs_list[i] << ",";
//				}
//				cout << endl;
//				cout.flush();

				for (int i = 0; i < PEs_list.size(); i++) {
					int buf_idx;
					int idsize;
					char* c = vertex_id.byteEncode(idsize);
					char* buf = (char*) malloc(
							(7 * sizeof(int) + idsize) * sizeof(char)); //bp.request_buffer(buf_idx);
					char* buf_start = buf;

					msgHeader x = _SYS;
					append_int_val(buf, x);
					SYS_CMDS y = DHT_U;
					append_int_val(buf, y);

					//we insert the string length
					memcpy(buf, &idsize, sizeof(int));
					buf += sizeof(int);

					//we insert the actual ID encoded in char
					memcpy(buf, c, idsize);
					buf += idsize;
					free(c);

					//we insert dest
					memcpy(buf, &rank, sizeof(int));
					buf += sizeof(int);

					y = ENDMSG;
					append_int_val(buf, y);

					int buf_count = buf - buf_start;

					sendMPICommand(buf_start, buf_count, PEs_list[i]);
					free(buf_start);
//					SendNONBlock(buf_start, buf_idx, buf_count, PEs_list[i]);
//					bp.set_free_buff(buf_idx);
				}

			}

		}

	}

	void listen_4cmds() {
		msgHeader mCode;
		int from_rank;
		bool exit = false;
		int actual_received_size;
		char* recv_buf = (char*) malloc(buffer_msgsize * sizeof(char));
		char *recvbuf_start = recv_buf;

		while (!exit) {

			RecvMPICommand(recv_buf, from_rank, actual_received_size);
			char* temp_buf = (char*) malloc(
					actual_received_size * sizeof(char));

			memcpy(temp_buf, recv_buf, actual_received_size);

			memcpy(&mCode, temp_buf, sizeof(int));

			if (mCode == _EXIT_PE) {
				exit = true;
			}

			CMDSQueue.enqueue(make_pair(temp_buf, from_rank));

		}
	}

	void process_queueCmds() {

		char* recv_buf;
		int from_rank;
		msgHeader mCode;
		bool exit = false;

		pair<char*, int> entry_cmd;

		while (!exit) {

			bool flag = CMDSQueue.dequeue(entry_cmd);
			if (flag) {

				recv_buf = entry_cmd.first;
				from_rank = entry_cmd.second;
				char *recvbuf_start = recv_buf;
				memcpy(&mCode, recv_buf, sizeof(int));
				recv_buf += sizeof(int);

				//			cout << "(" << getRank() << ") Received a "
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
						} else {
							cout << "(" << rank << ") UNKNOWN MSG HEADER"
									<< endl;
							cout.flush();
							throw(210);
						}

					}

				}

				free(recvbuf_start);

			}

		}
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

}
;

#endif /* PT2PTB_H_ */
