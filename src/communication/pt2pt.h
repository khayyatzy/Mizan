/*
 * pt2pt.h
 *
 *  Created on: Apr 11, 2012
 *      Author: awaraka
 */

#ifndef PT2PT_H_
#define PT2PT_H_

#include "Icommunicate.h"

//
//template<class K, class V, class A>
//class pt2pt: public Icommunicate<K,V,A> {
//
//private:
//	bufferpool bp;
//	int rank;
//	int psize;
//	//vector<vector<char> > dataMsgs(psize, vector<char> (msgsize));
//
//	void set_rank(int val) {
//		this->rank = val;
//	}
//	void set_psize(int val) {
//		this->psize = val;
//	}
//
//public:
//
//	pt2pt(int argc, char** argv) {
//		int val;
//		int provided;
//		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
//
//		if (provided != 3) {
//			cout << "ERROR: Thread MPI not enabled..." << endl;
//			exit(0);
//		}
//		//set up values of the compute node
//		MPI_Comm_rank(MPI_COMM_WORLD, &val);
//		set_rank(val);
//		MPI_Comm_size(MPI_COMM_WORLD, &val);
//		set_psize(val);
//
//		cout << "Rank " << this->rank << " started Successfully..." << endl;
//
//	}
//
//	void AppendBuffMsg(char* buf, int buf_len, int PEdest)
//	{
//
//	}
//
//	void listen_4cmds() {}
//	void sendMessage(K id, V THE_MSG) {}
//
//
//	void SendDataImmediately() {}
//	void sendMPICommand(char * sendbuf, int buf_size, int dest) {
//		MPI_Request send_req;
//		MPI_Status status;
//		MPI_Isend(sendbuf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
//				&send_req);
//		MPI_Wait(&send_req, &status);
//	}
//
//	void RecvMPICommand(char* recvbuf, int& from_rank) {
//		//recvbuf = new char[msgsize];
//		MPI_Request recv_req;
//		MPI_Status status;
//		MPI_Recv(recvbuf, data_msgsize, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,
//				MPI_COMM_WORLD, &status);
//		from_rank = status.MPI_SOURCE;
//	}
//
//	char* RequestBuff(int & buff_index) {
//		char * buf = bp.request_buffer(buff_index);
//		return buf;
//	}
//
//	void SendNONBlock(char* buf, int buff_index, int buf_size,  int dest) {
//
//
//		if(dest != -1)
//			MPI_Isend(buf, buf_size, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
//				(bp.mpi_reqs[buff_index]));
//
//		bp.set_free_buff(buff_index);
//
//		//MPI_Test(&(bp.mpi_reqs[buff_index]), &flag, &current_status);
//	}
//
//	void finalize() {
//		MPI_Finalize();
//	}
//	int get_rank() {
//		return rank;
//	}
//	int get_psize() {
//		return psize;
//	}
//
//};

#endif /* PT2PT_H_ */
