/*
 * ring.h
 *
 *  Created on: May 19, 2012
 *      Author: awaraka
 */

#ifndef RING_H_
#define RING_H_

#include "Icommunicate.h"


class pt2pt: public Icommunicate {

private:
	int rank;
	int psize;
	int successive_rank;
	int predecssor_rank;

	//vector<vector<char> > dataMsgs(psize, vector<char> (msgsize));

	void set_rank(int val) {
		this->rank = val;
	}
	void set_psize(int val) {
		this->psize = val;
	}

	void set_successive_rank()
	{
		if(rank != psize)
		{
			successive_rank++;
		} else
		{
			successive_rank = 0 ;
		}
	}

	void set_predecssor_rank()
		{
			if(rank != 0)
			{
				successive_rank--;
			} else
			{
				successive_rank = psize ;
			}
		}


public:

	pt2pt(int argc, char** argv) {
		int val;
		int provided;
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

		if (provided != 3)
		{
			cout<<"ERROR: Thread MPI not enabled..."<<endl;
			exit(0);
		}
		//set up values of the compute node
		MPI_Comm_rank(MPI_COMM_WORLD, &val);
		set_rank(val);
		MPI_Comm_size(MPI_COMM_WORLD, &val);
		set_psize(val);

		set_successive_rank();
		set_predecssor_rank();

		cout<<"Rank "<<this->rank<<" started Successfully..."<<endl;

	}

	void sendMPICommand(char * sendbuf, int dest) {
			MPI_Request send_req;
			MPI_Status status;
			MPI_Isend(sendbuf, msgsize, MPI_CHAR, dest, rank, MPI_COMM_WORLD,
					&send_req);
			//MPI_Wait(&send_req, &status);
		}

	void RecvMPICommand(char* recvbuf, int& from_rank) {
			//recvbuf = new char[msgsize];
			MPI_Request recv_req;
			MPI_Status status;
			MPI_Recv(recvbuf, msgsize, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
			from_rank = status.MPI_SOURCE;
		}
	void finalize ()
	{
	    MPI_Finalize();
	}
	int get_rank() {
		return rank;
	}
	int get_psize() {
		return psize;
	}

};




#endif /* RING_H_ */
