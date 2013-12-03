/*
 * bufferpool.h
 *
 *  Created on: May 21, 2012
 *      Author: awaraka
 */

#ifndef BUFFERPOOL_H_
#define BUFFERPOOL_H_

#include "general.h"

class bufferpool {
private:
	int buffer_count;
	vector<bool> buffer_inuse;
	vector<bool> firstime_used;
	boost::mutex buffer_lock;
	char** buffer;
	int buff_cursor;

	//boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock( this->buffer_lock);

	void init() {

		buff_cursor = 0;
		buffer_inuse.resize(buffer_count);
		firstime_used.resize(buffer_count);

		mpi_reqs.resize(buffer_count);
		for (int i = 0; i < buffer_count; i++) {
			mpi_reqs[i] = new MPI_Request();
		}

		buffer = (char**) malloc(buffer_count * sizeof(char*));

		for (int i = 0; i < buffer_count; i++) {
			buffer[i] = (char*) calloc(buffer_msgsize, sizeof(char));
		}

		for (int i = 0; i < buffer_count; i++) {
			buffer_inuse[i] = false;
		}

		for (int i = 0; i < buffer_count; i++) {
			firstime_used[i] = true;
		}

	}

	int next_free_buff() {
		bool found = false;
		//int i = 0;
		boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock( this->buffer_lock);
		int i = buff_cursor;
		//test_lock.unlock();

		int flag = 0;

		while (!found) {

			//test_lock = boost::mutex::scoped_lock( this->buffer_lock);
			if (!buffer_inuse[i]) {

				if(!firstime_used[i]) {
					MPI_Status current_status;
					MPI_Test(mpi_reqs[i], &flag, &current_status);

				}
				else
				{
					firstime_used[i] = false;
					flag = true;
					//found = true;
				}

			}




			if (flag == true) {
				found = true;
				buffer_inuse[i] = true;

			} else {
				i++;
			}

			if (i == buffer_count) {
				i = 0;

			}



		}





		if(i == buffer_count-1)
			buff_cursor = 0;
		else
			buff_cursor = i+1;


		test_lock.unlock();

		return i;

	}

public:
	vector<MPI_Request *> mpi_reqs;

	bufferpool() {
		buffer_count = 50;
		init();
	}
	bufferpool(int x) {
		buffer_count = x;
		init();
	}

	char* request_buffer(int & buff_index) {
		buff_index = next_free_buff();

		//cout<<"@@@@@  Found buff["<<buff_index<<"] EMPTY"<<endl;
		return buffer[buff_index];

	}

	void set_free_buff(int x) {
		boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock( this->buffer_lock);
		buffer_inuse[x] = false;
		test_lock.unlock();
	}
	void set_size(int x) {
		buffer_count = x;
		init();
	}

	void set_freeNotSend_buff(int x) {
			boost::mutex::scoped_lock test_lock = boost::mutex::scoped_lock( this->buffer_lock);
			firstime_used[x] = true;
			test_lock.unlock();
		}

};

#endif /* BUFFERPOOL_H_ */
