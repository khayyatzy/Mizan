/*
 * message.h
 *
 *  Created on: Apr 17, 2012
 *      Author: awaraka
 */

#ifndef MESSAGE_H_
#define MESSAGE_H_

#include "general.h"

class message {

private:
	messageCode msgType;
	long src;
	char* content;

public:
	char* serialize_obj() {
	}
	char* decerialize_obj() {
	}

};

#endif /* MESSAGE_H_ */
