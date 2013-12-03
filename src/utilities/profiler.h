/*
 * profiler.h
 *
 *  Created on: Jan 17, 2011
 *      Author: y
 */

#ifndef PROFILER_H_
#define PROFILER_H_

#include "../communication/dataStructures/general.h"
#include <ctime>

class Profiler {

private:

	map<string, timespec> timers;
	map<string, timespec> timerPeriods;
	map<string, bool> running;

public:

	void startTimer(string name);
	void pauseTimer(string name);

	void clearTimer(string name);

	double readPeriod(string name);
	bool isRunning(string name);

};

#endif /* PROFILER_H_ */
