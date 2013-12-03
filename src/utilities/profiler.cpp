/*
 * profiler.cpp
 *
 *  Created on: Jan 17, 2011
 *      Author:
 */

#include "profiler.h"

/*
timespec diff(timespec start, timespec end)
{
	timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
}

timespec add(timespec t1, timespec t2)
{
	timespec temp;
	if ((t1.tv_nsec+t2.tv_nsec)>=1000000000) {
		temp.tv_sec = t1.tv_sec+t2.tv_sec+1;
		temp.tv_nsec = t1.tv_nsec+t2.tv_nsec-1000000000;
	} else {
		temp.tv_sec = t1.tv_sec+t2.tv_sec;
		temp.tv_nsec = t1.tv_nsec+t2.tv_nsec;
	}
	return temp;
}
void Profiler::startTimer(string name) {

	if (running[name])
		writeToLog("WARNING: Timer [" + name + "] is already running.", true);

	timespec timer;
	clock_gettime(CLOCK_REALTIME, &timer);
	timers[name] = timer;
	running[name] = true;
}

void Profiler::pauseTimer(string name) {

	if (timers.find(name) == timers.end())
		writeToLog("WARNING: Timer [" + name + "] does not exist.", true);
	if (!running[name])
		writeToLog("WARNING: Timer [" + name + "] is already stopped.", true);

	timespec current;
	clock_gettime(CLOCK_REALTIME, &current);
	timespec period = diff(timers[name], current);
	if (timerPeriods.find(name) == timerPeriods.end()) //not found, insert the period
		timerPeriods[name] = period;
	else{
		timerPeriods[name] = add(period, timerPeriods[name]);
	}
	running[name] = false;
}

void Profiler::clearTimer(string name) {
	if (timers.find(name) == timers.end())
		writeToLog("WARNING: Timer [" + name + "] does not exist.", true);
	timerPeriods[name].tv_nsec = 0;
	timerPeriods[name].tv_sec = 0;
}

double Profiler::readPeriod(string name) {
	if (timers.find(name) == timers.end())
		writeToLog("WARNING: Timer [" + name + "] does not exist.", true);


	return timerPeriods[name].tv_sec + timerPeriods[name].tv_nsec/1000000000.0;
}

bool Profiler::isRunning(string name) {
	return running[name];
}
*/
