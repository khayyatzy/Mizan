/*
 * argParser.h
 *
 *  Created on: Mar 9, 2013
 *      Author: refops
 */

#ifndef ARGPARSER_H_
#define ARGPARSER_H_

#include <boost/program_options.hpp>
#include "../general.h"

#include <iostream>
#include <string>

class argParser {
public:
	argParser() {
	}

	virtual ~argParser() {
	}

	MizanArgs parse(int argc, char** argv) {
		const size_t ERROR_IN_COMMAND_LINE = 1;
		const size_t SUCCESS = 0;
		const size_t ERROR_UNHANDLED_EXCEPTION = 2;
		const size_t EXIT = 3;

		MizanArgs args;
		//default args
		args.algorithm = 1;
		args.fs = HDFS;
		args.partition = hashed;
		args.migration = NONE;
		args.communication = _pt2ptb;
		args.superSteps = 20;

		int partition = -1;
		int fileSystem = -1;
		int migration = -1;
		char * graphName;
		char * hdfsUserName;

		boost::program_options::options_description desc("Options");

		desc.add_options()("help,h", "Print help messages")\
("algorithm,a",
				boost::program_options::value<int>(),
				"Algorithm ID:\n\t 1)PageRank (default),\n\t 2)TopK PageRank,\n\t 3)Diameter Estimation,\n\t 4)Ad Simulation")(
				"supersteps,s", boost::program_options::value<int>(),
				"Max Number of Supersteps")("workers,w",
				boost::program_options::value<int>()->required(),
				"Number of Workers/Partitions")("graph,g",
				boost::program_options::value<string>()->required(),
				"Input Graph Name")("fs,f",
				boost::program_options::value<int>()->required(),
				"Input File system:\n\t 1)HDFS (default),\n\t 2)Local disk")(
				"partition,p", boost::program_options::value<int>(),
				"Partitioning Type:\n\t 1)Hash (default),\n\t 2)range")(
				"user,u", boost::program_options::value<string>(),
				"Linux user name, required in case of\n using option (-fs 1)")(
				"migration,m", boost::program_options::value<int>(),
				"(Advanced Option) Dynamic load balancing type:\n\t 1)none (default),\n\t 2)Delayed Migration,\n\t 3)Mix Migration Mode,\n\t 4)Pregel Work Stealing");

		boost::program_options::variables_map vm;
		try {
			boost::program_options::store(
					boost::program_options::parse_command_line(argc, argv,
							desc), vm); // can throw

			//cout << "args.hdfsUserName = " << args.hdfsUserName << std::endl;
			//cout << "args.graphName = " << args.graphName << std::endl;

			if (vm.count("help")) {
				std::cout << "Basic Command Line Parameter App" << std::endl
						<< desc << std::endl;
				exit(-1);
			}
			if (vm.count("algorithm")) {
				args.algorithm = (vm["algorithm"].as<int>());
			}
			if (vm.count("supersteps")) {
				args.superSteps = (vm["supersteps"].as<int>());
			}
			if (vm.count("workers")) {
				args.clusterSize = (vm["workers"].as<int>());
			}
			if (vm.count("graph")) {
				args.graphName.append(vm["graph"].as<std::string>());
			}
			if (vm.count("fs")) {
				fileSystem = (vm["fs"].as<int>());
				if (fileSystem == 1) {
					args.fs = HDFS;
					if ((args.fs == HDFS && args.hdfsUserName.length() == 0)) {
						std::cerr
								<< "ERROR: You have to specify the linux current user by using (-u username)."
								<< std::endl;
						exit(-1);
					}
				} else if (fileSystem == 2) {
					args.fs = OS_DISK_ALL;
				} else {
					cerr << "fileSystem Error " << endl;
					exit(-1);
				}
			}
			if (vm.count("partition")) {
				partition = (vm["partition"].as<int>());
				if (partition == 1) {
					args.partition = hashed;
				} else if (partition == 2) {
					args.partition = range;
				}
			}
			if (vm.count("user")) {
				if (args.fs == HDFS) {
					args.hdfsUserName.append(vm["user"].as<std::string>());
				} else {
					std::cerr
							<< "ERROR: You have to specify HDFS file System through --fs."
							<< std::endl;
					exit(-1);
				}
				//args.fs = HDFS;
			}

			if (vm.count("migration")) {
				migration = (vm["migration"].as<int>());
				if (migration == 1) {
					args.migration = NONE;
				} else if (migration == 2) {
					args.migration = DelayMigrationOnly;
				} else if (migration == 3) {
					args.migration = MixMigration;
				} else if (migration == 4) {
					args.migration = PregelWorkStealing;
				}
			}
			/*if ((args.fs == HDFS && args.hdfsUserName.length() == 0) || args.fs == NOFS) {
			 std::cerr
			 << "ERROR: You have to specify the linux current user by using (-u username)."
			 << std::endl;
			 exit(-1);
			 }*/

			boost::program_options::notify(vm);
		} catch (boost::program_options::error& e) {
			std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
			std::cerr << desc << std::endl;
			exit(-1);
		}
		return args;
	}

	char ** setInputPaths(fileSystem diskType, int pCount, string fileName,
			string userName, distType partition) {
		char ** inputBaseFile = (char **) calloc(pCount, sizeof(char *));

		if (diskType == HDFS) {
			char head[] = "/user/";
			char mid[] = "/m_output/mizan_";
			char tail[] = "/part-r-";

			char buffer[10];
			sprintf(buffer, "%d", pCount);

			int size = strlen(head) + strlen(mid) + strlen(tail) + 100;
			for (int i = 0; i < pCount; i++) {

				inputBaseFile[i] = (char *) calloc(size, sizeof(char));
				strcat(inputBaseFile[i], head);
				strcat(inputBaseFile[i], userName.c_str());
				strcat(inputBaseFile[i], mid);
				strcat(inputBaseFile[i], fileName.c_str());
				if (partition == hashed) {
					strcat(inputBaseFile[i], "_mhash_");
				} else if (partition == minCuts) {
					strcat(inputBaseFile[i], "_minc_");
				} else if (partition == range) {
					strcat(inputBaseFile[i], "_mrange_");
				}
				strcat(inputBaseFile[i], buffer);
				strcat(inputBaseFile[i], tail);
				char buffer2[10];
				if (i < 10) {
					sprintf(buffer2, "0000%d", i);
				} else if (i < 100) {
					sprintf(buffer2, "000%d", i);
				} else if (i < 1000) {
					sprintf(buffer2, "00%d", i);
				} else if (i < 10000) {
					sprintf(buffer2, "0%d", i);
				} else {
					sprintf(buffer2, "%d", i);
				}
				strcat(inputBaseFile[i], buffer2);
			}
		} else if (diskType == OS_DISK_ALL) {
			char mid[] = "mizan_";
			char tail[] = "/part-r-";

			char buffer[10];
			sprintf(buffer, "%d", pCount);

			int size = strlen(mid) + strlen(tail) + 100;
			for (int i = 0; i < pCount; i++) {

				inputBaseFile[i] = (char *) calloc(size, sizeof(char));
				strcat(inputBaseFile[i], mid);
				strcat(inputBaseFile[i], fileName.c_str());
				if (partition == hashed) {
					strcat(inputBaseFile[i], "_mhash_");
				} else if (partition == minCuts) {
					strcat(inputBaseFile[i], "_minc_");
				} else if (partition == range) {
					strcat(inputBaseFile[i], "_mrange_");
				}
				strcat(inputBaseFile[i], buffer);
				strcat(inputBaseFile[i], tail);
				char buffer2[10];
				if (i < 10) {
					sprintf(buffer2, "0000%d", i);
				} else if (i < 100) {
					sprintf(buffer2, "000%d", i);
				} else if (i < 1000) {
					sprintf(buffer2, "00%d", i);
				} else if (i < 10000) {
					sprintf(buffer2, "0%d", i);
				} else {
					sprintf(buffer2, "%d", i);
				}
				strcat(inputBaseFile[i], buffer2);
			}
		}
		return inputBaseFile;
	}
};

#endif /* ARGPARSER_H_ */
