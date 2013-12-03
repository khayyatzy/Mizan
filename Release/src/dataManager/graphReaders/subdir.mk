################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/dataManager/graphReaders/IgraphReader.cpp \
../src/dataManager/graphReaders/hdfsGraphReader.cpp \
../src/dataManager/graphReaders/sharedDiskGraphReader.cpp 

OBJS += \
./src/dataManager/graphReaders/IgraphReader.o \
./src/dataManager/graphReaders/hdfsGraphReader.o \
./src/dataManager/graphReaders/sharedDiskGraphReader.o 

CPP_DEPS += \
./src/dataManager/graphReaders/IgraphReader.d \
./src/dataManager/graphReaders/hdfsGraphReader.d \
./src/dataManager/graphReaders/sharedDiskGraphReader.d 


# Each subdirectory must supply rules for building sources it contributes
src/dataManager/graphReaders/%.o: ../src/dataManager/graphReaders/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


