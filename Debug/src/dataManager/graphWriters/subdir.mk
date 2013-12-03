################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/dataManager/graphWriters/IgraphWriter.cpp \
../src/dataManager/graphWriters/hdfsGraphWriter.cpp 

OBJS += \
./src/dataManager/graphWriters/IgraphWriter.o \
./src/dataManager/graphWriters/hdfsGraphWriter.o 

CPP_DEPS += \
./src/dataManager/graphWriters/IgraphWriter.d \
./src/dataManager/graphWriters/hdfsGraphWriter.d 


# Each subdirectory must supply rules for building sources it contributes
src/dataManager/graphWriters/%.o: ../src/dataManager/graphWriters/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O0 -g3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


