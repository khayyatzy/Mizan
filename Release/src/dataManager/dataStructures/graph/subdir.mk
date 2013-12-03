################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/dataManager/dataStructures/graph/Ielement.cpp \
../src/dataManager/dataStructures/graph/edge.cpp \
../src/dataManager/dataStructures/graph/mObject.cpp \
../src/dataManager/dataStructures/graph/messageIterator.cpp \
../src/dataManager/dataStructures/graph/vertex.cpp 

OBJS += \
./src/dataManager/dataStructures/graph/Ielement.o \
./src/dataManager/dataStructures/graph/edge.o \
./src/dataManager/dataStructures/graph/mObject.o \
./src/dataManager/dataStructures/graph/messageIterator.o \
./src/dataManager/dataStructures/graph/vertex.o 

CPP_DEPS += \
./src/dataManager/dataStructures/graph/Ielement.d \
./src/dataManager/dataStructures/graph/edge.d \
./src/dataManager/dataStructures/graph/mObject.d \
./src/dataManager/dataStructures/graph/messageIterator.d \
./src/dataManager/dataStructures/graph/vertex.d 


# Each subdirectory must supply rules for building sources it contributes
src/dataManager/dataStructures/graph/%.o: ../src/dataManager/dataStructures/graph/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


