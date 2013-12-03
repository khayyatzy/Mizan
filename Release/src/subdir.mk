################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/IAggregator.cpp \
../src/Mizan.cpp \
../src/dynamicPartitioner.cpp \
../src/main.cpp \
../src/unitTest.cpp 

OBJS += \
./src/IAggregator.o \
./src/Mizan.o \
./src/dynamicPartitioner.o \
./src/main.o \
./src/unitTest.o 

CPP_DEPS += \
./src/IAggregator.d \
./src/Mizan.d \
./src/dynamicPartitioner.d \
./src/main.d \
./src/unitTest.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


