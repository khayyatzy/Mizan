################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/computation/computeManager.cpp \
../src/computation/systemWideInfo.cpp 

OBJS += \
./src/computation/computeManager.o \
./src/computation/systemWideInfo.o 

CPP_DEPS += \
./src/computation/computeManager.d \
./src/computation/systemWideInfo.d 


# Each subdirectory must supply rules for building sources it contributes
src/computation/%.o: ../src/computation/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O0 -g3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


