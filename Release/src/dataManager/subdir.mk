################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/dataManager/dataManager.cpp \
../src/dataManager/userVertexObject.cpp 

OBJS += \
./src/dataManager/dataManager.o \
./src/dataManager/userVertexObject.o 

CPP_DEPS += \
./src/dataManager/dataManager.d \
./src/dataManager/userVertexObject.d 


# Each subdirectory must supply rules for building sources it contributes
src/dataManager/%.o: ../src/dataManager/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


