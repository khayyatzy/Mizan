################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/dataManager/dataStructures/data/IdataType.cpp \
../src/dataManager/dataStructures/data/mArrayIntTagNK.cpp \
../src/dataManager/dataStructures/data/mCharArray.cpp \
../src/dataManager/dataStructures/data/mCharArrayNoCpy.cpp \
../src/dataManager/dataStructures/data/mDouble.cpp \
../src/dataManager/dataStructures/data/mDoubleArray.cpp \
../src/dataManager/dataStructures/data/mInt.cpp \
../src/dataManager/dataStructures/data/mIntCharArrayPair.cpp \
../src/dataManager/dataStructures/data/mIntTagDouble.cpp \
../src/dataManager/dataStructures/data/mLong.cpp \
../src/dataManager/dataStructures/data/mLongArray.cpp 

OBJS += \
./src/dataManager/dataStructures/data/IdataType.o \
./src/dataManager/dataStructures/data/mArrayIntTagNK.o \
./src/dataManager/dataStructures/data/mCharArray.o \
./src/dataManager/dataStructures/data/mCharArrayNoCpy.o \
./src/dataManager/dataStructures/data/mDouble.o \
./src/dataManager/dataStructures/data/mDoubleArray.o \
./src/dataManager/dataStructures/data/mInt.o \
./src/dataManager/dataStructures/data/mIntCharArrayPair.o \
./src/dataManager/dataStructures/data/mIntTagDouble.o \
./src/dataManager/dataStructures/data/mLong.o \
./src/dataManager/dataStructures/data/mLongArray.o 

CPP_DEPS += \
./src/dataManager/dataStructures/data/IdataType.d \
./src/dataManager/dataStructures/data/mArrayIntTagNK.d \
./src/dataManager/dataStructures/data/mCharArray.d \
./src/dataManager/dataStructures/data/mCharArrayNoCpy.d \
./src/dataManager/dataStructures/data/mDouble.d \
./src/dataManager/dataStructures/data/mDoubleArray.d \
./src/dataManager/dataStructures/data/mInt.d \
./src/dataManager/dataStructures/data/mIntCharArrayPair.d \
./src/dataManager/dataStructures/data/mIntTagDouble.d \
./src/dataManager/dataStructures/data/mLong.d \
./src/dataManager/dataStructures/data/mLongArray.d 


# Each subdirectory must supply rules for building sources it contributes
src/dataManager/dataStructures/data/%.o: ../src/dataManager/dataStructures/data/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	mpic++ -I$(MPI_HOME)/include -I$(BOOST_ROOT)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(HADOOP_HOME)/src/c++/libhdfs -O0 -g3 -w -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


