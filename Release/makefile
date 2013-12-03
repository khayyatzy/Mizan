################################################################################
# Automatically-generated file. Do not edit!
################################################################################

-include ../makefile.init

RM := rm -rf

# All of the sources participating in the build are defined here
-include sources.mk
-include src/utilities/subdir.mk
-include src/dataManager/graphWriters/subdir.mk
-include src/dataManager/graphReaders/subdir.mk
-include src/dataManager/dataStructures/graph/subdir.mk
-include src/dataManager/dataStructures/data/subdir.mk
-include src/dataManager/subdir.mk
-include src/computation/subdir.mk
-include src/subdir.mk
-include subdir.mk
-include objects.mk

ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C++_DEPS)),)
-include $(C++_DEPS)
endif
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
ifneq ($(strip $(CC_DEPS)),)
-include $(CC_DEPS)
endif
ifneq ($(strip $(CPP_DEPS)),)
-include $(CPP_DEPS)
endif
ifneq ($(strip $(CXX_DEPS)),)
-include $(CXX_DEPS)
endif
ifneq ($(strip $(C_UPPER_DEPS)),)
-include $(C_UPPER_DEPS)
endif
endif

-include ../makefile.defs

# Add inputs and outputs from these tool invocations to the build variables 

# All Target
all: Mizan-0.2b

# Tool invocations
Mizan-0.2b: $(OBJS) $(USER_OBJS)
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C++ Linker'
	mpic++ -L$(BOOST_ROOT)/lib -L$(HADOOP_HOME)/c++/Linux-amd64-64/lib -L$(JAVA_HOME)/lib -L$(JAVA_HOME)/jre -L$(JAVA_HOME)/jre/lib -L$(JAVA_HOME)/jre/lib/amd64 -L$(JAVA_HOME)/jre/lib/amd64/server -o "Mizan-0.2b" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)$(C++_DEPS)$(C_DEPS)$(CC_DEPS)$(CPP_DEPS)$(EXECUTABLES)$(CXX_DEPS)$(C_UPPER_DEPS) Mizan-0.2b
	-@echo ' '

.PHONY: all clean dependents
.SECONDARY:

-include ../makefile.targets
