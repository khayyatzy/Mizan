What is Mizan?

Mizan is an advanced clone to Google’s graph processing system Pregel that utilizes online graph vertex migrations to dynamically optimizes the execution of graph algorithms. You can use our Mizan system to develop any vertex centric graph algorithm and run in parallel over a local cluster or over cloud infrastructure. Mizan is compatible with Pregel’s API, written in C++ and uses MPICH2 for communication. You can download a copy of Mizan and start using it today on your local machine or try Mizan on EC2. We also welcome programers who are interested to go deeper into our Mizan code to optimize or tweak.

User’s API

Implementing interface “IsuperStep.h” enables Mizan to run the user’s vertex centric graph algorithm. Function initialize() is called before starting the first superstep, which can be used to initialize vertices and/or edges values, which is called for each vertex pointed by the class pointer “userVertexObject<K,V1,M,A> *”. Function compute() is called once for each vertex at each superstep; “messageIterator *” pointer is a pointer to the message iterator for the vertex pointed by “userVertexObject<K,V1,M,A> *” while the class pointer “messageManager<K,V1,M,A> *” is used to send messages to other vertices. All of the user API depends on template classes to dynamically defines the vertex ID class <K>, vertex value class <V1>, message value class <M> and aggregation class <A>. The following is some of the predefined Mizan specific data types that can be used with the template classes:

    Basic data types:
        mDouble
        mInt
        mLong
    Array data types
        mCharArray
        mCharArrayNoCpy
        mDoubleArray
        mLongArray

Mizan also contains complex data types while the user can define his own data types. Please refer to the source code for more information.

Installation:

You have to make sure that you installed all of the following software before compiling Mizan:

    make and c++ compiler (build-essentials)
    Boost C++ library
    C++ threadpool library V 0.2.5
    MPICH2 3.0.2
    Java Development Kit (JDK) SE 1.6
    Hadoop MapReduce 1.0.4
    Metis/ParMetis (optional)


Please visit our blog http://thegraphsblog.wordpress.com/mizan-on-ubuntu/ for a step-by-step tutorial

Amazon EC2:

If you want to try Mizan on EC2, you can use our per-installed copy of Mizan on EC2′s through Amazon Machine Image (AMI). We provide AMI for each region, which are listed below:

Region 	 			AMI ID
US East (N. Virginia) 		ami-52ed743b
US West (Oregon) 		ami-8c2cb9bc
US West (N. California) 	ami-a03815e5
EU (Ireland) 			ami-08444e7c
Asia Pacific (Singapore)	ami-ec175bbe
Asia Pacific (Tokyo) 		ami-dbc646da
Asia Pacific (Sydney) 		ami-b26ffe88
South America (São Paulo) 	ami-fb5b80e6

Please visit our blog http://thegraphsblog.wordpress.com/running-mizan-on-ec2/ for more information
