# streamsx.utility.management

Perl library containing routines for administering and monitoring IBM InfoSphere Streams environments.

Prereqs:

To use these routines you must be running Streams 4.1.1.1 or later.

The perl JSON parser must be installed.  For example, it might be loaded with a perl-JSON rpm.

There is no seperate documentation for these routines.  There is a description of each routine in the comments of streamsManagement.pm.  There are also several sample perl scripts in the examples directory.  These samples can be used as-is or modified to one's own needs.

Under the covers the perl routines call java code.  The java code needs to be compiled prior to using these routines for the first time.  To build the java code cd to the streamsManagementInternal directory and run the build.sh script.

Most of the perl routines call a java program that makes a JMX connection under the covers.  The exception to this is routines dealing with creating/starting/stopping/removing domains since the domain has to be started in order for a JMX connection to be made.  In those cases, the streamtool command is called under the covers.

The JMX connection is made with a generated public/private key pair that allows user to connect without specifying a userid or password.  The key pair can be generated with either the streamtool genkey command or the genKey perl routine included with this project.

It is important to understand that the perl APIs maintain a cache of status information.  This cache persists through the life of the calling perl process.  As a result, if you have a task that requires 10 different API calls, it will be much quicker to have one perl program that calls each of the 10 APIs as opposed to having 10 small perl programs each calling a single API.  Having this cache greatly reduces the number of times we have to connect to the JMX server (as well as reduce the number of JVMs we are creating).

Users can control when this cache is cleared or reloaded.  If I have a script that retrieve metrics A, B, and C on 30 second intervals, the script will need to refresh the cache on that 30 second interval.  The logic would look something like this:

Loop

  Clear the cache
  
  Retrieve metric A
  
  Retrieve metric B
  
  Retrieve metric C
  
  Sleep 30 seconds
  
End loop
