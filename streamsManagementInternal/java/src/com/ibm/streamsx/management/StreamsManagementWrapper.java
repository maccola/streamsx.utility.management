/*******************************************************************************/
/* Copyright (C) 2016, International Business Machines Corporation             */
/* All Rights Reserved                                                         */
/*******************************************************************************/


package com.ibm.streamsx.management;

import com.ibm.streamsx.management.StreamsManagement;

import java.util.HashMap;
import java.util.Vector;

//
public class StreamsManagementWrapper {

	private static final int ACTION_DOMAININFO           = 0x000001;
	private static final int ACTION_INSTANCEINFO         = 0x000002;
	private static final int ACTION_SINGLEJOBINFO        = 0x000004;
	private static final int ACTION_ALLJOBINFO           = 0x000008;
	private static final int ACTION_SINGLEJOBINFOBYNAME  = 0x000010;
	private static final int ACTION_SUBMITJOB            = 0x000020;
	private static final int ACTION_CANCELJOB            = 0x000040;
	private static final int ACTION_ADDDOMAINHOST        = 0x000080;
	private static final int ACTION_REMOVEDOMAINHOST     = 0x000100;
	private static final int ACTION_GETDOMAINHOSTS       = 0x000200;
	private static final int ACTION_ADDTAGTOHOST         = 0x000400;
	private static final int ACTION_REMOVETAGFROMHOST    = 0x000800;
	private static final int ACTION_GETHOSTTAGS          = 0x001000;
	private static final int ACTION_MAKEINSTANCE         = 0x002000;
	private static final int ACTION_REMOVEINSTANCE       = 0x004000;	
	private static final int ACTION_STARTINSTANCE        = 0x008000;	
	private static final int ACTION_STOPINSTANCE         = 0x010000;
	private static final int ACTION_GETJOBLOGS           = 0x020000;
	private static final int ACTION_GETDOMAINLOGS        = 0x040000;
	
		
	
	//***********************************************
	// main entry point
	//***********************************************
	public static void main(String [] args) {
		
		if (args.length < 1) {
		      usage();
		      return;
			}
		
			// Main actions.
			String action = args[0];
			
			if (action.equals("getDomainInfo")) 
				getDomainInfo(args);	
			
			else if (action.equals("getInstanceInfo")) 
				getInstanceInfo(args);			
						
			else if (action.equals("getSingleJobInfo")) 
				getSingleJobInfo(args);
			
			else if (action.equals("getAllJobInfo")) 
				getAllJobInfo(args);
			
			else if (action.equals("getSingleJobInfoByName")) 
				getSingleJobInfoByName(args);
						
			else if (action.equals("submitJob")) 
				submitJob(args);
			
			else if (action.equals("cancelJob"))			
				cancelJob(args);	
			
			else if (action.equals("addDomainHost")) 
				addDomainHost(args);
			
			else if (action.equals("removeDomainHost")) 
				removeDomainHost(args);
			
			else if (action.equals("getDomainHosts"))				
				getDomainHosts(args);	
			
			else if (action.equals("addTagToHost"))
				addTagToHost(args);
			
			else if (action.equals("removeTagFromHost"))
				removeTagFromHost(args);
						
			else if (action.equals("getHostTags"))
				getHostTags(args);
			
			else if (action.equals("makeInstance"))
				makeInstance(args);
			
			else if (action.equals("removeInstance"))
				removeInstance(args);			
			
			else if (action.equals("startInstance"))
				startInstance(args);						
			
			else if (action.equals("stopInstance"))
				stopInstance(args);
						
			else if (action.equals("getJobLogs"))
				getJobLogs(args);		
			
			else if (action.equals("getDomainLogs"))
				getDomainLogs(args);	

			else {
				System.err.println("Invalid action:  " + action);
				usage();
				return;
			}		
			
			// Must exit due to fact that there is a background thread
			// kicked off by AuthenticatedUser object that will otherwise
			// not die.
		    System.exit(0);
	}
	

	//***********************************************
	// Get all domain info
	// Output printed to stdout to be collected
	// by perl wrapper.
	//***********************************************
	private static void getDomainInfo(String [] args) {
	  try {
        HashMap<String,Object> settings = getParms(args, ACTION_DOMAININFO);		
        if (null == settings)
          return;

        // connect
        StreamsManagement management = new StreamsManagement();
        connect(management, settings);

        String domainName = (String)(settings.get("DOMAIN"));
        String domainInfo = management.getDomainInfoJSONString(domainName);
        System.out.println(domainInfo);

        // disconnect
        disconnect(management);
      }  catch (Exception e) {
           printException(e);
         }  	  		
	}	

	//***********************************************
	// Get all instance Info
	// Output printed to stdout to be collected
	// by perl wrapper.
	//***********************************************
	private static void getInstanceInfo(String [] args) {
      try {
        HashMap<String,Object> settings = getParms(args, ACTION_INSTANCEINFO);		
          if (null == settings)
            return;
		
        // connect
        StreamsManagement management = new StreamsManagement();
        connect(management, settings);

        String domainName = (String)(settings.get("DOMAIN"));
        String instanceName = (String)(settings.get("INSTANCE"));
        String instanceInfo = management.getInstanceInfoJSONString(domainName, instanceName);
        System.out.println(instanceInfo);

        // Disconnect
        disconnect(management);
      }  catch (Exception e) {
         printException(e);
        }  	  				
		
	}
	

	//***********************************************
	// Get single job Info
	// Output printed to stdout to be collected
	// by perl wrapper.
	//***********************************************
	private static void getSingleJobInfo(String [] args) {
      try {
		HashMap<String,Object> settings = getParms(args, ACTION_SINGLEJOBINFO);		
		if (null == settings)
			return;
		
		// connect
		StreamsManagement management = new StreamsManagement();
		connect(management, settings);
		
		String domainName = (String)(settings.get("DOMAIN"));
		String instanceName = (String)(settings.get("INSTANCE")); 
		long jobId = Long.parseLong((String)(settings.get("JOB"))); 
		String jobInfo = management.getSingleJobJSONString(domainName, instanceName, jobId);
		System.out.println(jobInfo);
		
		// disconnect
		disconnect(management);
      }  catch (Exception e) {
    	  printException(e);
      }
	}
	

	//***********************************************
	// Get all job Info
	// Output printed to stdout to be collected
	// by perl wrapper.
	//***********************************************
	private static void getAllJobInfo(String [] args) {
      try {
		HashMap<String,Object> settings = getParms(args, ACTION_ALLJOBINFO);		
		if (null == settings)
			return;
		
		// connect
		StreamsManagement management = new StreamsManagement();
		connect(management, settings);
		
		String domainName = (String)(settings.get("DOMAIN"));
		String instanceName = (String)(settings.get("INSTANCE")); 
		String jobInfo = management.getAllJobJSONString(domainName, instanceName);
		System.out.println(jobInfo);
		
		// disconnect
		disconnect(management);
      }  catch (Exception e) {
    	  printException(e);
      }
	}
	

	//***********************************************
	// Get single job Info by job name
	// Output printed to stdout to be collected
	// by perl wrapper.
	//***********************************************
	private static void getSingleJobInfoByName(String [] args) {
      try {
		HashMap<String,Object> settings = getParms(args, ACTION_SINGLEJOBINFOBYNAME);		
		if (null == settings)
			return;
		
		// connect
		StreamsManagement management = new StreamsManagement();
		connect(management, settings);		
		
		String domainName = (String)(settings.get("DOMAIN"));
        String instanceName = (String)(settings.get("INSTANCE"));
        String jobName = (String)(settings.get("JOBNAME"));
        String jobInfo = management.getSingleJobByNameJSONString(domainName, instanceName, jobName);
        System.out.println(jobInfo);

		
		// disconnect
		disconnect(management);
      }  catch (Exception e) {
    	  printException(e);
      }
	}
	

	//***********************************************
	// Submit a job
	//***********************************************
	private static void submitJob(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_SUBMITJOB);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE")); 
	 		String bundle = (String)(settings.get("BUNDLE"));		
	 		@SuppressWarnings("unchecked") HashMap <String, String> jobParms = (HashMap<String, String>)(settings.get("JOBPARMS"));
	 		String jobGroup = (String)(settings.get("JOBGROUP"));
	 		String jobName = (String)(settings.get("JOBNAME")); 

	 		String jobInfo = management.submitJob(domainName, instanceName, bundle, jobParms, jobGroup, jobName);
	        System.out.println(jobInfo);	 		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
		

	//***********************************************
	// Cancel a job
	//***********************************************
	private static void cancelJob(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_CANCELJOB);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE")); 
			long jobId = Long.parseLong((String)(settings.get("JOB"))); 	 		
	 		Boolean force = (Boolean)(settings.get("FORCE"));
	 		if (null == force)
	 		  force = new Boolean(false);

            management.cancelJob(domainName, instanceName, jobId, force.booleanValue());		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	//***********************************************
	// Add a domain host
	//***********************************************
	private static void addDomainHost(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_ADDDOMAINHOST);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String hostName = (String)(settings.get("HOST"));

            management.addDomainHost(domainName, hostName);		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	//***********************************************
	// Remove a domain host
	//***********************************************
	private static void removeDomainHost(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_REMOVEDOMAINHOST);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String hostName = (String)(settings.get("HOST"));

            management.removeDomainHost(domainName, hostName);		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	
	//***********************************************
	// Get domain hosts
	//***********************************************
	private static void getDomainHosts(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_GETDOMAINHOSTS);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));

            String info = management.getDomainHostsJSONString(domainName);
            System.out.println(info);
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	
	//***********************************************
	// Add a tag to a host
	//***********************************************
	private static void addTagToHost(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_ADDTAGTOHOST);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String hostName = (String)(settings.get("HOST"));
	 		String tag = (String)(settings.get("TAG"));	 		

            management.addTagToHost(domainName, hostName, tag);		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	//***********************************************
	// Remove tag from a host
	//***********************************************
	private static void removeTagFromHost(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_REMOVETAGFROMHOST);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String hostName = (String)(settings.get("HOST"));
	 		String tag = (String)(settings.get("TAG"));	 		

            management.removeTagFromHost(domainName, hostName, tag);		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	//***********************************************
	// Get tags on a host
	//***********************************************
	private static void getHostTags(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_GETHOSTTAGS);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String hostName = (String)(settings.get("HOST"));	 		

            String info = management.getHostTagsJSONString(domainName, hostName);
            System.out.println(info);
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}	
	
	//***********************************************
	// Make instance
	//***********************************************
	private static void makeInstance(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_MAKEINSTANCE);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE"));
	 		String adminGroup = (String)(settings.get("ADMINGROUP"));
	 		String userGroup = (String)(settings.get("USERGROUP"));
	 		@SuppressWarnings("unchecked") Vector<String> properties = (Vector<String>)(settings.get("PROPERTIES"));            
	 		
	 		// Convert resourceCount, resourceTags, and resourceExclusive parameters into a 
	 		// Vector of ResourceSpec objects
	 		Vector<StreamsManagement.ResourceSpec> resourceSpecs = new Vector<StreamsManagement.ResourceSpec>();
	 		@SuppressWarnings("unchecked") Vector<String> counts = (Vector<String>)(settings.get("RESOURCECOUNTS"));
	 		@SuppressWarnings("unchecked") Vector<String> tags = (Vector<String>)(settings.get("RESOURCETAGS"));
	 		@SuppressWarnings("unchecked") Vector<String> exclusives = (Vector<String>)(settings.get("RESOURCEEXCLUSIVES"));
	 		int numResourceSpecs = 0;
	 		if ((null != counts) && (null != tags) && (null != exclusives)) {
	 			numResourceSpecs = counts.size();
	 			if (tags.size() < numResourceSpecs) numResourceSpecs = tags.size();
	 			if (exclusives.size() < numResourceSpecs) numResourceSpecs = exclusives.size();
	 		}
	 		for (int i = 0; i < numResourceSpecs; i++) {
	 			String tagsArray[] = tags.elementAt(i).split(",");
	 			Vector<String> tagsVector = new Vector<String>();
	 			for (int j=0; j < tagsArray.length; j++) {
	 				tagsVector.add(tagsArray[j]);
	 			}
	 			boolean tagExclusive = false;
	 			if (exclusives.elementAt(i).equals("true"))
	 				tagExclusive = true;
	 			int cnt = Integer.parseInt(counts.elementAt(i));
	 			StreamsManagement.ResourceSpec newSpec = new StreamsManagement.ResourceSpec(cnt, tagsVector, tagExclusive);
	 			resourceSpecs.addElement(newSpec);
	 		}
	 		
            management.makeInstance(domainName, instanceName, adminGroup, userGroup, properties, resourceSpecs);		 		
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}	
	
	//***********************************************
	// Remove an instance
	//***********************************************
	private static void removeInstance(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_REMOVEINSTANCE);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE"));	 		

            management.removeInstance(domainName, instanceName);
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
		
	//***********************************************
	// Start an instance
	//***********************************************
	private static void startInstance(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_STARTINSTANCE);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE"));	 		

            management.startInstance(domainName, instanceName);
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
	
	//***********************************************
	// Stop an instance
	//***********************************************
	private static void stopInstance(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_STOPINSTANCE);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE"));
	 		Boolean force = (Boolean)(settings.get("FORCE"));
	 		if (null == force)
	 		  force = new Boolean(false);
	 		
            management.stopInstance(domainName, instanceName, force.booleanValue());
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}		
	
	
	//***********************************************
	// Get job logs
	//***********************************************
	private static void getJobLogs(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_GETJOBLOGS);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String instanceName = (String)(settings.get("INSTANCE"));
			long jobId = Long.parseLong((String)(settings.get("JOB"))); 	
	 		String logFile = (String)(settings.get("LOGFILE"));
	 		
            management.getJobLogs(domainName, instanceName, jobId, logFile);
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}
		
	//***********************************************
	// Get domain logs
	//***********************************************
	private static void getDomainLogs(String [] args) {
	     try {
	 		HashMap<String,Object> settings = getParms(args, ACTION_GETDOMAINLOGS);		
	 		if (null == settings)
	 			return;
	 		
	 		// connect
	 		StreamsManagement management = new StreamsManagement();
	 		connect(management, settings);
	 		
	 		String domainName = (String)(settings.get("DOMAIN"));
	 		String logFile = (String)(settings.get("LOGFILE"));
	 		
            management.getDomainLogs(domainName, logFile);
	 		
	 		// disconnect
	 		disconnect(management);
	       }  catch (Exception e) {
	     	  printException(e);
	       }
	}			
	
	
	
	private static void connect(StreamsManagement management, HashMap<String,Object> settings) throws Exception {
		
		String zk = (String)(settings.get("ZK"));
		String domainName = (String)(settings.get("DOMAIN"));

		
		management.connectToJMX(zk, domainName);
	}
	
	private static void disconnect(StreamsManagement management) {
		management.disconnectFromJMX();
	}
	
	private static void printException(Exception e) {
		System.out.println("EXCEPTION:" + e.toString());
		//System.out.println("EXCEPTION:" + e.getMessage());
		//e.printStackTrace();
	}
	
	//***********************************************
	// Parse and verify passed in parameters.
	//***********************************************	  	  
	private static HashMap<String,Object> getParms(String [] args, int action) throws Exception {
		
		HashMap<String,Object> settings = parseParms(args, action);
		if (null == settings)
			return(null);
		
		if (false == verifyParms(settings, action))
			return(null);
		
		return(settings);
	
	}
		

	//***********************************************
	// Parse passed in parameters.
	//***********************************************	  	
	private static HashMap<String,Object> parseParms(String [] args, int action) throws Exception {
		
		HashMap<String,Object> settings = new HashMap<String,Object>();
		// skip the first arg as that is the action
		int i=0;
		for (i=1; i < args.length; i++) {		
			
			// -zkconnect <zkString>
			if (args[i].equals("-zkconnect")) {
				if (args.length == i + 1) {
					System.err.println("Missing -zkconnect value");
					usage();
					return(null);
				}
				else {
					settings.put("ZK", args[i+1]);
					i++;
				}
			}			
						
			// -domain <domain>
			else if (args[i].equals("-domain")) {
				if (args.length == i + 1) {
					System.err.println("Missing -domain value");
					usage();
					return(null);
				}
				else {
					settings.put("DOMAIN", args[i+1]);
					i++;
				}
			}			
			
			// -instance <instance>
			else if (args[i].equals("-instance")) {
				if (args.length == i + 1) {
					System.err.println("Missing -instance value");
					usage();
					return(null);
				}
				else {
					settings.put("INSTANCE", args[i+1]);
					i++;
				}
			}			
						
			// -job <jobID>
			else if (args[i].equals("-job")) {
				if (args.length == i + 1) {
					System.err.println("Missing -job value");
					usage();
					return(null);
				}
				else {
					settings.put("JOB", args[i+1]);
					i++;
				}
			}
		
			// -jobGroup <jobGroup>
			else if (args[i].equals("-jobGroup")) {
				if (args.length == i + 1) {
					System.err.println("Missing -jobGroup value");
					usage();
					return(null);
				}
				else {
					settings.put("JOBGROUP", args[i+1]);
					i++;
				}
			}
			
			// -jobName <jobName>
			else if (args[i].equals("-jobName")) {
				if (args.length == i + 1) {
					System.err.println("Missing -jobName value");
					usage();
					return(null);
				}
				else {
					settings.put("JOBNAME", args[i+1]);
					i++;
				}
			}
						
			// -bundle <sab file>
			else if (args[i].equals("-bundle")) {
				if (args.length == i + 1) {
					System.err.println("Missing -bundle value");
					usage();
					return(null);
				}
				else {
					settings.put("BUNDLE", args[i+1]);
					i++;
				}
			}
			
			
            // -jobParm <name=value>>
            else if (args[i].equals("-jobParm")) {
	          if (args.length == i + 1) {
		        System.err.println("Missing -jobParm value");
		        usage();
		        return(null);
	          }
	          else {	        	  
	        	String nextParm = args[i+1];
	        	int index = nextParm.indexOf('=');
	        	if (-1 == index)
	              throw new Exception("Incorrectly formed parameter:  " + nextParm);    	
	        	String name = nextParm.substring(0, index);
	        	String value = nextParm.substring(index+1);
	        	if ((0 == name.length()) || (0 == value.length()))
		          throw new Exception("Incorrectly formed parameter:  " + nextParm);   	        	
	        	if (!settings.containsKey("JOBPARMS")) {
	        	  HashMap<String, String> jobParms = new HashMap<String, String>();
	              settings.put("JOBPARMS", jobParms);
	        	}
	        	@SuppressWarnings("unchecked") HashMap<String, String> jobParms = (HashMap<String, String>)(settings.get("JOBPARMS"));
	        	jobParms.put(name, value);
		        i++;
	          }
            }
					
            // -force
            else if (args[i].equals("-force")) {
              Boolean force = new Boolean(true);
		      settings.put("FORCE", force);
            }
					
			// -host <hostName>
			else if (args[i].equals("-host")) {
				if (args.length == i + 1) {
					System.err.println("Missing -host value");
					usage();
					return(null);
				}
				else {
					settings.put("HOST", args[i+1]);
					i++;
				}
			}
			
			// -tag <tagName>
			else if (args[i].equals("-tag")) {
				if (args.length == i + 1) {
					System.err.println("Missing -tag value");
					usage();
					return(null);
				}
				else {
					settings.put("TAG", args[i+1]);
					i++;
				}
			}
					
			// -adminGroup <adminGroup>
			else if (args[i].equals("-adminGroup")) {
				if (args.length == i + 1) {
					System.err.println("Missing -adminGroup value");
					usage();
					return(null);
				}
				else {
					settings.put("ADMINGROUP", args[i+1]);
					i++;
				}
			}
			
			// -userGroup <userGroup>
			else if (args[i].equals("-userGroup")) {
				if (args.length == i + 1) {
					System.err.println("Missing -userGroup value");
					usage();
					return(null);
				}
				else {
					settings.put("USERGROUP", args[i+1]);
					i++;
				}
			}
						
			// -property <property>
			// -property can be specified multiple times.  Store in a Vector
			else if (args[i].equals("-property")) {
				if (args.length == i + 1) {
					System.err.println("Missing -property value");
					usage();
					return(null);
				}
				else {
					if (!settings.containsKey("PROPERTIES")) {
						settings.put("PROPERTIES", new Vector<String>());
					}
					@SuppressWarnings("unchecked") Vector <String>properties = (Vector<String>)(settings.get("PROPERTIES"));
					properties.add(args[i+1]);
					i++;
				}
			}
			
			// -resourceCount <resourceCount>
			// -resourceCount can be specified multiple times.  Store in a Vector
			else if (args[i].equals("-resourceCount")) {
				if (args.length == i + 1) {
					System.err.println("Missing -resourceCount value");
					usage();
					return(null);
				}
				else {
					if (!settings.containsKey("RESOURCECOUNTS")) {
						settings.put("RESOURCECOUNTS", new Vector<String>());
					}
					@SuppressWarnings("unchecked") Vector <String>resourceCounts = (Vector<String>)(settings.get("RESOURCECOUNTS"));
					resourceCounts.add(args[i+1]);
					i++;
				}
			}
						
			// -resourceTags <resourceTags>
			// -resourceTags can be specified multiple times.  Store in a Vector
			else if (args[i].equals("-resourceTags")) {
				if (args.length == i + 1) {
					System.err.println("Missing -resourceTags value");
					usage();
					return(null);
				}
				else {
					if (!settings.containsKey("RESOURCETAGS")) {
						settings.put("RESOURCETAGS", new Vector<String>());
					}
					@SuppressWarnings("unchecked") Vector <String>resourceTags = (Vector<String>)(settings.get("RESOURCETAGS"));
					resourceTags.add(args[i+1]);
					i++;
				}
			}
			
			// -resourceExclusive <resourceExclusive>
			// -resourceExclusive can be specified multiple times.  Store in a Vector
			else if (args[i].equals("-resourceExclusive")) {
				if (args.length == i + 1) {
					System.err.println("Missing -resourceExclusive value");
					usage();
					return(null);
				}
				else {
					if (!settings.containsKey("RESOURCEEXCLUSIVES")) {
						settings.put("RESOURCEEXCLUSIVES", new Vector<String>());
					}
					@SuppressWarnings("unchecked") Vector <String>resourceExclusives = (Vector<String>)(settings.get("RESOURCEEXCLUSIVES"));
					resourceExclusives.add(args[i+1]);
					i++;
				}
			}
						
			// -logFile <logFile>
			else if (args[i].equals("-logFile")) {
				if (args.length == i + 1) {
					System.err.println("Missing -logFile value");
					usage();
					return(null);
				}
				else {
					settings.put("LOGFILE", args[i+1]);
					i++;
				}
			}
			
			
		
			
			// else unknown
			else {
				System.err.println("Unknown option:  " + args[i]);
				usage();
				return(null);
			}		
		}  			
		
		return(settings);		
	}

	
	//***********************************************
	// Verify passed in parameters.
	//***********************************************	  		
	private static boolean verifyParms(HashMap<String,Object> settings, int action) {
		
		// URL
		if ((!settings.containsKey("ZK")) || (0 == ((String)(settings.get("ZK"))).length())) {
			System.err.println("Must specify a -zkconnect value");
			usage();
			return(false);
		}
				
		// DOMAIN
		if ((!settings.containsKey("DOMAIN")) || (0 == ((String)(settings.get("DOMAIN"))).length())) {
			System.err.println("Must specify a -domain value");
			usage();
			return(false);
		}		
		
		// INSTANCE needed for instanceInfo, singleJobInfo, allJobInfo, singleJobInfoByName, submitJob, canceljob, 
		// makeInstance, removeInstance, startInstance, stopInstance, getJobLogs
		if ((0 != (action & ACTION_INSTANCEINFO)) || 
			(0 != (action & ACTION_SINGLEJOBINFO)) ||
			(0 != (action & ACTION_ALLJOBINFO)) ||	
			(0 != (action & ACTION_SINGLEJOBINFOBYNAME)) ||	
			(0 != (action & ACTION_SUBMITJOB)) ||				
			(0 != (action & ACTION_CANCELJOB)) ||
			(0 != (action & ACTION_MAKEINSTANCE)) ||
			(0 != (action & ACTION_REMOVEINSTANCE)) ||
			(0 != (action & ACTION_STARTINSTANCE)) ||
			(0 != (action & ACTION_STOPINSTANCE)) ||
			(0 != (action & ACTION_GETJOBLOGS))) {	
			if ((!settings.containsKey("INSTANCE")) || (0 == ((String)(settings.get("INSTANCE"))).length())) {
				System.err.println("Must specify an -instance value");
				usage();
				return(false);
			}						
		}
		else {
			if (settings.containsKey("INSTANCE")) {
				System.err.println("The -instance parameter is not valid for this action.");
				usage();
				return(false);				
			}
		}					
		
		// JOB needed for singleJobInfo, cancelJob, getJobLog
		if ((0 != (action & ACTION_SINGLEJOBINFO)) ||
			(0 != (action & ACTION_CANCELJOB)) ||
		    (0 != (action & ACTION_GETJOBLOGS))) {
			if ((!settings.containsKey("JOB")) || (0 == ((String)(settings.get("JOB"))).length())) {
				System.err.println("Must specify a -job value");
				usage();
				return(false);
			}			
		}
		else {
			if (settings.containsKey("JOB")) {
				System.err.println("The -job parameter is not valid for this action.");
				usage();
				return(false);				
			}
		}
				
		// JOBGROUP is optional for submitJob
		if (0 != (action & ACTION_SUBMITJOB)) {
			if ((!settings.containsKey("JOBGROUP")) || (0 == ((String)(settings.get("JOBGROUP"))).length())) {
				settings.put("JOBGROUP", null);
			}			
		}						
		else {
			if (settings.containsKey("JOBGROUP")) {
				  System.err.println("The -jobGroup parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}		
		
		// JOBNAME only required for singleJobInfoByName.
		// JOBNAME is optional for submitJob
		if (0 != (action & ACTION_SINGLEJOBINFOBYNAME)) {
			if ((!settings.containsKey("JOBNAME")) || (0 == ((String)(settings.get("JOBNAME"))).length())) {
				System.err.println("Must specify a -jobName value");
				usage();
				return(false);
			}			
		}
		else if (0 != (action & ACTION_SUBMITJOB)) {
			if ((!settings.containsKey("JOBNAME")) || (0 == ((String)(settings.get("JOBNAME"))).length())) {
				settings.put("JOBNAME", null);
			}			
		}						
		else {
			if (settings.containsKey("JOBNAME")) {
				  System.err.println("The -jobName parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}		
		
		// BUNDLE only needed for submitJob
		if (0 != (action & ACTION_SUBMITJOB)) {
			if ((!settings.containsKey("BUNDLE")) || (0 == ((String)(settings.get("BUNDLE"))).length())) {
				System.err.println("Must specify a -bundle value");
				usage();
				return(false);
			}			
		}
		else {
			if (settings.containsKey("BUNDLE")) {
				System.err.println("The -bundle parameter is not valid for this action.");
				usage();
				return(false);				
			}
		}		
		
		// JOBPARMS only needed for submitJob, but is not required.
		if (0 == (action & ACTION_SUBMITJOB)) {
			if (settings.containsKey("JOBPARMS")) {
			  System.err.println("The -jobParm parameter is not valid for this action.");
			  usage();
			  return(false);				
			}
		}
		
		// FORCE needed by cancelJob, stopInstance, but it not required. 
		if ((0 == (action & ACTION_CANCELJOB)) &&
			(0 == (action & ACTION_STOPINSTANCE))) {
			if (settings.containsKey("FORCE")) {
			  System.err.println("The -force parameter is not valid for this action.");
			  usage();
			  return(false);				
			}
		}
				
		// HOST only needed for addDomainHost, removeDomainHost, addTagToHost, removeTagFromHost, getHostTags
		if ((0 != (action & ACTION_ADDDOMAINHOST)) ||
		    (0 != (action & ACTION_REMOVEDOMAINHOST)) ||
		    (0 != (action & ACTION_ADDTAGTOHOST)) ||
		    (0 != (action & ACTION_REMOVETAGFROMHOST)) ||
		    (0 != (action & ACTION_GETHOSTTAGS))) {				
			if ((!settings.containsKey("HOST")) || (0 == ((String)(settings.get("HOST"))).length())) {
				System.err.println("Must specify a -host value");
				usage();
				return(false);
			}			
		}
		else {
			if (settings.containsKey("HOST")) {
				System.err.println("The -host parameter is not valid for this action.");
				usage();
				return(false);				
			}
		}
		
		// TAG only needed for addTagToHost, removeTagFromHost
		if ((0 != (action & ACTION_ADDTAGTOHOST)) ||
		    (0 != (action & ACTION_REMOVETAGFROMHOST))) {
			if ((!settings.containsKey("TAG")) || (0 == ((String)(settings.get("TAG"))).length())) {
				System.err.println("Must specify a -tag value");
				usage();
				return(false);
			}			
		}
		else {
			if (settings.containsKey("TAG")) {
				System.err.println("The -tag parameter is not valid for this action.");
				usage();
				return(false);				
			}
		}
				
		// ADMINGROUP is optional for makeInstance
		if (0 != (action & ACTION_MAKEINSTANCE)) {
			if ((!settings.containsKey("ADMINGROUP")) || (0 == ((String)(settings.get("ADMINGROUP"))).length())) {
				settings.put("ADMINGROUP", null);
			}			
		}						
		else {
			if (settings.containsKey("ADMINGROUP")) {
				  System.err.println("The -adminGroup parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}
		
		// USERGROUP is optional for makeInstance
		if (0 != (action & ACTION_MAKEINSTANCE)) {
			if ((!settings.containsKey("USERGROUP")) || (0 == ((String)(settings.get("USERGROUP"))).length())) {
				settings.put("USERGROUP", null);
			}			
		}						
		else {
			if (settings.containsKey("USERGROUP")) {
				  System.err.println("The -userGroup parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}
				
		// PROPERTIES is optional for makeInstance
		if (0 != (action & ACTION_MAKEINSTANCE)) {
			if ((!settings.containsKey("PROPERTIES")) || (0 == ((Vector)(settings.get("PROPERTIES"))).size())) {
				settings.put("PROPERTIES", null);
			}			
		}						
		else {
			if (settings.containsKey("PROPERTIES")) {
				  System.err.println("The -property parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}
		
		// RESOURCECOUNTS is optional for makeInstance
		if (0 != (action & ACTION_MAKEINSTANCE)) {
			if ((!settings.containsKey("RESOURCECOUNTS")) || (0 == ((Vector)(settings.get("RESOURCECOUNTS"))).size())) {
				settings.put("RESOURCECOUNTS", null);
			}			
		}						
		else {
			if (settings.containsKey("RESOURCECOUNTS")) {
				  System.err.println("The -resourceCount parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}
		
		// RESOURCETAGS is optional for makeInstance
		if (0 != (action & ACTION_MAKEINSTANCE)) {
			if ((!settings.containsKey("RESOURCETAGS")) || (0 == ((Vector)(settings.get("RESOURCETAGS"))).size())) {
				settings.put("RESOURCETAGS", null);
			}			
		}						
		else {
			if (settings.containsKey("RESOURCETAGS")) {
				  System.err.println("The -resourceTag parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}
		
		// RESOURCEEXCLUSIVES is optional for makeInstance
		if (0 != (action & ACTION_MAKEINSTANCE)) {
			if ((!settings.containsKey("RESOURCEEXCLUSIVES")) || (0 == ((Vector)(settings.get("RESOURCEEXCLUSIVES"))).size())) {
				settings.put("RESOURCEEXCLUSIVES", null);
			}			
		}						
		else {
			if (settings.containsKey("RESOURCEEXCLUSIVES")) {
				  System.err.println("The -resourceExclusive parameter is not valid for this action.");
				  usage();
				  return(false);
			}
		}
		
		
		// LOGFILE needed for getJobLogs, getDomainLogs
		if ((0 != (action & ACTION_GETJOBLOGS)) ||
			(0 != (action & ACTION_GETDOMAINLOGS))) {
			if ((!settings.containsKey("LOGFILE")) || (0 == ((String)(settings.get("LOGFILE"))).length())) {
				System.err.println("Must specify a -logFile value");
				usage();
				return(false);
			}			
		}
		else {
			if (settings.containsKey("LOGFILE")) {
				System.err.println("The -logFile parameter is not valid for this action.");
				usage();
				return(false);				
			}
		}
		
		return(true);
	}	

	//***********************************************
	// Display usage
	//***********************************************	  		
	private static void usage() {
		System.err.println("java StreamsManagementWrapper getDomainInfo -zkconnect <zkString> -domain <domainName>");			
        System.err.println("   or");		
		System.err.println("java StreamsManagementWrapper getInstanceInfo -zkconnect <zkString> -domain <domainName> -instance <instance>");			
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper getSingleJobInfo zkconnect <zkString> -domain <domainName> -instance <instance> -job <jobID>");		        
   		System.err.println("");
        System.err.println("   or");   		
		System.err.println("java StreamsManagementWrapper getAllJobInfo -zkconnect <zkString> -domain <domainName> -instance <instance>");		        
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper getSingleJobInfoByName -zkconnect <zkString> -domain <domainName> -instance <instance> -jobName <jobName>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper submitJob -zkconnect <zkString> -domain <domainName> -instance <instance>  -bundle <sab file> -jobName <jobName> -jobParm <var1=value1> -jobParm = <var2=value2>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper cancelJob -zkconnect <zkString> -domain <domainName> -instance <instance>  -job <jobID> -force");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper addDomainHost -zkconnect <zkString> -domain <domainName> -host <hostName>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper removeDomainHost -zkconnect <zkString> -domain <domainName> -host <hostName>");						
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper getDomainHosts -zkconnect <zkString> -domain <domainName>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper addTagToHost -zkconnect <zkString> -domain <domainName> -host <hostName> -tag <tagName>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper removeTagFromHost -zkconnect <zkString> -domain <domainName> -host <hostName> -tag <tagName>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper getHostTags -zkconnect <zkString> -domain <domainName> -host <hostName>");												
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper makeInstance -zkconnect <zkString> -domain <domainName> -instance <instanceName> -adminGroup <adminGroup> -userGroup <userGroup> [-property <name=value>] [[-resourceCount <resourceCount> -resourceTags <resourceTags> -resourceExclusive <true|false>...");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper removeInstance -zkconnect <zkString> -domain <domainName> -instance <instanceName>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper startInstance -zkconnect <zkString> -domain <domainName> -instance <instanceName>");		
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper stopInstance -zkconnect <zkString> -domain <domainName> -instance <instanceName> -force");	        
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper getJobLogs -zkconnect <zkString> -domain <domainName> -instance <instanceName> -job <jobID> -logFile <logFile>");
        System.err.println("   or");
		System.err.println("java StreamsManagementWrapper getDomainLogs -zkconnect <zkString> -domain <domainName> -logFile <logFile>");	
   		System.err.println("");		
		System.err.println("where:");
		System.out.println("-zkconnect <zkStringL> : zookeeper connect string.  Often stored in the $STREAMS_ZKCONNECT environment variable.");
		System.err.println("-domain <domainName> : the Streams domain name.");		
		System.err.println("-instance <instance> : streams instance ID.");
		System.err.println("-job <jobID> : Streams job ID.");
		System.err.println("-jobName <jobName> : Streams job name.  When submitting a job, this value can be \"*DEFAULT\"");
		System.err.println("-bundle <sab file>:  application bundle file used when submitting a job.");
		System.err.println("-jobParm <var1=value1:  job submit value.  Format is \"varName=varValue\".  This parameter can be specified multiple times.");
		System.err.println("-force:  Force the cancel.");
		System.err.println("-host <hostName>:  host name");
		System.err.println("-tag <tagName>:  tag name");
		System.err.println("-adminGroup <adminGroup> : admin group");
		System.err.println("-userGroup <userGroup> : user group");
		System.err.println("-property <name=value>:  String containing propertyName and propertyValue.  Must have form \"name=value\".  Can be specified multiple times");
		System.err.println("-resourceCount <resourceCount> : Count for a resource specification.  Used in conjunction with -resourceTags and -resourceExclusive parameters.  Can be specified multiple times, but order specified is relavant to correlate with -resourceTags and -resourceExclusive parameters.");
		System.err.println("-resourceTags <resourceTags> : Comma separated list of tags for a resource specification.  Used in conjunction with -resourceCount and -resourceExclusive parameters.  Can be specified multiple times, but order specified is relavant to correlate with -resourceCount and -resourceExclusive parameters.");
		System.err.println("-resourceExclusive <true|false> : True or false to specify an exclusvie resource specification.  Used in conjunction with -resourceCount and -resourceTags parameters.  Can be specified multiple times, but order specified is relavant to correlate with -resourceCount and -resourceTags parameters.");
		System.err.println("-logFile <logFile>:  log file");

	}	
	
}	