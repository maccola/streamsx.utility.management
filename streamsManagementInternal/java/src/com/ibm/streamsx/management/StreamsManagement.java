/*******************************************************************************/
/* Copyright (C) 2016, International Business Machines Corporation             */
/* All Rights Reserved                                                         */
/*******************************************************************************/


package com.ibm.streamsx.management;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.File;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.omg.CORBA.portable.InputStream;
import org.omg.CORBA_2_3.portable.OutputStream;

import com.ibm.streams.management.ObjectNameBuilder; 
import com.ibm.streams.management.domain.DomainMXBean;
import com.ibm.streams.management.instance.InstanceMXBean;
import com.ibm.streams.management.job.JobMXBean;
import com.ibm.streams.management.resource.StreamsHostResourceManagerMXBean;
import com.ibm.streams.management.resource.ResourceMXBean;
import com.ibm.streams.management.resource.ResourceSpecification;
import com.ibm.streams.management.job.DeployInformation;

import com.ibm.streams.management.persistence.ITreePersistence;
import com.ibm.streams.management.persistence.Persistence;
import com.ibm.streams.admin.internal.api.StreamsInstall;
import com.ibm.streams.admin.internal.api.StreamsDomain;
import com.ibm.streams.admin.internal.api.AuthenticatedUser;


import com.ibm.json.java.*; 

//
public class StreamsManagement {
	
		
	
	//***********************************************
	// constructor
	//***********************************************
	public StreamsManagement() {
		
	}

	//***********************************************
	// Connect to the JMX server 
	// using userid/password
	//***********************************************	
	public void connectToJMX(String url, String user, String password) throws Exception {
		if (isConnected())
	      return;
        HashMap<String,Object> env = new HashMap<String,Object>();
        String [] creds = {user, password};
        env.put("jmx.remote.credentials", creds);
        env.put("jmx.remote.protocol.provider.pkgs", "com.ibm.streams.management");       
        JMXConnector jmxc = JMXConnectorFactory.connect(new JMXServiceURL(url), env);
        setMsbc(jmxc.getMBeanServerConnection());		
	}
	

	//***********************************************
	// Connect to the JMX server 
	// by retrieving AAS token.
	// Note that calling this connect() method
	// causes a background thread to be kicked off
	// to keep a session alive.  The calling
	// code will need to execute a System.exit()
	// call to terminate that thread properly.
	//***********************************************	
	public void connectToJMX(String zkString, String domainName) throws Exception {
		
		if (isConnected())
		      return;	
	    org.apache.log4j.BasicConfigurator.configure(new org.apache.log4j.varia.NullAppender());
	    final JMXConnector jmxc = com.ibm.streams.management.internal.utils.JmxUtils.getJmxConnectorFromKey(domainName, zkString, null);

	    setMsbc(jmxc.getMBeanServerConnection());			
	}
	

	//***********************************************
	// Disconnect from the JMX Server
	//***********************************************	
	public void disconnectFromJMX() {
		setMsbc(null);		
	}
	
	
	//***********************************************
	// Get domain info in JSON format
	//***********************************************	
	public String getDomainInfoJSONString(String domainName) throws Exception {
		checkConnected();
		
		JSONObject info = new JSONObject();
		
		JSONObject domainInfo = getDomainInfoJSON(domainName);
		info.put("domainInfo", domainInfo);
		return(info.serialize());		
	}		
	
	//***********************************************
	// Get instance info in JSON format
	// domain info will also be retrieved
	//***********************************************	
	public String getInstanceInfoJSONString(String domainName, String instanceName) throws Exception {
		checkConnected();
		
		JSONObject info = new JSONObject();
		
		JSONObject domainInfo = getDomainInfoJSON(domainName);	
		info.put("domainInfo", domainInfo);

		JSONObject instanceInfo = getInstanceInfoJSON(domainName, instanceName);
		info.put("instanceInfo", instanceInfo);
		
		return(info.serialize());		
	}
	
	
	//***********************************************
	// Get single job info in JSON format
	// domain info and instance info will also be retrieved
	//***********************************************	
	public String getSingleJobJSONString(String domainName, String instanceName, long jobId) throws Exception {
		checkConnected();				

		JSONObject info = new JSONObject();
		
		JSONObject domainInfo = getDomainInfoJSON(domainName);	
		info.put("domainInfo", domainInfo);

		JSONObject instanceInfo = getInstanceInfoJSON(domainName, instanceName);
		info.put("instanceInfo", instanceInfo);	

		JSONObject jobInfo = getSingleJobInfoJSON(domainName, instanceName, jobId);
		info.put("jobInfo", jobInfo);
		
		return(info.serialize());		
	}	
	
	//***********************************************
	// Get all job info in JSON format
	// domain info and instance info will also be retrieved
	//***********************************************	
	public String getAllJobJSONString(String domainName, String instanceName) throws Exception {
		checkConnected();				

		JSONObject info = new JSONObject();
		
		JSONObject domainInfo = getDomainInfoJSON(domainName);	
		info.put("domainInfo", domainInfo);

		JSONObject instanceInfo = getInstanceInfoJSON(domainName, instanceName);
		info.put("instanceInfo", instanceInfo);	

		JSONObject jobInfo = getAllJobInfoJSON(domainName, instanceName);
		info.put("jobInfo", jobInfo);

		
		return(info.serialize());		
	}
	
	
	//***********************************************
	// Special case which will find a job ID from a job name.
	// Will then return job info for that job.
	// Domain info and instance info will also be retrieved
	//***********************************************	
	public String getSingleJobByNameJSONString(String domainName, String instanceName, String jobName) throws Exception {
		checkConnected();				

		JSONObject info = new JSONObject();
		
		JSONObject domainInfo = getDomainInfoJSON(domainName);	
		info.put("domainInfo", domainInfo);

		JSONObject instanceInfo = getInstanceInfoJSON(domainName, instanceName);
		info.put("instanceInfo", instanceInfo);	

		JSONObject jobInfo = getSingleJobInfoByNameJSON(domainName, instanceName, jobName);
		info.put("jobInfo", jobInfo);
		
		return(info.serialize());		
	}
			
	
	//***********************************************
	// Submits an application
	//***********************************************	
	public String submitJob(String domainName, String instanceName, String bundleName, HashMap<String, String> jobParms, String jobGroup, String jobName) throws Exception {
		checkConnected();				
		
		// First, deploy the application
		File bundle = new File(bundleName);
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		DeployInformation deployInfo = instance.deployApplication(bundle.getName());
		
		// Push the .sab up to the server
		setDefaultHostnameVerifier();
		URL url = new URL(deployInfo.getUri());
		pushFileToServer(url, bundle);
		
		// Submit the application
		long jobId = instance.submitJob(deployInfo.getApplicationId(), 
				                        jobParms,
				                        null,
				                        false,
				                        jobGroup,
				                        jobName,
				                        null).longValue();
		
		
		JSONObject info = new JSONObject();
		info.put("jobId", jobId);
		return(info.serialize());		
	}
	
	
	//***********************************************
	// cancels a job
	//***********************************************	
	public void cancelJob(String domainName, String instanceName, long jobId, boolean force) throws Exception {
		checkConnected();				
		
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		instance.cancelJob(java.math.BigInteger.valueOf(jobId), force);
					
		return;		
	}	
	
	//***********************************************
	// add a host to a domain
	//***********************************************	
	public void addDomainHost(String domainName, String hostName) throws Exception {
		checkConnected();				
		
		StreamsHostResourceManagerMXBean resourceManager = getHostResourceManagerBean(domainName);
		resourceManager.addDomainHost(hostName, null);
					
		return;		
	}
	
	//***********************************************
	// remove a host from a domain
	//***********************************************	
	public void removeDomainHost(String domainName, String hostName) throws Exception {
		checkConnected();				
		
		StreamsHostResourceManagerMXBean resourceManager = getHostResourceManagerBean(domainName);
		resourceManager.removeDomainHost(hostName, null);
					
		return;		
	}
	
	//***********************************************
	// get hosts in a domain
	//***********************************************	
	public String getDomainHostsJSONString(String domainName) throws Exception {
		checkConnected();				
		
		StreamsHostResourceManagerMXBean resourceManager = getHostResourceManagerBean(domainName);
		//Set<String> hosts = resourceManager.getDomainHosts();
		
		// list of hosts
		JSONArray ja = new JSONArray();
        ja.addAll(resourceManager.getDomainHosts());
        return(ja.serialize());
	}
	
	//***********************************************
	// add a tag to a host
	//***********************************************	
	public void addTagToHost(String domainName, String hostName, String tag) throws Exception {
		checkConnected();				
		
		StreamsHostResourceManagerMXBean resourceManager = getHostResourceManagerBean(domainName);
		resourceManager.addTag(hostName, tag);
					
		return;		
	}
		
	//***********************************************
	// remove a tag from a host
	//***********************************************	
	public void removeTagFromHost(String domainName, String hostName, String tag) throws Exception {
		checkConnected();				
		
		StreamsHostResourceManagerMXBean resourceManager = getHostResourceManagerBean(domainName);
		resourceManager.removeTag(hostName, tag, null);
					
		return;		
	}
	
	//***********************************************
	// get tags on a host
	//***********************************************	
	public String getHostTagsJSONString(String domainName, String hostName) throws Exception {
		checkConnected();				
		
		ResourceMXBean resource = getResourceBean(domainName, hostName);
		//Set<String> tags = resource.getTags();
		
		// list of tags
		JSONArray ja = new JSONArray();
        ja.addAll(resource.getTags());
        return(ja.serialize());
	}
	
	//***********************************************
	// create an instance
	//***********************************************
	public static class ResourceSpec {
		public int count;
		public Vector<String> tags;
		public boolean exclusive;
		ResourceSpec(int count, Vector<String> tags, boolean exclusive) {
			this.count = count;
			this.tags = tags;
			this.exclusive = exclusive;
		}
	}
	public void makeInstance(String domainName, String instanceName, String adminGroup,
			                 String userGroup, Vector<String> properties, Vector<ResourceSpec> resources) throws Exception {
		checkConnected();				
		
		DomainMXBean domain = getDomainBean(domainName);
		
		// Convert properties to format suitable for JMX call
		HashMap<InstanceMXBean.PropertyId, String> preppedProperties = null;
		if (null != properties) {
			preppedProperties = new HashMap<InstanceMXBean.PropertyId, String>();
			Iterator<String> i1 = properties.iterator();
			while (i1.hasNext()) {
				String nextProp = i1.next();
				// nextProp should have the format "name=value"
				String[] propParts = nextProp.split("=");
				if (2 != propParts.length)
					throw new Exception("Format of property name=value not valid:  " + nextProp);
				preppedProperties.put(InstanceMXBean.PropertyId.fromString(propParts[0]), propParts[1]);
			}
		}
		
		// Convert resources to format suitable for JMX call
		Vector<ResourceSpecification> preppedResources = null;
		if (null != resources) {
			preppedResources = new Vector<ResourceSpecification>();
			Iterator<ResourceSpec> i1 = resources.iterator();
			while (i1.hasNext()) {
				ResourceSpec nextResource = i1.next();
				Iterator<String> i2 = nextResource.tags.iterator();
				HashSet <String> preppedTags = new HashSet<String>();
				while (i2.hasNext()) {
					preppedTags.add(i2.next());
				}				
				ResourceSpecification preppedResource = new ResourceSpecification(nextResource.count, preppedTags, nextResource.exclusive);
				preppedResources.add(preppedResource);
			}
		}
		
		// Create the instance
		domain.makeInstance(instanceName, adminGroup, userGroup, preppedProperties, preppedResources, null);
	}
	
	//***********************************************
	// remove an instance
	//***********************************************	
	public void removeInstance(String domainName, String instanceName) throws Exception {
		checkConnected();				
		
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		instance.remove(null);
					
		return;		
	}
		
	//***********************************************
	// start an instance
	//***********************************************	
	public void startInstance(String domainName, String instanceName) throws Exception {
		checkConnected();				
		
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		instance.start(null);
					
		return;		
	}
	
	//***********************************************
	// stop an instance
	//***********************************************	
	public void stopInstance(String domainName, String instanceName, boolean force) throws Exception {
		checkConnected();				
		
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		instance.stop(force, null);
					
		return;		
	}
	
	
	//***********************************************
	// get application/trace files from a job
	//***********************************************	
	public void getJobLogs(String domainName, String instanceName, long jobId, String logFile) throws Exception {
		checkConnected();				
		
		JobMXBean job = getJobBean(domainName, instanceName, jobId);
		
		URL url = new URL(job.retrieveApplicationLogAndTraceFiles(null));
		
		setDefaultHostnameVerifier();
		readFromURLIntoFile(url, logFile);
		
		return;		
	}
	
	
	//***********************************************
	// get product and trace logs for a domain
	//***********************************************	
	public void getDomainLogs(String domainName, String logFile) throws Exception {
		checkConnected();				
		
		DomainMXBean domain = getDomainBean(domainName);
		
		URL url = new URL(domain.retrieveProductLogAndTraceFiles(null));
		
		setDefaultHostnameVerifier();
		readFromURLIntoFile(url, logFile);
		
		return;		
	}
	
	

	
	
	
	


	//***********************************************
	// Get job status snapshot info JSON format
	//***********************************************	
	public String getJobStatusInfoJSONString(String domainName, String instanceName, long jobId) throws Exception {
		
		JobMXBean job = getJobBean(domainName, instanceName, jobId);
		setDefaultHostnameVerifier();
		return(getJobStatusInfoJSONString(job));
	}
		
	private String getJobStatusInfoJSONString(JobMXBean job) throws Exception {		
	    URL url = new URL(job.snapshot(-1, true) );
	    String jsonString = readFromURL(url);
		return(jsonString);
	}
	

	//***********************************************
	// Get job metrics snapshot info JSON format
	//***********************************************	
	public String getJobMetricInfoJSONString(String domainName, String instanceName, long jobId) throws Exception {
		
		JobMXBean job = getJobBean(domainName, instanceName, jobId);
		setDefaultHostnameVerifier();
		return(getJobMetricInfoJSONString(job));
	}
	private String getJobMetricInfoJSONString(JobMXBean job) throws Exception {		
	    URL url = new URL(job.snapshotMetrics());
	    String jsonString = readFromURL(url);
		return(jsonString);
	}		
	
	
	//***********************************************
	// Get domain info into a JSON object
	//***********************************************
	private JSONObject getDomainInfoJSON(String domainName) {
		DomainMXBean domain = getDomainBean(domainName);
		JSONObject domainInfo = new JSONObject();
		
		// status
		domainInfo.put("status", domain.getStatus().toString());
		
		// list of instances
		JSONArray ja = new JSONArray();
        ja.addAll(domain.getInstances());
		domainInfo.put("instances", ja);
		
		return(domainInfo);
	}

	//***********************************************
	// Get instance info into a JSON object
	//***********************************************
	private JSONObject getInstanceInfoJSON(String domainName, String instanceName) {
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		JSONObject instanceInfo = new JSONObject();
		
		// status
		instanceInfo.put("status", instance.getStatus().toString());
		
		// list of jobs
		JSONArray ja = new JSONArray();
        ja.addAll(instance.getJobs());
		instanceInfo.put("jobs", ja);
		
		return(instanceInfo);
	}
	
	//***********************************************
	// Get single job info into a JSON object
	//***********************************************
	private JSONObject getSingleJobInfoJSON(String domainName, String instanceName, long jobId) throws Exception {

		JSONObject jobInfo = new JSONObject();
		JSONObject singleJobJson = getJobInfoJSON(domainName, instanceName, jobId);		
				
		// this will result in info for only 1 job being added
		jobInfo.put(Long.toString(jobId), singleJobJson);
		
		return(jobInfo);
	}
	
	//***********************************************
	// Get all job info into a JSON object
	//***********************************************
	private JSONObject getAllJobInfoJSON(String domainName, String instanceName) throws Exception {
		
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		Set<BigInteger> jobIds = instance.getJobs();

		JSONObject jobInfo = new JSONObject();
		Iterator<BigInteger> it = jobIds.iterator();
		while (it.hasNext()) {
			long jobId = it.next().longValue();
			JSONObject singleJobJson = getJobInfoJSON(domainName, instanceName, jobId);	
			jobInfo.put(Long.toString(jobId), singleJobJson);	
		}
		
		return(jobInfo);
	}	
	
	//***********************************************
	// Get single job info into a JSON object
	// from job name
	//***********************************************
	private JSONObject getSingleJobInfoByNameJSON(String domainName, String instanceName, String jobName) throws Exception {
		
		InstanceMXBean instance = getInstanceBean(domainName, instanceName);
		Set<BigInteger> jobIds = instance.getJobs();

		JSONObject jobInfo = new JSONObject();
		Iterator<BigInteger> it = jobIds.iterator();
		while (it.hasNext()) {
			long nextId = it.next().longValue();
			JobMXBean jobBean = getJobBean(domainName, instanceName, nextId);
			if (jobBean.getName().equals(jobName)) {
			  long jobId = nextId;
			  JSONObject singleJobJson = getJobInfoJSON(domainName, instanceName, jobId);
			  jobInfo.put(Long.toString(jobId), singleJobJson);
			  return(jobInfo);
				
			}
		}
		
		throw new Exception("Could not find job named " + jobName); 
	
	}	
	
	//***********************************************
	// Build JSON object for individual job
	//***********************************************
	private JSONObject getJobInfoJSON(String domainName, String instanceName, long jobId) throws Exception {

		JSONObject jobInfo = new JSONObject();
		
		JobMXBean job = getJobBean(domainName, instanceName, jobId);
		setDefaultHostnameVerifier();
		
		String jobStatusJsonString = getJobStatusInfoJSONString(job);
		jobInfo.put("jobStatusInfoRaw", jobStatusJsonString);
		
		String jobMetricsJsonString = getJobMetricInfoJSONString(job);
		jobInfo.put("jobMetricInfoRaw", jobMetricsJsonString);		
		
		return(jobInfo);
	}
	

	//***********************************************
	// Get a domain bean
	//***********************************************
	private DomainMXBean getDomainBean(String domainName) { 
      ObjectName objName = ObjectNameBuilder.domain(domainName);
      DomainMXBean domain = JMX.newMXBeanProxy(getMsbc(), objName, DomainMXBean.class, true);
      return(domain);
	}
	

	//***********************************************
	// Get an instance bean
	//***********************************************
	private InstanceMXBean getInstanceBean(String domainName, String instanceName) { 
      ObjectName objName = ObjectNameBuilder.instance(domainName, instanceName ) ; 
      InstanceMXBean instance = JMX.newMXBeanProxy(getMsbc(), objName, InstanceMXBean.class, true);     
      return(instance);
	}	

	//***********************************************
	// Get a job bean
	//***********************************************
	private JobMXBean getJobBean(String domainName, String instanceName, long jobId) { 
      java.math.BigInteger jobNumber = java.math.BigInteger.valueOf(jobId);
      InstanceMXBean instance = getInstanceBean(domainName, instanceName);
      instance.registerJob(jobNumber);
      ObjectName objName = ObjectNameBuilder.job(domainName,instanceName, jobNumber ); 
      JobMXBean job = JMX.newMXBeanProxy(getMsbc(), objName, JobMXBean.class, true);        
      return(job);
	}
	
	//***********************************************
	// Get a host resource manager bean
	//***********************************************
	private StreamsHostResourceManagerMXBean getHostResourceManagerBean(String domainName) { 
      ObjectName objName = ObjectNameBuilder.resourceManager(domainName, ResourceMXBean.RESOURCE_TYPE_STREAMS);
      StreamsHostResourceManagerMXBean resourceManager = JMX.newMXBeanProxy(getMsbc(), objName, StreamsHostResourceManagerMXBean.class, true);
      return(resourceManager);
	}
	
	//***********************************************
	// Get a resource bean
	//***********************************************
	private ResourceMXBean getResourceBean(String domainName, String hostName) { 
      ObjectName objName = ObjectNameBuilder.resource(domainName, hostName);
      ResourceMXBean resource = JMX.newMXBeanProxy(getMsbc(), objName, ResourceMXBean.class, true);
      return(resource);
	}		
	
	//***********************************************
	// Set default hostname verifier
	//***********************************************
	private void setDefaultHostnameVerifier() throws Exception {
	  // Set up SSL so that we will trust signer certificates as well as verify any default hostname
	  TrustManager[] trustAllCerts = new TrustManager[] { 
	                  new X509TrustManager() {     
	                  public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
	                      return new X509Certificate[0];                    
	                  }                  
	                  public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {} 
	                  public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) throws CertificateException {}
	              } 
	          };     
	  SSLContext ctx = SSLContext.getInstance("TLSv1");
	  ctx.init(null, trustAllCerts, null); 
	  SSLSocketFactory ssf = ctx.getSocketFactory(); 
	  HttpsURLConnection.setDefaultSSLSocketFactory(ssf);    
	  HostnameVerifier hv = new HostnameVerifier() {
	      public boolean verify(String hostname, SSLSession session) {
	    	  return(true);
	      }
	  };
	  HttpsURLConnection.setDefaultHostnameVerifier(hv);
	  return;		
	}
	
	//***********************************************
	// Read from a URL
	//***********************************************
	private String readFromURL(URL url) throws Exception {
	  HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
	  conn.setRequestMethod("GET");		
      conn.connect();
	  BufferedReader in = new BufferedReader(new InputStreamReader(
	                                         conn.getInputStream()));
	  String inputLine;
	  String jsonString ="";
	  while ((inputLine = in.readLine()) != null) {  
	    jsonString = jsonString + inputLine;          
	  }
	  in.close();	    	
	  return(jsonString);		
	}
	
	
	//***********************************************
	// Read from a URL into a file
	//***********************************************
	private void readFromURLIntoFile(URL url, String fileName) throws Exception {
	  HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
	  conn.setRequestMethod("GET");		
      conn.connect();

      FileOutputStream out = new FileOutputStream(fileName);      
      java.io.InputStream in = conn.getInputStream();
      
      byte[] buffer = new byte[1024];
      int numBytes = 0;
      while (-1 != (numBytes = in.read(buffer))) {
    	  out.write(buffer, 0, numBytes);
      }
      in.close();
      out.close();
      return;

	}
	
	
	//***********************************************
	// Push a bundle file up to servia via
	// a http push
	//***********************************************
	private void pushFileToServer(URL url, File bundle) throws Exception {
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

      conn.setRequestMethod("PUT");
      conn.setRequestProperty("Content-Type", "application/x-jar");
      conn.setRequestProperty("Content-Length", Long.toString(bundle.length()));
      conn.setFixedLengthStreamingMode((int)bundle.length());
      conn.setDoOutput(true);
      
      FileInputStream fiStream = new FileInputStream(bundle.getAbsolutePath());
      
      java.io.OutputStream out = conn.getOutputStream();
      
      byte [] byteBuf = new byte[1024*64];
      int  bytesRead = 0;
      
      while (-1 != (bytesRead = fiStream.read(byteBuf))) {
    	  out.write(byteBuf, 0, bytesRead);
      }
      out.close();
      fiStream.close();
      
      // read the response
      int response = conn.getResponseCode();
      if (200 != response)
    	  throw new Exception("Unexpected response code pushing bundle file:  " + Integer.toString(response));
      
      return; 		
	}
	
	//***********************************************
	// Throw exception if not connected
	//***********************************************
	private void checkConnected() throws Exception {
      if (!isConnected())
	    throw new Exception("Not connected. Must call connectToJMX");  
	}
	
	
	//***********************************************	
	// Bean server connection
	//***********************************************	
	private MBeanServerConnection _mbsc;
	
	private boolean isConnected() {
	  if (null == getMsbc())
	    return(false);
	  else
		return(true);
	}
	
	private void setMsbc(MBeanServerConnection mbsc) {
      _mbsc = mbsc;
	}
	
	private MBeanServerConnection getMsbc() {
	  return(_mbsc);
	}
	
	
}	
