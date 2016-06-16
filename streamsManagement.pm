#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************
 
use strict;

use JSON;

use Data::Dumper;

package streamsManagement;

############
# Globals
############
use File::Basename;
my $thisDir = dirname(__FILE__);
my $zk;

my %_cache = ();


#######################################
# External routines start
#######################################


#######################################
# loadDomainCache
#
# Loads domain cache.  Caches domain information
# and a list of instances, but does not 
# drill down and retrieve instance information.
#
# Parms:
#
#   domainName - name of the domain
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub loadDomainCache($) {
  my ($domainName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "getDomainInfo -domain $domainName";
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(1, $output);
  }

  my $decodedJson = JSON::decode_json($output);
  _addToDomainCache($domainName, $decodedJson);
    
  return(0, undef);
}

#######################################
# loadInstanceCache
#
# Loads instance cache.  Caches instances information
# and a list of jobs, but does not 
# drill down and retrieve job information.
# By extension, parent domain cache is also
# updated.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of instance
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub loadInstanceCache($$) {
  my ($domainName, $instanceName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my $parms = "getInstanceInfo -domain $domainName -instance $instanceName";
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output);  
  _addToDomainCache($domainName, $decodedJson);
  _addToInstanceCache($domainName, $instanceName, $decodedJson);
    
  return(0, undef);
}

#######################################
# loadSingleJobCache
#
# Loads job cache for a specified job.
# By extension, parent instance cache and
# parent domain caches are also
# updated.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of instance
#   jobId  - job ID
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub loadSingleJobCache($$$) {
  my ($domainName, $instanceName, $jobId) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my $parms = "getSingleJobInfo -domain $domainName -instance $instanceName -job $jobId";
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output);   
  _addToDomainCache($domainName, $decodedJson);
  _addToInstanceCache($domainName, $instanceName, $decodedJson);
  _addToJobCache($domainName, $instanceName, $decodedJson);
    
  return(0, undef);
}

#######################################
# loadAllJobsCache
#
# Loads job cache for all jobs in an instance.
# By extension, parent instance cache and
# parent domain caches are also
# updated.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of instance
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub loadAllJobsCache($$) {
  my ($domainName, $instanceName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my $parms = "getAllJobInfo -domain $domainName -instance $instanceName";
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output);   
  _addToDomainCache($domainName, $decodedJson);
  _addToInstanceCache($domainName, $instanceName, $decodedJson);
  _addToJobCache($domainName, $instanceName, $decodedJson);
    
  return(0, undef);
}

#######################################
# clearCache
#
# Clear entire cache
#
# Parms:
#
#   none
#
# Returns:
#    none
#
#######################################
sub clearCache() {
  %_cache = ();
}


#######################################
# getDomainStatus
#
# Retrieve status of a domain
#
# Parms:
#
#   domainName - name of the domain
#   reloadCache(optional) - force domain cache to be reloaded
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    domain status (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getDomainStatus($;$) {
  my ($domainName, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getDomainInfo($domainName, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  my $status = $$output{status};    
  return(0, $status);
}


#######################################
# getDomainInstances
#
# Retrieve array of instances in a domain
#
# Parms:
#
#   domainName - name of the domain
#   reloadCache(optional) - force domain cache to be reloaded
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing instance 
#        names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getDomainInstances($;$) {
  my ($domainName, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getDomainInfo($domainName, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $instances = $$output{instances};    
  return(0, $instances);
}

#######################################
# getInstanceStatus
#
# Retrieve status of an instance
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   reloadCache(optional) - force instance cache to be reloaded
#              By extension, parent domain cache is
#              also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    domain status (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getInstanceStatus($$;$) {
  my ($domainName, $instanceName, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getInstanceInfo($domainName, $instanceName, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $status = $$output{status};    
  return(0, $status);
}

#######################################
# getInstanceJobs
#
# Retrieve array of jobs in an instance
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   reloadCache(optional) - force instance cache to be reloaded
#              By extension, parent domain cache is
#              also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing job 
#        ids (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getInstanceJobs($$;$) {
  my ($domainName, $instanceName, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getInstanceInfo($domainName, $instanceName, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobs = $$output{jobs};    
  return(0, $jobs);
}

#######################################
# getJobStatusInfoJSONString
#
# Returns raw string of output from the 
# JMX job bean's snapshot() method.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    JSON string (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobStatusInfoJSONString($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_);  
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
    
  my ($rc, $output) = _getJobInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobStatusInfoJSONString = $$output{jobStatusInfoRaw};    
  return(0, $jobStatusInfoJSONString);
}

#######################################
# getJobMetricInfoJSONString
#
# Returns raw string of output from the 
# JMX job bean's snapshotMetrics() method.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    JSON string (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobMetricInfoJSONString($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_);  
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
    
  my ($rc, $output) = _getJobInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobMetricInfoJSONString = $$output{jobMetricInfoRaw};    
  return(0, $jobMetricInfoJSONString);
}

#######################################
# getJobStatus
#
# Retrieves status of a job
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    job status (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobStatus($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getJobStatusInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobStatus = $$output{status};
     
  return(0, $jobStatus);  
}

#######################################
# getJobHealth
#
# Retrieves health of a job
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    job health (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobHealth($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getJobStatusInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobHealth = $$output{health};
     
  return(0, $jobHealth);  
}

#######################################
# getJobPEs
#
# Retrieves list of PE Ids in a job
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing PE
#        ids (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobPEs($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getJobStatusInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $allPEInfo = $$output{pes};
  
  my @peArray;
  foreach my $nextPE (@$allPEInfo) {
    my $nextId = $$nextPE{id};
    push(@peArray, $nextId);
  }
     
  return(0, \@peArray);  
}

#######################################
# getJobName
#
# Retrieves name of a job
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    job name (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobName($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getJobStatusInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobName = $$output{name};
     
  return(0, $jobName);  
}


#######################################
# getJobIdByJobName
#
# Retrieves a job id that correlates to a
# given job ID.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobName  - job Name
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobIdByName($$$;$) {
  my ($domainName, $instanceName, $jobName, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  
  # Search if is already cached
  if ((!defined($reloadCache)) || (!$reloadCache)) {
    my $jobId = _searchJobCacheForIdByName($domainName, $instanceName, $jobName);
    if (-1 != $jobId) {
      return(0, $jobId);
    }
  }
  
  # Try looking up the job by name
  my ($rc, $output) = _loadSingleJobCacheByName($domainName, $instanceName, $jobName);
  if ($rc) {
    return($rc, $output);
  }
  
  # If we get to here, we can assume it is in the cache
  my $jobId = _searchJobCacheForIdByName($domainName, $instanceName, $jobName);         
  return(0, $jobId);  
}


#######################################
# getJobOperators
#
# Retrieves operators running in a job
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator
#        names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getJobOperators($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getJobInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $opToPeMap = $$output{opToPeMap};
  
  my @operatorArray;
  foreach my $nextOperator (keys %$opToPeMap) {
    push(@operatorArray, $nextOperator);
  }
     
  return(0, \@operatorArray);  
}

#######################################
# getPEStatus
#
# Retrieves status of a PE
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    PE status (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEStatus($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEStatusInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $peStatus = $$output{status}; 
     
  return(0, $peStatus);  
}

#######################################
# checkIfAllPEsAreRunning
#
# Checks if all PEs within a specified job
# have a status of "running"
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    1 if all PEs are running, 0 if all PES
#      are not running (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub checkIfAllPEsAreRunning($$$;$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
   
  my $output;
  ($rc, $output) = streamsManagement::getJobPEs($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { return($rc, $output); }
  foreach my $nextPE (@$output) {
    ($rc, $output) = getPEStatus($domainName, $instanceName, $jobId, $nextPE);
    if ($output ne "running") {
      return(0, 0);
    }
  }
  
  # If we get here, all PEs are running
  return(0, 1);
}

#######################################
# getPEHealth
#
# Retrieves health of a PE
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    PE health (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEHealth($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEStatusInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $peHealth = $$output{health}; 
     
  return(0, $peHealth);  
}

#######################################
# getPEPid
#
# Retrieves process ID of a PE
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    PE pid (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEPid($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEStatusInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $pePid = $$output{processId}; 
     
  return(0, $pePid);  
}

#######################################
# getPEResource
#
# Retrieves resource that PE is running in
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    PE resource (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEResource($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEStatusInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $peResource = $$output{resource}; 
     
  return(0, $peResource);  
}

#######################################
# getPEOperators
#
# Retrieves operators running in a PE
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator
#        names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEOperators($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEStatusInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $allOperatorInfo = $$output{operators};
  
  my @operatorArray;
  foreach my $nextOperator (@$allOperatorInfo) {
    my $name = $$nextOperator{name};
    push(@operatorArray, $name);
  }
     
  return(0, \@operatorArray);  
}

#######################################
# getPEIdFromOperatorName
#
# Retrieves a PE ID that contains a given operator
# name.  
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    PE ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEIdFromOperatorName($$$$;$) {
  my ($domainName, $instanceName, $jobId, $operatorName, $reloadCache) = (@_);
     
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
    
  my ($rc, $output) = _getPeIdFromOperator($domainName, $instanceName, $jobId, $operatorName, $reloadCache);
  return($rc, $output);
}

#######################################
# getOperatorOutputPorts
#
# Retrieves a list of operator output port names
# for an operator
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        output port names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPorts($$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorStatusInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorOutputPortInfo = $$output{outputPorts};
    
  my @operatorOutputPortArray;
  foreach my $nextOperatorOutputPort (@$allOperatorOutputPortInfo) {
    my $name = $$nextOperatorOutputPort{name};
    push(@operatorOutputPortArray, $name);
  }
     
  return(0, \@operatorOutputPortArray);  
}

#######################################
# getOperatorOutputPortConnections
#
# Retrieves a list of operator output port connections
# for a output port.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        output port connection IDs (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnections($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorOutputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorOutputPortConnectionInfo = $$output{connections};
    
  my @operatorOutputPortConnectionArray;
  foreach my $nextOperatorOutputPortConnection (@$allOperatorOutputPortConnectionInfo) {
    my $id = $$nextOperatorOutputPortConnection{id};
    push(@operatorOutputPortConnectionArray, $id);
  }
     
  return(0, \@operatorOutputPortConnectionArray);  
}

#######################################
# getOperatorOutputPortConnectionSourceJob
#
# Retrieves operator output port connection source job ID
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnectionSourceJob($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my ($rc, $output) = _getOperatorOutputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $source = $$output{source};
  my $sourceJob = $$source{job}; 
     
  return(0, $sourceJob);  
}

#######################################
# getOperatorOutputPortConnectionSourcePE
#
# Retrieves operator output port connection source PE ID
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source PE ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnectionSourcePE($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorOutputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $source = $$output{source};
  my $sourcePE = $$source{pe}; 
     
  return(0, $sourcePE);  
}

#######################################
# getOperatorOutputPortConnectionSourceOperator
#
# Retrieves operator output port connection source operator
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source operator (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnectionSourceOperator($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorOutputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $source = $$output{source};
  my $sourceOperator = $$source{operator}; 
     
  return(0, $sourceOperator);  
}

#######################################
# getOperatorOutputPortConnectionTargetJob
#
# Retrieves operator output port connection target job ID
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnectionTargetJob($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorOutputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $target = $$output{target};
  my $targetJob = $$target{job}; 
     
  return(0, $targetJob);  
}

#######################################
# getOperatorOutputPortConnectionTargetPE
#
# Retrieves operator output port connection target job PE
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnectionTargetPE($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorOutputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $target = $$output{target};
  my $targetPE = $$target{pe}; 
     
  return(0, $targetPE);  
}

#######################################
# getOperatorOutputPortConnectionTargetOperator
#
# Retrieves operator output port connection target job operator
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortConnectionTargetOperator($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my ($rc, $output) = _getOperatorOutputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $target = $$output{target};
  my $targetOperator = $$target{operator}; 
     
  return(0, $targetOperator);  
}

#######################################
# checkIfOperatorOutputPortHasExport
#
# Checks if an operator output port has an
# Export operator attached to it.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    1 if there is an export, 0 if there is not an export(if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub checkIfOperatorOutputPortHasExport($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache) = (@_);
  
  
  my ($rc, $output) = _getOperatorOutputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  if (defined($$output{export})) {
    return(0, 1);
  }
  else {
    return(0, 0);
  }
}

#######################################
# getOperatorOutputPortExportOperatorName
#
# Returns the name of the Export operator
# attached to an output port (if there is one).
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    name of output port export operator (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortExportOperatorName($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache) = (@_);
  
  
  my ($rc, $output) = _getOperatorOutputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  if (!defined($$output{export})) {
    my $msg = "Operator output port $operatorOutputPort does not have an attached Export operator.";
    return(1, $msg);
  }
  
  my $export = $$output{export};
  my $exportOperatorName = $$export{operator};
  return(0, $exportOperatorName);
}

#######################################
# getOperatorInputPorts
#
# Retrieves a list of operator input port names
# for an operator
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        output port names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPorts($$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorStatusInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorInputPortInfo = $$output{inputPorts};
    
  my @operatorInputPortArray;
  foreach my $nextOperatorInputPort (@$allOperatorInputPortInfo) {
    my $name = $$nextOperatorInputPort{name};
    push(@operatorInputPortArray, $name);
  }
     
  return(0, \@operatorInputPortArray);  
}


#######################################
# getOperatorInputPortConnections
#
# Retrieves a list of operator input port connections
# for an input port.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        input port connection IDs (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnections($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorInputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorInputPortConnectionInfo = $$output{connections};
    
  my @operatorInputPortConnectionArray;
  foreach my $nextOperatorInputPortConnection (@$allOperatorInputPortConnectionInfo) {
    my $id = $$nextOperatorInputPortConnection{id};
    push(@operatorInputPortConnectionArray, $id);
  }
     
  return(0, \@operatorInputPortConnectionArray);  
}

#######################################
# getOperatorInputPortConnectionSourceJob
#
# Retrieves operator input port connection source job ID
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnectionSourceJob($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my ($rc, $output) = _getOperatorInputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $source = $$output{source};
  my $sourceJob = $$source{job}; 
     
  return(0, $sourceJob);  
}

#######################################
# getOperatorInputPortConnectionSourcePE
#
# Retrieves operator input port connection source PE ID
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source PE ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnectionSourcePE($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorInputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $source = $$output{source};
  my $sourcePE = $$source{pe}; 
     
  return(0, $sourcePE);  
}

#######################################
# getOperatorInputPortConnectionSourceOperator
#
# Retrieves operator output port connection source operator
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source operator (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnectionSourceOperator($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorInputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $source = $$output{source};
  my $sourceOperator = $$source{operator}; 
     
  return(0, $sourceOperator);  
}

#######################################
# getOperatorInputPortConnectionTargetJob
#
# Retrieves operator input port connection target job ID
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnectionTargetJob($$$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorInputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $target = $$output{target};
  my $targetJob = $$target{job}; 
     
  return(0, $targetJob);  
}

#######################################
# getOperatorInputPortConnectionTargetPE
#
# Retrieves operator input port connection target job PE
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnectionTargetPE($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorInputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $target = $$output{target};
  my $targetPE = $$target{pe}; 
     
  return(0, $targetPE);  
}

#######################################
# getOperatorInputPortConnectionTargetOperator
#
# Retrieves operator input port connection target job operator
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   connection - id of connection
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    connection source job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortConnectionTargetOperator($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my ($rc, $output) = _getOperatorInputPortConnectionStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $connection, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $target = $$output{target};
  my $targetOperator = $$target{operator}; 
     
  return(0, $targetOperator);  
}

#######################################
# checkIfOperatorInputPortHasImport
#
# Checks if an operator input port has an
# Import operator attached to it.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator ibput port - name of operator input port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    1 if there is an import, 0 if there is not an import(if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub checkIfOperatorInputPortHasImport($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache) = (@_);
    
  my ($rc, $output) = _getOperatorInputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }  
  
  if (defined($$output{import})) {
    return(0, 1);
  }
  else {
    return(0, 0);
  }
}

#######################################
# getOperatorInputPortImportOperatorName
#
# Returns the name of the Import operator
# attached to an input port (if there is one).
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    name of output port export operator (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortImportOperatorName($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache) = (@_);
  
  
  my ($rc, $output) = _getOperatorInputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  if (!defined($$output{import})) {
    my $msg = "Operator input port $operatorInputPort does not have an attached Import operator.";
    return(1, $msg);
  }
  
  my $import = $$output{import};
  my $importOperatorName = $$import{operator};
  return(0, $importOperatorName);
}

#######################################
# getPELastTimeRetrieveMetrics
#
# Retrieves timestamp of last time
# metrics from a PE were retrieved by Streams
# runtime.  
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    Epoch timestamp of when metrics were retrieved (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPELastTimeRetrievedMetrics($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEMetricsInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $lastTimeRetrieved = $$output{lastTimeRetrieved};
  
  return(0, $lastTimeRetrieved);  
}  

#######################################
# getPEMetricNames
#
# Retrieves a list of PE metric names
# available for a given PE.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing PE metric 
#        names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEMetricNames($$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEMetricsInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allPEMetricsInfo = $$output{metrics};
    
  my @peMetricArray;
  foreach my $nextMetric (@$allPEMetricsInfo) {
    my $name = $$nextMetric{name};
    push(@peMetricArray, $name);
  }
     
  return(0, \@peMetricArray);  
}

#######################################
# getPEMetricValue
#
# Retrieves a value of a PE Meric
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   peId   - PE ID
#   metricName - name of metric
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    PE metric value (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getPEMetricValue($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $peId, $metricName, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getPEMetricsInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allPEMetricsInfo = $$output{metrics};
    
  foreach my $nextMetric (@$allPEMetricsInfo) {
    if ($$nextMetric{name} eq $metricName) {
      return(0, $$nextMetric{value});
    }
  }

  my $msg = "Unable to find PE metric $metricName for PE $peId";     
  return(0, $msg);  
}

#######################################
# getOperatorMetricNames
#
# Retrieves a list of operator metric names
# available for a given operator.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        metric names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorMetricNames($$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorMetricsInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorMetricsInfo = $$output{metrics};
    
  my @operatorMetricArray;
  foreach my $nextMetric (@$allOperatorMetricsInfo) {
    my $name = $$nextMetric{name};
    push(@operatorMetricArray, $name);
  }
     
  return(0, \@operatorMetricArray);  
}

#######################################
# getOperatorMetricValue
#
# Retrieves a value of a operator metric
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   metricName - name of metric
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    Operator metric value (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorMetricValue($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $metricName, $reloadCache) = (@_); 
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;  
  
  my ($rc, $output) = _getOperatorMetricsInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorMetricsInfo = $$output{metrics};
    
  foreach my $nextMetric (@$allOperatorMetricsInfo) {
    if ($$nextMetric{name} eq $metricName) {
      return(0, $$nextMetric{value});
    }
  }

  my $msg = "Unable to find Operator metric $metricName for Operator $operator";     
  return(0, $msg);  
}

#######################################
# getOperatorOutputPortMetricNames
#
# Retrieves a list of operator output port
# metric names available for a given output port.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        output port metric names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortMetricNames($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $outputPort, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my ($rc, $output) = _getOperatorOutputPortMetricsInfo($domainName, $instanceName, $jobId, $operator, $outputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorOutputPortMetricsInfo = $$output{metrics};
    
  my @operatorOutputPortMetricArray;
  foreach my $nextMetric (@$allOperatorOutputPortMetricsInfo) {
    my $name = $$nextMetric{name};
    push(@operatorOutputPortMetricArray, $name);
  }
     
  return(0, \@operatorOutputPortMetricArray);  
}

#######################################
# getOperatorOutputPortMetricValue
#
# Retrieves a value of a operator output port
# metric
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator output port - name of operator output port
#   metricName - name of metric
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    Operator metric value (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorOutputPortMetricValue($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $outputPort, $metricName, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my ($rc, $output) = _getOperatorOutputPortMetricsInfo($domainName, $instanceName, $jobId, $operator, $outputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOutputPortMetricsInfo = $$output{metrics};
    
  foreach my $nextMetric (@$allOutputPortMetricsInfo) {
    if ($$nextMetric{name} eq $metricName) {
      return(0, $$nextMetric{value});
    }
  }

  my $msg = "Unable to find Operator Output Port metric $metricName for Output Port $outputPort";     
  return(0, $msg);  
}

#######################################
# getOperatorInputPortMetricNames
#
# Retrieves a list of operator input port
# metric names available for a given input port.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    reference to array containing operator 
#        input port metric names (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortMetricNames($$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $inputPort, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;   
  
  my ($rc, $output) = _getOperatorInputPortMetricsInfo($domainName, $instanceName, $jobId, $operator, $inputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allOperatorInputPortMetricsInfo = $$output{metrics};
    
  my @operatorInputPortMetricArray;
  foreach my $nextMetric (@$allOperatorInputPortMetricsInfo) {
    my $name = $$nextMetric{name};
    push(@operatorInputPortMetricArray, $name);
  }
     
  return(0, \@operatorInputPortMetricArray);  
}


#######################################
# getOperatorInputPortMetricValue
#
# Retrieves a value of a operator input port
# metric
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   operator - name of operator
#   operator input port - name of operator input port
#   metricName - name of metric
#   reloadCache(optional) - force job cache to be reloaded
#              By extension, parent domain cache and 
#              parent instance caches are also updated.
#      0 = do not reload the cache (default)
#      1 = reload the cache
#
# Returns:
#    return code
#    Operator metric value (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getOperatorInputPortMetricValue($$$$$$;$) {
  my ($domainName, $instanceName, $jobId, $operator, $inputPort, $metricName, $reloadCache) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;   
  
  my ($rc, $output) = _getOperatorInputPortMetricsInfo($domainName, $instanceName, $jobId, $operator, $inputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
    
  my $allInputPortMetricsInfo = $$output{metrics};
    
  foreach my $nextMetric (@$allInputPortMetricsInfo) {
    if ($$nextMetric{name} eq $metricName) {
      return(0, $$nextMetric{value});
    }
  }

  my $msg = "Unable to find Operator Input Port metric $metricName for Input Port $inputPort";     
  return(0, $msg);  
}

#######################################
# submitJob
#
# Submit a streams application
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   bundle  - application bundle file
#   jobParms - reference to an array
#              containing job submit parameters.
#              Optional parameter.
#              Each entry in the array should
#              contain the format name=value.
#   jobGroup - name for jobGroup.  Optional parameter.
#               If undefined, the
#               default job group will be used.
#   jobName - name for job.  Optional parameter.
#               If undefined, a
#               default name will be used.
#
# Returns:
#    return code
#    Job ID (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub submitJob($$$;$$$) {
  my ($domainName, $instanceName, $bundle, $jobParms, $jobGroup, $jobName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "submitJob -domain $domainName -instance $instanceName -bundle $bundle";
  if (defined($jobParms)) {
    foreach my $nextParm (@$jobParms) {
      $parms = "$parms -jobParm $nextParm";
    }
  }
  if ((defined($jobGroup)) && (length($jobGroup) > 0)) {
    $parms = "$parms -jobGroup $jobGroup";
  }
  if ((defined($jobName)) && (length($jobName) > 0)) {
    $parms = "$parms -jobName $jobName";
  }
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output);  
  my $jobId = $$decodedJson{jobId};  
   
  return(0, $jobId);  
}

#######################################
# cancelJob
#
# Cancels a streams application
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   forceCancel - force job to be cancelled.
#      0 = do not force the cancel (default)
#      1 = force the cancel
#                 
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub cancelJob($$$;$) {
  my ($domainName, $instanceName, $jobId, $forceCancel) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "cancelJob -domain $domainName -instance $instanceName -job $jobId";
  if (!(defined($forceCancel)) || ($forceCancel)) {
    $parms = "$parms -force";
  }
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}


#######################################
# addDomainHost
#
# Add a host to a domain
#
# Parms:
#
#   domainName - name of the domain
#   hostName - name of the host
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub addDomainHost($$) {
  my ($domainName, $hostName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "addDomainHost -domain $domainName -host $hostName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# removeDomainHost
#
# Remove a host from a domain
#
# Parms:
#
#   domainName - name of the domain
#   hostName - name of the host
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub removeDomainHost($$) {
  my ($domainName, $hostName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "removeDomainHost -domain $domainName -host $hostName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# getDomainHosts
#
# Retrieve hosts of a domain
#
# Parms:
#
#   domainName - name of the domain
#
# Returns:
#    return code
#    reference to array containing hosts 
#        in the domain (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getDomainHosts($) {
  my ($domainName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "getDomainHosts -domain $domainName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output); 
     
  return(0, $decodedJson);  
}

#######################################
# addTagToHost
#
# Add a tag to a host
#
# Parms:
#
#   domainName - name of the domain
#   hostName - name of the host
#   tag      - name of tag
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub addTagToHost($$$) {
  my ($domainName, $hostName, $tagName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "addTagToHost -domain $domainName -host $hostName -tag $tagName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# removeTagFromHost
#
# Remove a tag from a host
#
# Parms:
#
#   domainName - name of the domain
#   hostName - name of the host
#   tag      - name of tag
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub removeTagFromHost($$$) {
  my ($domainName, $hostName, $tagName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "removeTagFromHost -domain $domainName -host $hostName -tag $tagName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# getHostTags
#
# Retrieve tags assigned to a hosts 
# within a domain
#
# Parms:
#
#   domainName - name of the domain
#   hostName - name of the host
#
# Returns:
#    return code
#    reference to array containing tags 
#        on the host (if return code is 0)
#    error message (if return code is not 0)
#
#######################################
sub getHostTags($$) {
  my ($domainName, $hostName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "getHostTags -domain $domainName -host $hostName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output); 
     
  return(0, $decodedJson);  
}

#######################################
# makeInstance
#
# Create an instance
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   adminGroup - name of the admin group.  Leave
#                undefined to take default.
#                Optional parameter.
#   userGroup - name of the user group.  Leave
#                undefined to take default.
#                Optional parameter.
#   properties - reference to an array
#                containing parameters.
#                Optional parameter.
#                Each entry in the array should
#                contain the format name=value.
#   resourceSpecs - reference to a structure containing
#                   resource specs for the instance.
#                   This structure should be populated
#                   by the initResourceSpecs() and
#                   addToResourceSpecs() routines.
#                   Optional parameter.
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub makeInstance($$;$$$$) {
  my ($domainName, $instanceName, $adminGroup, $userGroup, $properties, $resourceSpecs) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "makeInstance -domain $domainName -instance $instanceName";
  
  if (defined($adminGroup)) { $parms = "$parms -adminGroup $adminGroup"; }
  if (defined($userGroup))  { $parms = "$parms -userGroup $userGroup"; }
  
  # properties
  if (defined($properties)) {
    foreach my $nextProperty (@$properties) {
      $parms = "$parms -property $nextProperty";
    }
  }
  
  # resource specs
  if (defined($resourceSpecs)) {
    foreach my $nextResourceSpec (@$resourceSpecs) {
      my $count = $$nextResourceSpec{COUNT};
      my $tagArray = $$nextResourceSpec{TAGS};
      my $comma = "";
      my $tags = "";
      foreach my $nextTag (@$tagArray) {
        $tags = "$comma" . "$nextTag";
        $comma = ",";
      }
      my $exclusive = "false";
      if (1 == $$nextResourceSpec{EXCLUSIVE}) {
        $exclusive = "true";
      }
      $parms = "$parms -resourceCount $count -resourceTags $tags -resourceExclusive $exclusive";
    }
  }

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# initResourceSpecs
#
# Initialize an empty structure that can be used
# for the resourceSpecs parameter for the 
# makeInstance routine.
# After this, addToResourceSpec() can be called
# to populate this structure.
#
# Parms:
#
#   none

#
# Returns:
#    reference to empty resource spec structure.
#
#######################################
sub initResourceSpecs() {

  my @resourceSpecs = ();
  return(\@resourceSpecs);

}
#######################################
# addToResourceSpecs
#
# Add resource specification to a structure
# that can be passed into the makeInstance
# routine.  initResourceSpecs should 
# be called prior to this routine to initialize.
#
# Parms:
#
#   resouceSpecs - structure that the resource spec
#                  will be added to.  
#   count - number of resources
#   tags  - reference to an array of tags
#   exclusive - must this resource be exclusive
#                to this particular resource.
#                valid values:
#                   1:  yes, should be exclusive
#                   0:  no, does not need to be exclusive
#
# Returns:
#    nothing
#
#######################################
sub addToResourceSpecs($$$$) {

  my ($resourceSpecs, $count, $tags, $exclusive) = (@_);

  my $nextSpec = {};
  $$nextSpec{COUNT} = $count;
  $$nextSpec{TAGS} = $tags;
  $$nextSpec{EXCLUSIVE} = $exclusive;
  
  push(@$resourceSpecs, $nextSpec);
  
  return;
}

#######################################
# removeInstance
#
# Removes a streams instance
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub removeInstance($$) {
  my ($domainName, $instanceName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "removeInstance -domain $domainName -instance $instanceName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# startInstance
#
# Starts a streams instance
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub startInstance($$) {
  my ($domainName, $instanceName) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "startInstance -domain $domainName -instance $instanceName";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# stopInstance
#
# Stops a streams instance
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   forceStop - force instance to be stopped
#      0 = do not force (default)
#      1 = force
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub stopInstance($$;$) {
  my ($domainName, $instanceName, $forceStop) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "stopInstance -domain $domainName -instance $instanceName";
  if (!(defined($forceStop)) || ($forceStop)) {
    $parms = "$parms -force";
  }

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
     
  return(0, undef);  
}

#######################################
# makeDomain
#
# Create a domain
# Note this API does not use the JMX interface like
# most of the APIs in this module.
# Instead it uses the streamtool command.
#
# Parms:
#
#   domainName - name of the domain
#   hosts - reference to an array
#                containing host names for the domain.
#                Optional parameter.
#                By default, the host this command is run
#                from will also be added to the domain
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub makeDomain($;$) {
  my ($domainName, $hosts) = (@_);
  
  # build hosts file
  my $hostFile = "/tmp/hosts.$ENV{USER}.$$";
  open(my $fh, '>', $hostFile) or die "Could not open $hostFile";
  foreach my $nextHost (@$hosts) {
    print $fh "$nextHost\n";
  }
  close $fh;
  
  my $cmd = "streamtool mkdomain -d $domainName --hfile $hostFile --property sws.port=0 --property jmx.port=0";
  
  my ($rc, $output) = _runCommand($cmd);
  
  system("rm -f $hostFile");
  
  return($rc, $output);
}


#######################################
# genKey
#
# Generate a public and private key pair
# Note this API does not use the JMX interface like
# most of the APIs in this module.
# Instead it uses the streamtool command.
#
# Parms:
#
#   domainName - name of the domain
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub genKey($) {
  my ($domainName) = (@_);
  
  my $cmd = "streamtool genkey -d $domainName";
  
  my ($rc, $output) = _runCommand($cmd);
  
  return($rc, $output);
}

#######################################
# removeDomain
#
# Remove a domain
# Note this API does not use the JMX interface like
# most of the APIs in this module.
# Instead it uses the streamtool command.
#
# Parms:
#
#   domainName - name of the domain
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub removeDomain($) {
  my ($domainName) = (@_);

  
  my $cmd = "streamtool rmdomain -d $domainName --noprompt";
  
  my ($rc, $output) = _runCommand($cmd);
  
  return($rc, $output);
}

#######################################
# startDomain
#
# Start a domain
# Note this API does not use the JMX interface like
# most of the APIs in this module.
# Instead it uses the streamtool command.
#
# Parms:
#
#   domainName - name of the domain
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub startDomain($) {
  my ($domainName) = (@_);

  
  my $cmd = "streamtool startdomain -d $domainName";
  
  my ($rc, $output) = _runCommand($cmd);
  
  return($rc, $output);
}


#######################################
# stopDomain
#
# Stop a domain
# Note this API does not use the JMX interface like
# most of the APIs in this module.
# Instead it uses the streamtool command.
#
# Parms:
#
#   domainName - name of the domain
#   forceStop - force domain to be stopped
#      0 = do not force (default)
#      1 = force
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub stopDomain($;$) {
  my ($domainName, $forceStop) = (@_);
  
  my $cmd = "streamtool stopdomain -d $domainName";
  if (!(defined($forceStop)) || ($forceStop)) {
    $cmd = "$cmd --force";
  }
  
  my ($rc, $output) = _runCommand($cmd);
  
  return($rc, $output);
}

#######################################
# getJobLogs
#
# Gets application log and trace files
# from a specified job and puts them 
# into a specified tar.gz file.
#
# Parms:
#
#   domainName - name of the domain
#   instanceName - name of the instance
#   jobId  - job ID
#   logFile - Filename for log files.  If 
#             file already exists it will
#             be overwritten.  This file
#             will be in tar.gz format
#             so it is suggested that 
#             this file be given a
#             .tar.gz extension
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub getJobLogs($$$$) {
  my ($domainName, $instanceName, $jobId, $logFile) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "getJobLogs -domain $domainName -instance $instanceName -job $jobId -logFile $logFile";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
  
  return(0, undef);
}

#######################################
# getDomainLogs
#
# Gets domain log and trace files
# from a specified domain and puts them 
# into a specified tar.gz file.
#
# Parms:
#
#   domainName - name of the domain
#   logFile - Filename for log files.  If 
#             file already exists it will
#             be overwritten.  This file
#             will be in tar.gz format
#             so it is suggested that 
#             this file be given a
#             .tar.gz extension
#
# Returns:
#    return code
#    error message (if return code is not 0)
#
#######################################
sub getDomainLogs($$) {
  my ($domainName, $logFile) = (@_);
  
  my ($rc, $msg) = _checkConnInfo();
  return($rc, $msg) if $rc;
  
  my $parms = "getDomainLogs -domain $domainName -logFile $logFile";

  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(-1, $output);
  }
  
  return(0, undef);
}

#######################################
# dumpCache
#
# Dumps the contents of the cache.
#
# Parms:
#
#   none
#
# Returns:
#    printed representation of the cache
#
#######################################
sub dumpCache() {
  my $dump = Data::Dumper::Dumper(\%_cache);
  return($dump);
}


#######################################
# External routines end
#######################################

#######################################
# Internal routines start
#######################################

#######################################
# _checkConnInfo
#######################################
sub _checkConnInfo() {

  # Make sure STREAMS_INSTALL is set 
  if (!defined($ENV{STREAMS_INSTALL})) {
    my $msg = "STREAMS_INSTALL is not set.";
    return(1, $msg);
  }
  
  # Make sure STREAMS_ZKCONNECT is set OR
  # STREAMS_EMBEDDEDZK = 1
  
  # only need to do this once:
  if (!defined($zk)) {
  
    if ((defined($ENV{STREAMS_ZKCONNECT})) && (length($ENV{STREAMS_ZKCONNECT}) > 0)) {
      $zk = $ENV{STREAMS_ZKCONNECT};
    } 
    elsif ((defined($ENV{STREAMS_EMBEDDEDZK})) && ("$ENV{STREAMS_EMBEDDEDZK}x" eq "1x")) {
      my ($rc, $output) = _getEmbeddedZKString();
      if ($rc) {
        return($rc, $output);
      }
      else {
        $zk = $output;
      }
    }
    else {
      my $msg = "Either Environment variable STREAMS_ZKCONNECT must be set OR " .
                "environment variable STREAMS_EMBEDDEDZK must be set to 1";
      return(1, $msg);
    }
  }
  
  return(0, undef);
}


#######################################
# _getEmbeddedZKString
#######################################
sub _getEmbeddedZKString() {
  my $cmd = "streamtool getbootproperty streams.zookeeper.quorum";
  my $results = `$cmd`;
  my $rc = $?;
  chomp $results;
  my $zkString = "";
  my $emsg = "Error determining embedded zookeeper location.";  
  if ($rc) {
    return(1, $emsg);
  }
  else {
    if ($results =~ /.*=(.*)$/) {
        $zkString = $1;       
    }
    else {   
      return(1, $emsg);
    }
  }
    
  my $cmd = "streamtool getbootproperty streams.zookeeper.property.clientPort";    
  my $results = `$cmd`;
  my $rc = $?;
  chomp $results;
  if ($rc) {
    return(1, $emsg);
  }
  else {
    if ($results =~ /.*=(.*)$/) {
      $zkString = "$zkString:$1";       
    }
    else {
      return(1, $emsg);
    }
  }    
     
  return(0, $zkString);        
}

#######################################
# _javaManagement
#######################################
sub _javaManagement($) {
  my ($parms) = (@_);
  my $internalJava = "$thisDir/streamsManagementInternal/java/bin";
  my $streamsLoc = $ENV{STREAMS_INSTALL};
  my $cp = "$internalJava" 
            . ":$streamsLoc/lib/com.ibm.streams.management.jmxmp.jar" 
            . ":$streamsLoc/lib/com.ibm.streams.management.mx.jar"
            . ":$streamsLoc/lib/jmxremote_optional.jar"
            . ":$streamsLoc/ext/lib/JSON4J.jar"
            . ":$streamsLoc/system/impl/lib/com.ibm.streams.platform.jar"
            . ":$streamsLoc/system/impl/lib/com.ibm.streams.management.mx.util.jar";
      
  my $connectParms = "-zkconnect $zk";  # $zk was set in _checkConnInfo   
  my $cmd = "java -cp $cp com.ibm.streamsx.management.StreamsManagementWrapper $parms $connectParms";
  #print("ZZZ $cmd\n");
  my $result = `$cmd`;
  my $rc = $?;
  chomp $result;   
  if ($rc) {
    my $msg = "Error invoking java:  $result";
    return(1, $msg);
  }
  if ($result =~ /^EXCEPTION:(.*)/) {
    my $msg = $1;
    return(1, $msg);
  }
  #else $result should contain JSON string
  return(0, $result);
}

#######################################
# _addToDomainCache
#######################################
sub _addToDomainCache($$) {
  my ($domainName, $decodedJson) = (@_);
  
    if (!defined($_cache{DOMAINCACHE})) {
    $_cache{DOMAINCACHE} = {};
  }
  my $domainCache = $_cache{DOMAINCACHE};
  
  my $domainInfo = $$decodedJson{domainInfo};
  $$domainCache{$domainName} = $domainInfo;
}

#######################################
# _getDomainInfo
#######################################
sub _getDomainInfo($$) {
  my ($domainName, $reloadCache) = (@_);  
  
  my ($rc, $output) = _checkDomainCache($domainName, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $domainCache = $_cache{DOMAINCACHE};
  my $domainInfo = $$domainCache{$domainName};
  return(0, $domainInfo);
}

#######################################
# _checkDomainCache
#######################################
sub _checkDomainCache($$) {
  my ($domainName, $reloadCache) = (@_);
  
  if (not defined($reloadCache)) {
    $reloadCache = 0;
  }
  
  my $reload = 0;
  if ($reloadCache)  {
    $reload = 1;
  }
  else {
    my $domainCache = $_cache{DOMAINCACHE};    
    if (!defined($domainCache)) {
      $reload = 1;
    }
    else {  
      if (!defined($$domainCache{$domainName})) {       
        $reload = 1;
      }
    }      
  }
  
  if ($reload) {
    my ($rc, $output) = loadDomainCache($domainName);
    if ($rc) {
      return($rc, $output);
    }
  }
  return(0, undef);
}


#######################################
# _addToInstanceCache
#######################################
sub _addToInstanceCache($$$) {
  my ($domainName, $instanceName, $decodedJson) = (@_);
  
  if (!defined($_cache{INSTANCECACHE})) {
    $_cache{INSTANCECACHE} = {};
  }
  my $instanceDomainCache = $_cache{INSTANCECACHE};
  
  if (!defined($$instanceDomainCache{$domainName})) {
    $$instanceDomainCache{$domainName} = {};
  }
  my $instanceCache = $$instanceDomainCache{$domainName}; 
   
  my $instanceInfo = $$decodedJson{instanceInfo};  
  $$instanceCache{$instanceName} = $instanceInfo;  
  
  _removeObsoleteCachedJobs($domainName, $instanceName);
}


#######################################
# _getInstanceInfo
#######################################
sub _getInstanceInfo($$$) {
  my ($domainName, $instanceName, $reloadCache) = (@_);  
  
  my ($rc, $output) = _checkInstanceCache($domainName, $instanceName, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $instanceDomainCache = $_cache{INSTANCECACHE};
  
  my $instanceCache = $$instanceDomainCache{$domainName};
  my $instanceInfo = $$instanceCache{$instanceName};
  return(0, $instanceInfo);
}


#######################################
# _checkInstanceCache
#######################################
sub _checkInstanceCache($$$) {
  my ($domainName, $instanceName, $reloadCache) = (@_);
  
  if (not defined($reloadCache)) {
    $reloadCache = 0;
  }
  
  my $reload = 0;
  if ($reloadCache)  {
    $reload = 1;
  }
  else {
    my $instanceDomainCache = $_cache{INSTANCECACHE};    
    if (!defined($instanceDomainCache)) {
      $reload = 1;
    }
    else {
      my $instanceCache = $$instanceDomainCache{$domainName};
      if (!defined($instanceCache)) {
        $reload = 1;      
      }  
      else {
        if (!defined($$instanceCache{$instanceName})) {       
          $reload = 1;         
        }
      }
    }      
  }
  
  if ($reload) {
    my ($rc, $output) = loadInstanceCache($domainName, $instanceName);
    if ($rc) {
      return($rc, $output);
    }
  }
  return(0, undef);
}


#######################################
# _addToJobCache
#######################################
sub _addToJobCache($$$) {
  my ($domainName, $instanceName, $decodedJson) = (@_);
  
  if (!defined($_cache{JOBCACHE})) {
    $_cache{JOBCACHE} = {};
  }
  my $jobDomainCache = $_cache{JOBCACHE};
  
  if (!defined($$jobDomainCache{$domainName})) {
    $$jobDomainCache{$domainName} = {};
  }
  my $jobInstanceCache = $$jobDomainCache{$domainName};  
   
  if (!defined($$jobInstanceCache{$instanceName})) {
    $$jobInstanceCache{$instanceName} = {};
  }
  my $allJobsCache = $$jobInstanceCache{$instanceName};
  
  my $allJobsInfo = $$decodedJson{jobInfo};
  
  foreach my $nextJobId (keys %$allJobsInfo) {
    my $singleJobInfo = $$allJobsInfo{$nextJobId};
    
    # Parse/expand the jobInfo and jobMetrics structures
    $$singleJobInfo{jobStatusInfo} = JSON::decode_json($$singleJobInfo{jobStatusInfoRaw});
    $$singleJobInfo{jobMetricsInfo} = JSON::decode_json($$singleJobInfo{jobMetricInfoRaw});
    
    # Load the operator to pe map
    $$singleJobInfo{opToPeMap} = _loadOpToPeMap($$singleJobInfo{jobStatusInfo});
         
    $$allJobsCache{$nextJobId} = $singleJobInfo;
  }  
}

#######################################
# _removeObsoleteCachedJobs
#######################################
sub _removeObsoleteCachedJobs($$) {
  my ($domainName, $instanceName) = (@_);
  
  # Find the instance cache for this instance...
  my $instanceCache = $_cache{INSTANCECACHE};
  return if !defined($instanceCache);
  $instanceCache = $$instanceCache{$domainName};
  return if !defined($instanceCache);
  $instanceCache = $$instanceCache{$instanceName};
  return if !defined($instanceCache);  
  my $instanceJobs = $$instanceCache{jobs};
  
  # Find the job cache for this instance...
  my $jobCache = $_cache{JOBCACHE};
  return if !defined($jobCache);
  $jobCache = $$jobCache{$domainName};
  return if !defined($jobCache);
  $jobCache = $$jobCache{$instanceName};
  return if !defined($jobCache);

  foreach my $nextJobId (keys %$jobCache) {
    # If this doesn't exist, then remove from jobCache
    my $found = 0;
    foreach my $nextInstanceJob (@$instanceJobs) {
      if ($nextInstanceJob == $nextJobId) {
        $found = 1;
        next;
      }
    }
    if (!$found) {
      delete $$jobCache{$nextJobId};
    }
  }  
}


#######################################
# _loadOpToPEmap
#######################################
sub _loadOpToPeMap($) {
  my ($jobStatusInfo) = (@_);
  
  my $opToPeMap = {};
  
  my $peList = $$jobStatusInfo{pes};
  foreach my $nextPE (@$peList) {
    my $peId = $$nextPE{id};
    my $operatorList = $$nextPE{operators};
    foreach my $nextOperator (@$operatorList) {
      my $operatorName = $$nextOperator{name};
      $$opToPeMap{$operatorName} = $peId;
    }
  }
  
  return($opToPeMap);
}

#######################################
# _getJobInfo
#######################################
sub _getJobInfo($$$$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_);  
  
  my ($rc, $output) = _checkJobCache($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $jobDomainCache = $_cache{JOBCACHE};
  my $jobInstanceCache = $$jobDomainCache{$domainName};
  my $allJobsCache = $$jobInstanceCache{$instanceName};
  my $jobInfo = $$allJobsCache{$jobId};
  return(0, $jobInfo);
}

#######################################
# _checkJobCache
#######################################
sub _checkJobCache($$$$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_);
  
  if (not defined($reloadCache)) {
    $reloadCache = 0;
  }
  
  my $reload = 0;
  if ($reloadCache)  {
    $reload = 1;
  }
  else {
    my $jobDomainCache = $_cache{JOBCACHE};    
    if (!defined($jobDomainCache)) {
      $reload = 1;
    }
    else {
      my $jobInstanceCache = $$jobDomainCache{$domainName};
      if (!defined($jobInstanceCache)) {
        $reload = 1;      
      }  
      else {
        my $allJobsCache = $$jobInstanceCache{$instanceName};
        if (!defined($allJobsCache)) {
          $reload = 1;
        }      
        else {
          if (!defined($$allJobsCache{$jobId})) {       
            $reload = 1;         
          }
        }
      }
    }      
  }
  
  if ($reload) {
    my ($rc, $output) = loadSingleJobCache($domainName, $instanceName, $jobId);
    if ($rc) {
      return($rc, $output);
    }
  }
  return(0, undef);
}

#######################################
# _getJobStatusInfo
#######################################
sub _getJobStatusInfo($$$$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getJobInfo($domainName, $instanceName, $jobId, $reloadCache);
    if ($rc) {
    return($rc, $output);
  }
  
  my $jobStatusInfo = $$output{jobStatusInfo};  
  return(0, $jobStatusInfo);  
}

#######################################
# _getPEStatusInfo
#######################################
sub _getPEStatusInfo($$$$$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getJobStatusInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through PEs until we find the desired PE.  
  my $allPEInfo = $$output{pes};
  foreach my $nextPE (@$allPEInfo) {
    my $nextId = $$nextPE{id};
    if ($nextId == $peId) {
      return(0, $nextPE);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired PE.
  my $msg = "PE with ID $peId does not exist in job $jobId";
  return(1, $msg);
}


#######################################
# _getPeIdFromOperator
#######################################
sub _getPeIdFromOperator($$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $reloadCache) = (@_);
  
  my ($rc, $output) = _getJobInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  my $opToPeMap = $$output{opToPeMap};
  my $peId = $$opToPeMap{$operator};
  if (!defined($peId)) {
    my $msg = "Cannot find operator $operator in job $jobId";
    return(1, $msg);
  }    
  return(0, $peId);
}

#######################################
# _getOperatorStatusInfo
#######################################
sub _getOperatorStatusInfo($$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $reloadCache) = (@_); 
  
  my ($rc, $output) = _getPeIdFromOperator($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  } 
  my $peId = $output;
  $reloadCache = 0;  # no point in loading again here
  
  my ($rc, $output) = _getPEStatusInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through operators until we find the desired Operator.  
  my $allOperatorsInfo = $$output{operators};
  foreach my $nextOperator (@$allOperatorsInfo) {
    my $name = $$nextOperator{name};
    if ($name eq $operator) {
      return(0, $nextOperator);
    }
  }
  
  # We shouldn't ever get here now that we are using the operator to PE cache
  # If we get here.  Error occurred -- cannot find the desired operator.
  my $msg = "Operator name $operator does not exist in PE $peId";
  return(1, $msg);
}

#######################################
# _getOperatorOutputPortStatusInfo
#######################################
sub _getOperatorOutputPortStatusInfo($$$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getOperatorStatusInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through operator output ports until we find the desired output port 
  my $allOperatorOutputPortInfo = $$output{outputPorts};
  foreach my $nextOperatorOutputPort (@$allOperatorOutputPortInfo) {
    my $name = $$nextOperatorOutputPort{name};
    if ($name eq $operatorOutputPort) {
      return(0, $nextOperatorOutputPort);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator output port..
  my $msg = "Operator output port  $operatorOutputPort does not exist in operator $operator";
  return(1, $msg);
}

#######################################
# _getOperatorOutputPortConnectionStatusInfo
#######################################
sub _getOperatorOutputPortConnectionStatusInfo($$$$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $operatorOutputPortConnection, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getOperatorOutputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through operator output port connections until we find the desired connection  
  my $allOperatorOutputPortConnectionInfo = $$output{connections};
  foreach my $nextOperatorOutputPortConnection (@$allOperatorOutputPortConnectionInfo) {
    my $id = $$nextOperatorOutputPortConnection{id};
    if ($id eq $operatorOutputPortConnection) {
      return(0, $nextOperatorOutputPortConnection);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator output port connection..
  my $msg = "Operator output port connection $operatorOutputPortConnection does not exist in operatorPort  $operatorOutputPort";
  return(1, $msg);
}


#######################################
# _getOperatorInputPortStatusInfo
#######################################
sub _getOperatorInputPortStatusInfo($$$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getOperatorStatusInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through operator input ports until we find the desired input port 
  my $allOperatorInputPortInfo = $$output{inputPorts};
  foreach my $nextOperatorInputPort (@$allOperatorInputPortInfo) {
    my $name = $$nextOperatorInputPort{name};
    if ($name eq $operatorInputPort) {    
      return(0, $nextOperatorInputPort);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator input port..
  my $msg = "Operator input port  $operatorInputPort does not exist in operator $operator";
  return(1, $msg);
}

#######################################
# _getOperatorInputPortConnectionStatusInfo
#######################################
sub _getOperatorInputPortConnectionStatusInfo($$$$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $operatorInputPortConnection, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getOperatorInputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through operator input port connections until we find the desired connection  
  my $allOperatorInputPortConnectionInfo = $$output{connections};
  foreach my $nextOperatorInputPortConnection (@$allOperatorInputPortConnectionInfo) {
    my $id = $$nextOperatorInputPortConnection{id};
    if ($id eq $operatorInputPortConnection) {
      return(0, $nextOperatorInputPortConnection);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator input port connection..
  my $msg = "Operator input port connection $operatorInputPortConnection does not exist in operatorPort  $operatorInputPort";
  return(1, $msg);
}


#######################################
# _getJobMetricsInfo
#######################################
sub _getJobMetricsInfo($$$$) {
  my ($domainName, $instanceName, $jobId, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getJobInfo($domainName, $instanceName, $jobId, $reloadCache);
    if ($rc) {
    return($rc, $output);
  }
  
  my $jobMetricsInfo = $$output{jobMetricsInfo};  
  return(0, $jobMetricsInfo);  
}

#######################################
# _getPEMetricsInfo
#######################################
sub _getPEMetricsInfo($$$$$) {
  my ($domainName, $instanceName, $jobId, $peId, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getJobMetricsInfo($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through PEs until we find the desired PE.  
  my $allPEInfo = $$output{pes};
  foreach my $nextPE (@$allPEInfo) {
    my $nextId = $$nextPE{id};
    if ($nextId == $peId) {
      return(0, $nextPE);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired PE.
  my $msg = "PE with ID $peId does not exist in job $jobId";
  return(1, $msg);
}

#######################################
# _getOperatorMetricsInfo
#######################################
sub _getOperatorMetricsInfo($$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $reloadCache) = (@_);
  
  my ($rc, $output) = _getPeIdFromOperator($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  } 
  my $peId = $output;
  $reloadCache = 0;  # no point in loading again here    
  
  my ($rc, $output) = _getPEMetricsInfo($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }

  # Loop through operator until we find the desired operator.  
  my $allOperatorInfo = $$output{operators};
  foreach my $nextOperator (@$allOperatorInfo) {
    my $nextName = $$nextOperator{name};
    if ($nextName eq $operator) {    
      return(0, $nextOperator);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator.
  my $msg = "Operator with name $operator does not exist in PE $peId";
  return(1, $msg);
}

#######################################
# _getOperatorOutputPortMetricsInfo
#######################################
sub _getOperatorOutputPortMetricsInfo($$$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getOperatorMetricsInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  # Output ports in metrics info are only given by index.
  # So we need to look up index in statusInfo
  my ($rc, $output2) = _getOperatorOutputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, 0);
  if ($rc) {
    return($rc, $output2);
  }
  my $outputPortIndex = $$output2{indexWithinOperator};

  # Loop through operator until we find the desired operator output port (by index).  
  my $allOperatorOutputPortInfo = $$output{outputPorts};
  foreach my $nextOperatorOutputPort (@$allOperatorOutputPortInfo) {
    my $nextIndex = $$nextOperatorOutputPort{indexWithinOperator};
    if ($nextIndex eq $outputPortIndex) {    
      return(0, $nextOperatorOutputPort);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator output port.
  my $msg = "Operator output port with name $operatorOutputPort does not exist in operator $operator";
  return(1, $msg);
}

#######################################
# _getOperatorInputPortMetricsInfo
#######################################
sub _getOperatorInputPortMetricsInfo($$$$$$) {
  my ($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache) = (@_);  
  
  my ($rc, $output) = _getOperatorMetricsInfo($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) {
    return($rc, $output);
  }
  
  # Input ports in metrics info are only given by index.
  # So we need to look up index in statusInfo
  my ($rc, $output2) = _getOperatorInputPortStatusInfo($domainName, $instanceName, $jobId, $operator, $operatorInputPort, 0);
  if ($rc) {
    return($rc, $output2);
  }
  my $inputPortIndex = $$output2{indexWithinOperator};

  # Loop through operator until we find the desired operator input port (by index).  
  my $allOperatorInputPortInfo = $$output{inputPorts};
  foreach my $nextOperatorInputPort (@$allOperatorInputPortInfo) {
    my $nextIndex = $$nextOperatorInputPort{indexWithinOperator};
    if ($nextIndex eq $inputPortIndex) {    
      return(0, $nextOperatorInputPort);
    }
  }
  
  # If we get here.  Error occurred -- cannot find the desired operator input port.
  my $msg = "Operator input port with name $operatorInputPort does not exist in operator $operator";
  return(1, $msg);
}


#######################################
# _searchJobCacheForIdByName
# see if there is already a job
# in the cache with the given name.
# Return the job ID.
# if it is not cached, then return -1;
#######################################
sub _searchJobCacheForIdByName($$$) {
  my ($domainName, $instanceName, $jobName) = (@_);
  
  if (defined($_cache{JOBCACHE})) {
    my $jobCache = $_cache{JOBCACHE};
   
    if (defined($$jobCache{$domainName})) {
      my $domain = $$jobCache{$domainName};
      
      if (defined($$domain{$instanceName})) {
        my $instance = $$domain{$instanceName};
        
        foreach my $nextJobId (keys %$instance) {
          my $nextJobInfo = $$instance{$nextJobId};
          
          my $nextJobStatusInfo = $$nextJobInfo{jobStatusInfo};
          
          my $nextJobName = $$nextJobStatusInfo{name};
          if ($nextJobName eq $jobName) {
            return($nextJobId);     
          }  
        }
      }
    }
  }
  
  # If we get to here we did not find the job we were looking for
  return(-1);
}

#######################################
# _loadSingleJobCacheByName
#######################################
sub _loadSingleJobCacheByName($$$) {
  my ($domainName, $instanceName, $jobName) = (@_);

  
  my $parms = "getSingleJobInfoByName -domain $domainName -instance $instanceName -jobName $jobName";
  my ($rc,$output) = _javaManagement($parms);
  if ($rc) {
    return(1, $output);
  }
  
  my $decodedJson = JSON::decode_json($output);   
  _addToDomainCache($domainName, $decodedJson);
  _addToInstanceCache($domainName, $instanceName, $decodedJson);
  _addToJobCache($domainName, $instanceName, $decodedJson);
    
  return(0, undef);
}


#######################################
# _runCommand
#######################################
sub _runCommand($) {
  my ($cmd) = (@_);
  
  my @results = `$cmd 2>&1`;
  my $rc = $?;

  my @results2;

  foreach my $nextResult (@results) {
    chomp $nextResult;
    push(@results2, $nextResult);
  }
  
  my $resultString = join("  ", @results2); 
  return($rc, $resultString);  
}





#######################################
# Internal routines end
#######################################

1;
