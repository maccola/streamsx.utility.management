#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Script that will check if a job that is assigned a particular job name is running.
# If the job is running, then verify it is healthy.
# If the job is not healthy, then cancel the job
# If the job is not running or was not healthy (and cancelled) then start the job
# This script could be set up as a cron job to monitor job status every so often.

use strict;

use FindBin;
use lib "$FindBin::Bin/..";
use streamsManagement;

use Getopt::Long;
use File::Basename;


sub main() {

  my $domain;
  my $instance;
  my $bundle;
  my $jobName;
  my $jobParms;

 my $rc = GetOptions(
             'domain|d=s'        => \$domain,
             'instance|i=s'      => \$instance,
             'bundle|b=s'        => \$bundle,
             'jobname|j=s'       => \$jobName,
             'P=s@'              => \$jobParms,
           );
           
                        

  if (!$rc) {
    usage();
    return(-1);
  }
       
  if (!defined($domain)) {
    if (defined($ENV{STREAMS_DOMAIN_ID})) {
      $domain = $ENV{STREAMS_DOMAIN_ID};
    }
    else {
      print("Must specify a domain\n");
      usage();
      return(1);
    }
  }
     
  if (!defined($instance)) {
    print("Must specify an instance\n");
    usage();
    return(1);
  }
         
  if (!defined($bundle)) {
    print("Must specify a bundle (.sab file)\n");
    usage();
    return(1);
  }
           
  if (!defined($jobName)) {
    print("Must specify a job name\n");
    usage();
    return(1);
  }
  
  if (defined($jobParms)) {
    foreach my $nextParm (@$jobParms) {
      if ($nextParm !~ /.*=.*/) {
        print("Parameters must be in format of parmName=parmValue\n");
        usage();
        return(1);
      }
    }
  }
  
  
  # Make sure the instance is running
  my ($rc, $output) = streamsManagement::getInstanceStatus($domain, $instance);
  if ($rc) { print("Error retrieving instance status:  $output\n"); return(1); }
  if ("running" ne $output) { print("Instance $instance does not have running status ($output)\n"); return(1); }
  
  # See if job with specified name is currently running
  my $jobId = -1;
  ($rc, $output) = streamsManagement::getJobIdByName($domain, $instance, $jobName);
  if ($rc) {
    # if an error occurs, look for the "Could not find job" error message
    # jobId will remain at -1 if we find this message
    if ($output !~ /Could not find job/) {
      print("Error looking for job with name $jobName: $output.\n");
      return(1);
    }
    else {
      print("Job with name $jobName is not currently running.\n");
    }
  }
  # else job is running
  else {
    $jobId = $output;
  }
    
  # if job is running, then check status of job
  my $healthy = 1;
  if (-1 != $jobId) {
    ($rc, $output) = streamsManagement::getJobStatus($domain, $instance, $jobId);
    print("Job ID $jobId status:  $output\n");
    if ($output ne "running") {
      $healthy = 0;
    }
  }
  
  # just to make sure, make sure all PEs are in running status
  if ((-1 != $jobId) && (0 != $healthy)) {
    ($rc, $output) = streamsManagement::checkIfAllPEsAreRunning($domain, $instance, $jobId);
    if ($output != 1) {
      print("Not all PEs are healthy.\n");
      $healthy = 0;
      last;
    }
  }
  
  # if job or PEs are unhealthy, then cancel the job
  if (!$healthy) {
    print("Cancelling unhealthy job...\n");
    ($rc, $output) = streamsManagement::cancelJob($domain, $instance, $jobId);
    if ($rc) { print("Error cancelling job $jobId:  $output\n"); return(1); }
    $jobId = -1;
  }
  
  # restart the job if needed
  if (-1 == $jobId) {
    print("Submitting application $bundle...\n");
    ($rc, $output) = streamsManagement::submitJob($domain, $instance, $bundle, $jobParms, undef, $jobName);
    if ($rc) { print("Error submitting job:  $output\n"); return(1); }
    print("Submitted job $output\n");
  }
  else {
    print("Job with name $jobName already running.  Job ID = $jobId\n");
  }

  return(0);
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -bundle bundleFile -jobname jobName [-P parmName1=parmValue1 -parmName2=parmValue2]\n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");
}


my $rc = main();
exit($rc);
