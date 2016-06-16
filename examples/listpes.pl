#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Simple program to list PEs in a job
# Job name or Job ID is taken as input

use strict;

use FindBin;
use lib "$FindBin::Bin/..";
use streamsManagement;

use Getopt::Long;
use File::Basename;


sub main() {

  my $domain;
  my $instance;
  my $jobId;
  my $jobName;

 my $rc = GetOptions(
             'domain|d=s'        => \$domain,
             'instance|i=s'      => \$instance,
             'jobid=i'      => \$jobId,
             'jobname=s'    => \$jobName,
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
       
  if ((!defined($jobId)) && (!defined($jobName))) {
    print("Must specify a job id or a job name\n");
    usage();
    return(1);
  }
         
  if ((defined($jobId)) && (defined($jobName))) {
    print("Cannot specify both a job id or a job name\n");
    usage();
    return(1);
  }  
  
  # Look up job ID if job name was specified.
  if (defined($jobName)) {
    my ($rc, $output) = streamsManagement::getJobIdByName($domain, $instance, $jobName);
    if ($rc) {
      # if an error occurs, look for the "Could not find job" error message
      if ($output !~ /Could not find job/) {
        print("Error looking for job with name $jobName: $output.\n");
        return(1);
      }
      else {
        print("Job with name $jobName is not currently running.\n");
        return(1);
      }
    }
    else  {
      $jobId = $output;
    }
  }    
  
  # get the list of PEs
  my ($rc, $output) = streamsManagement::getJobPEs($domain, $instance, $jobId);
  if ($rc) { print("Error running getJobPEs:  $output\n"); }
  
  # Loop through each PE to get health, resource, PID
  my $peIds = $output;
  printf("%10s %10s %10s %10s %10s \n", "PEID", "STATUS", "HEALTH", "PID", "RESOURCE");
  foreach my $nextPeId (@$peIds) {
  
    my $status;
    my $health;
    my $resource;
    my $pid;
        
    ($rc, $output) = streamsManagement::getPEStatus($domain, $instance, $jobId, $nextPeId);
    if ($rc) { print("Error running getPEStatus:  $output\n"); return(1);}
    else { $status = $output }    
    
    ($rc, $output) = streamsManagement::getPEHealth($domain, $instance, $jobId, $nextPeId);
    if ($rc) { print("Error running getPEHealth:  $output\n"); return(1);}
    else { $health = $output }
    
    ($rc, $output) = streamsManagement::getPEResource($domain, $instance, $jobId, $nextPeId);
    if ($rc) { print("Error running getPEResource:  $output\n"); return(1);}
    else { $resource = $output }
    
    ($rc, $output) = streamsManagement::getPEPid($domain, $instance, $jobId, $nextPeId);
    if ($rc) { print("Error running getPEPid:  $output\n"); return(1);}
    else { $pid = $output }    
    
    printf("%10s %10s %10s %10s %10s \n", $nextPeId, $status, $health, $pid, $resource);    
  }
  
  

  return(0);
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -jobid <jobID>|-jobname <jobName>\n\n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");    
}


my $rc = main();
exit($rc);
