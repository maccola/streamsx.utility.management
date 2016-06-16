#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Script that will cancel a job given either a job's name
# or its job ID.

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
             'jobname|j=s'       => \$jobName,            
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
  
  # Make sure the instance is running
  my ($rc, $output) = streamsManagement::getInstanceStatus($domain, $instance);
  if ($rc) { print("Error retrieving instance status:  $output\n"); return(1); }
  if ("running" ne $output) { print("Instance $instance does not have running status ($output)\n"); return(1); }
  
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
  
  # Cancel the job
  print("Cancelling job $jobId...\n");
  ($rc, $output) = streamsManagement::cancelJob($domain, $instance, $jobId);
  if ($rc) { print("Error cancelling job $jobId:  $output\n"); return(1); }
  print("Job cancelled.\n");

  return(0);
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -jobid <jobID>|-jobname <jobName> \n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");
}


my $rc = main();
exit($rc);
