#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Script that will monitor the nTupleSubmitted metric for 
# a specified output port over intervals of time


use strict;

use FindBin;
use lib "$FindBin::Bin/..";
use streamsManagement;

use Getopt::Long;
use File::Basename;



# How often to check the metric (in seconds)
my $INTERVAL = 20.0;


sub main() {

  my $domain;
  my $instance;
  my $jobId;
  my $jobName;
  my $operator;
  my $outputPort;

 my $rc = GetOptions(
             'domain|d=s'        => \$domain,
             'instance|i=s'      => \$instance,
             'jobid=i'           => \$jobId,             
             'jobname=s'         => \$jobName,
             'operator=s'        => \$operator,
             'outputport=s'      => \$outputPort,
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
  
   if (!defined($operator)) {
    print("Must specify an operator\n");
    usage();
    return(1);
  }
  
  if (!defined($outputPort)) {
    print("Must specify an operator output port\n");
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
  
  
  # Find the PE that the operator is contained in
  my ($rc, $output) = streamsManagement::getPEIdFromOperatorName($domain, $instance, $jobId, $operator);
  if ($rc) { print("$output\n"); return(1); }
  my $peId = $output;
  
  # Get in a never ending loop to monitor
  my $first = 1;
  my $lastTime;
  while (1) {
    my ($rc, $output) = streamsManagement::getOperatorOutputPortMetricValue($domain, $instance, $jobId, $operator, $outputPort, "nTuplesSubmitted");
    if ($rc) { print("$output\n"); return(1); }
    my $numTuples = $output;
    
    my ($rc, $output) = streamsManagement::getPELastTimeRetrievedMetrics($domain, $instance, $jobId, $peId);
    if ($rc) { print("$output\n"); return(1); }
    my $metricTime = $output;
    if ($first) {
      print("Timestamp,nTuplesSubmitted\n");
      $first = 0;
    }
    print("$metricTime,$numTuples\n");
    my $curTime = time();
    my $sleepTime;
    if (defined($lastTime)) { $sleepTime = $INTERVAL - ($curTime - $lastTime);}
    else { $sleepTime = $INTERVAL;}
    if ($sleepTime > 0) {
      sleep($sleepTime);
    }
    
    #print ("curTime:  $curTime,  lastTime:  $lastTime,  sleepTime:  $sleepTime\n");

    $lastTime = time();
    my ($rc, $output) = streamsManagement::loadSingleJobCache($domain, $instance, $jobId);
    if ($rc) { print("$output\n"); return(1); } 
  }
      
  return(0);
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -jobid <jobID>|-jobname <jobName> -operator <operatorName> -outputport <operatorOutputPort>\n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");  
}


my $rc = main();
exit($rc);
