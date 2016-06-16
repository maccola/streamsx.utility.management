#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Script that will find all source operators AND all import
# operators and will report the number of tuples submitted
# (taken from nTuplesSubmitted metric)
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
             'jobname=s'       => \$jobName,
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
  
  # First find all source operators
  # Source operators defined as not having any input ports
  # Report tuples submitted for all 
  my ($rc, $output) = streamsManagement::getJobOperators($domain, $instance, $jobId);
  if ($rc) { print("Error retrieving Operators: $output\n"); return(1);}
  my $operators = $output;    
  foreach my $nextOperator (@$operators) {
    my ($rc, $inputPorts) = streamsManagement::getOperatorInputPorts($domain, $instance, $jobId, $nextOperator);
    my $arraySize = scalar @$inputPorts;
    # 0 input ports means it is a source operator
    if (0 == $arraySize) {
      my ($rc, $outputPorts) = streamsManagement::getOperatorOutputPorts($domain, $instance, $jobId, $nextOperator);
      foreach my $nextOutputPort (@$outputPorts) {
        my ($rc, $numTuples) = streamsManagement::getOperatorOutputPortMetricValue($domain, $instance, $jobId, $nextOperator, $nextOutputPort, "nTuplesSubmitted");
        print("SOURCE   job:  $jobId,  Operator:  $nextOperator  OutputPort:  $nextOutputPort  nTuplesSubmitted:  $numTuples\n");
      }
    } 
  }
  
  # Next, find all operators attached to an Import operator
  # Report tuples submitted for all 
  my ($rc, $operators) = streamsManagement::getJobOperators($domain, $instance, $jobId);
  foreach my $nextOperator (@$operators) {
    my ($rc, $inputPorts) = streamsManagement::getOperatorInputPorts($domain, $instance, $jobId, $nextOperator);
    foreach my $nextInputPort (@$inputPorts) {
      my ($rc, $isImport) = streamsManagement::checkIfOperatorInputPortHasImport($domain, $instance, $jobId, $nextOperator, $nextInputPort);
      if ($isImport) {
        my ($rc, $outputPorts) = streamsManagement::getOperatorOutputPorts($domain, $instance, $jobId, $nextOperator);
        foreach my $nextOutputPort (@$outputPorts) {
          my ($rc, $numTuples) = streamsManagement::getOperatorOutputPortMetricValue($domain, $instance, $jobId, $nextOperator, $nextOutputPort, "nTuplesSubmitted");
          print("IMPORT   job:  $jobId,  Operator:  $nextOperator  OutputPort:  $nextOutputPort  nTuplesSubmitted:  $numTuples\n");
        }
      }  
    }  
  
  }
  

  return(0);
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -jobid <jobID>|-jobname <jobName> \n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");  
}


my $rc = main();
exit($rc);
