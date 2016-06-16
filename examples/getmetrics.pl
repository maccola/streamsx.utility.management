#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Simple program to retrieve all available metrics in a job.
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
    
  # get the list of PEs for a job
  my ($rc, $output) = streamsManagement::getJobPEs($domain, $instance, $jobId);
  if ($rc) { print("Error retrieving PE lists:  $output\n"); return(1);}
  
  # Loop through each PE
  my $jobIds = $output;
  foreach my $nextPE (@$jobIds) {
  
    print("PE $nextPE:\n");
    
    # get PE Metrics
    my ($rc, $peMetricNames) = streamsManagement::getPEMetricNames($domain, $instance, $jobId, $nextPE);
    tab();  print("PE Metrics:\n");
    foreach my $nextMetricName (@$peMetricNames) {
      my ($rc, $value) = streamsManagement::getPEMetricValue($domain, $instance, $jobId, $nextPE, $nextMetricName);
      tab(); tab(); print("$nextMetricName:  $value\n");
    }
    
    # Loop through each operator
    my ($rc, $operators) = streamsManagement::getPEOperators($domain, $instance, $jobId, $nextPE);
    foreach my $nextOperator (@$operators) {
      tab(); print("Operator $nextOperator:\n");
    
      # get Operator Metrics
      my ($rc, $opMetricNames) = streamsManagement::getOperatorMetricNames($domain, $instance, $jobId, $nextOperator);
      tab();  tab(); print("Operator Metrics:\n");
      foreach my $nextMetricName (@$opMetricNames) {
        my ($rc, $value) = streamsManagement::getOperatorMetricValue($domain, $instance, $jobId, $nextOperator, $nextMetricName);
        tab(); tab(); tab(); print("$nextMetricName:  $value\n");
      }
      
      # Loop through each operator input port
      my ($rc, $inputPorts) = streamsManagement::getOperatorInputPorts($domain, $instance, $jobId, $nextOperator);
      foreach my $nextInputPort (@$inputPorts) {
        tab(); tab(); print("Operator Input Port $nextInputPort:\n");
            
        # get Input Port Metrics
        my ($rc, $inputPortMetricNames) = streamsManagement::getOperatorInputPortMetricNames($domain, $instance, $jobId, $nextOperator, $nextInputPort);
        tab();  tab(); tab(); print("Operator Input Port Metrics:\n");
        foreach my $nextMetricName (@$inputPortMetricNames) {
          my ($rc, $value) = streamsManagement::getOperatorInputPortMetricValue($domain, $instance, $jobId, $nextOperator, $nextInputPort, $nextMetricName);
          tab(); tab(); tab(); tab(); print("$nextMetricName:  $value\n");
        }                        
      }
                  
      # Loop through each operator output port
      my ($rc, $outputPorts) = streamsManagement::getOperatorOutputPorts($domain, $instance, $jobId, $nextOperator);
      foreach my $nextOutputPort (@$outputPorts) {
        tab(); tab(); print("Operator Output Port $nextOutputPort:\n");
            
        # get Output Port Metrics
        my ($rc, $outputPortMetricNames) = streamsManagement::getOperatorOutputPortMetricNames($domain, $instance, $jobId, $nextOperator, $nextOutputPort);
        tab();  tab(); tab(); print("Operator Output Port Metrics:\n");
        foreach my $nextMetricName (@$outputPortMetricNames) {
          my ($rc, $value) = streamsManagement::getOperatorOutputPortMetricValue($domain, $instance, $jobId, $nextOperator, $nextOutputPort, $nextMetricName);
          tab(); tab(); tab(); tab(); print("$nextMetricName:  $value\n");
        }                        
      }  
           
    }
  }

  return(0);
}

sub tab() {
  print("   ");
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -jobid <jobID>|-jobname <jobName> \n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");    
}


my $rc = main();
exit($rc);
