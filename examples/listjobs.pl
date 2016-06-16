#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Simple program to list jobs in an instance

use strict;

use FindBin;
use lib "$FindBin::Bin/..";
use streamsManagement;

use Getopt::Long;
use File::Basename;


sub main() {

  my $domain;
  my $instance;

 my $rc = GetOptions(
             'domain|d=s'        => \$domain,
             'instance|i=s'      => \$instance,
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
  
  # load all jobs into the cache at once for better performance
  my ($rc, $errMsg) = streamsManagement::loadAllJobsCache($domain, $instance);
  if ($rc) { print("Error running loadAllJobsCache:  $errMsg\n"); return(1); }
  
  # get the list of jobs
  my ($rc, $output) = streamsManagement::getInstanceJobs($domain, $instance);
  
  # Loop through each job to get health
  my $jobIds = $output;
  printf("%10s %10s %10s %10s\n", "JOBID", "STATUS", "HEALTH", "NAME");
  foreach my $nextJobId (@$jobIds) {
    my $status;
    my $health;
    my $name;
    ($rc, $status) = streamsManagement::getJobStatus($domain, $instance, $nextJobId);
    ($rc, $health) = streamsManagement::getJobHealth($domain, $instance, $nextJobId);
    ($rc, $name) = streamsManagement::getJobName($domain, $instance, $nextJobId);
    printf("%10s %10s %10s %10s\n", $nextJobId, $status, $health, $name);  
  }

  return(0);
}

sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance\n");
  print("Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");    
}


my $rc = main();
exit($rc);
