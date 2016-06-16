#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Script that will monitor any specified application metric for 
# a specified output port over intervals of time.
# By default, all metrics are captures, but caller
# can specify filters to reduce size of metric
# info collected.


use strict;

use File::Spec;
use FindBin;
use lib "$FindBin::Bin/..";
use streamsManagement;

use Getopt::Long;
use File::Basename;

my $thisDir = File::Spec->rel2abs(dirname($0));


###############################################
sub main() {

  my $domain;
  my $instance;
  my $jobId;
  my $jobName;
  my $interval;
  my $metricFilter;
  my $operatorFilter;
  my $peFilter;
  my $stopFile;
  my $outFile;
  my $initialDelay;
  my $testFilter;
  my $testString;

 my $rc = GetOptions(
             'domain|d=s'      => \$domain,
             'instance|i=s'    => \$instance,
             'jobid=i'         => \$jobId,             
             'jobname=s'       => \$jobName,
             'interval=i'      => \$interval,
             'metricFilter=s'  => \$metricFilter,
             'operatorFilter=s' => \$operatorFilter,
             'peFilter=s'       => \$peFilter,
             'stopFile=s'      => \$stopFile,
             'outFile=s'       => \$outFile,
             'initialDelay=i'  => \$initialDelay,
             'testFilter=s'    => \$testFilter,
             'testString=s'    => \$testString,
           );

  if (!$rc) {
    usage();
    return(-1);
  }
  
  if (defined($testFilter) || defined($testString)) {
    if (!defined($testFilter) || !defined($testString)) {
      print("Must specify -testFilter and -testString together.\n");
      usage();
      return(1);
    }
    testFilter($testFilter, $testString);
    return(0);
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
  
  # interval is optional
  # default is 30 seconds
  if (!defined($interval)) {
    $interval = 30;
  }
  
  # stopFile is optional
  # default is "./stopFile'
  if (!defined($stopFile)) {
    $stopFile = "./stopFile";
  }
  # make sure stopFile is absolute
  $stopFile = File::Spec->rel2abs($stopFile);
  
  if (!defined($outFile)) {
    print("Must specify an outFile\n");
    usage();
    return(1);
  }
  
  # initialDelay is optional
  if (!defined($initialDelay)) {
    $initialDelay = 0;
  }                      
 
  sleep($initialDelay) if $initialDelay > 0;
  
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
  
  # Create the stopFile if it doesn't exist.
  if (! -f $stopFile) {
    my $rc = system("touch $stopFile");
    die "Unable to create stopFile $stopFile" if $rc;
  }
  
  print("Retrieving data for job $jobId every $interval seconds.\n");
  print("Run the following command from another session to halt this program:\n");
  print("   rm $stopFile\n");
  
  my $rc = captureMetrics($domain, $instance, $jobId, $metricFilter, $operatorFilter, $peFilter, $interval, $stopFile, $outFile);
      
  return($rc);
}

###############################################
sub captureMetrics($$$$$$$$$) {
  my ($domain, $instance, $jobId, $metricFilter, $operatorFilter, $peFilter, $interval, $stopFile, $outFile) = (@_);
  
    
  #********************************************************************
  # First time, get list of all metrics.  Need to do this because
  # we can't be sure if they will always come back in the same order.
  # Also, keep track of which PEs we will be getting info for.
  # We only want to show the lastTimeRetrieve columns for the PEs
  # we are interested in.
  #********************************************************************
  my @metricInfo;
  my @observedPEs;
  my ($rc, $output) = streamsManagement::getJobPEs($domain, $instance, $jobId);
  if ($rc) {
    print("Error: $output\n");
    return($rc);
  }
  
  # Loop through each PE
  my $jobPEs = $output;
  foreach my $nextPE (@$jobPEs) {
  
    if (checkForPatternMatch($peFilter, $nextPE)) {
  
      my $observed = 0;
  
      # PE Metrics
      my ($rc, $peMetricNames) = streamsManagement::getPEMetricNames($domain, $instance, $jobId, $nextPE);
      foreach my $nextMetricName (@$peMetricNames) {
        if (checkForPatternMatch($metricFilter, $nextMetricName)) {
          my $metricString = "PE,$nextPE,$nextMetricName";
          push(@metricInfo, $metricString);
          $observed = 1;
        }
      }
    
      # Loop through each operator
      my ($rc, $operators) = streamsManagement::getPEOperators($domain, $instance, $jobId, $nextPE);
      foreach my $nextOperator (@$operators) {
    
        if (checkForPatternMatch($operatorFilter, $nextOperator)) {
      
          # get Operator Metrics
          my ($rc, $opMetricNames) = streamsManagement::getOperatorMetricNames($domain, $instance, $jobId, $nextOperator);
          foreach my $nextMetricName (@$opMetricNames) {
            if (checkForPatternMatch($metricFilter, $nextMetricName)) {
              my $metricString = "Operator,$nextPE,$nextOperator,$nextMetricName";
              push(@metricInfo, $metricString);
              $observed = 1;
            }
          }
      
          # Loop through each operator input port
          my ($rc, $inputPorts) = streamsManagement::getOperatorInputPorts($domain, $instance, $jobId, $nextOperator);
          foreach my $nextInputPort (@$inputPorts) {
      
            my ($rc, $inPortMetricNames) = streamsManagement::getOperatorInputPortMetricNames($domain, $instance, $jobId, $nextOperator, $nextInputPort);
            foreach my $nextInPortMetricName (@$inPortMetricNames) {
              if (checkForPatternMatch($metricFilter, $nextInPortMetricName)) {
                my $metricString = "InputPort,$nextPE,$nextOperator,$nextInputPort,$nextInPortMetricName";
                push(@metricInfo, $metricString);
                $observed = 1;
              }
            }        
          } # end input ports
      
          # Loop through each operator output port
          my ($rc, $outputPorts) = streamsManagement::getOperatorOutputPorts($domain, $instance, $jobId, $nextOperator);
          foreach my $nextOutputPort (@$outputPorts) {
      
            my ($rc, $outPortMetricNames) = streamsManagement::getOperatorOutputPortMetricNames($domain, $instance, $jobId, $nextOperator, $nextOutputPort);
            foreach my $nextOutPortMetricName (@$outPortMetricNames) {
              if (checkForPatternMatch($metricFilter, $nextOutPortMetricName)) {
                my $metricString = "OutputPort,$nextPE,$nextOperator,$nextOutputPort,$nextOutPortMetricName";
                push(@metricInfo, $metricString);
                $observed = 1;
              }
            }
          } # end output ports
        }  # end if operator matches filter
      }  # end operators
    
    
      if ($observed) {
        push(@observedPEs, $nextPE);
      }
    
    }  # end if PE matches pe filter
  }  # end each PE
  
  #********************************************************************
  # Print the headers using the metric info we just gathered
  #********************************************************************
  my $monFH;
  if (!open($monFH, '>', $outFile)) {
    pring("Could not open file $outFile\n");
    return(1);
  }
  
  for (my $i=0; $i < 4; $i++) {
    foreach my $nextPE (@observedPEs) {  
      if (0 == $i) {
        print($monFH "PE,");
      }
      elsif (1 == $i) {
         print($monFH "$nextPE,");
      }
      else {
        print($monFH ",");
      }
    }
    foreach my $nextMetric (@metricInfo) {
      my @ar = split /,/, $nextMetric;
      my $sz = @ar;
      if ($i < ($sz-1)) {
        print($monFH "$ar[$i],");
      }
      else {
        print($monFH ",");
      }
    }
    print($monFH "\n");
  }
  foreach my $nextPE (@observedPEs) {
    print($monFH "LastTimeRetrieved,");
  }  
  foreach my $nextMetric (@metricInfo) {
    my @ar = split /,/, $nextMetric;
    my $sz = @ar;
      print($monFH "$ar[$sz-1],");
  }
  print($monFH "\n");


  #********************************************************************
  # Now retrieve the metrics values until we see the stopfile removed...
  #********************************************************************
  my $lastTime;
  while(1) {
    foreach my $nextPE (@observedPEs) {      
      my ($rc, $output) = streamsManagement::getPELastTimeRetrievedMetrics($domain, $instance, $jobId, $nextPE);
      print($monFH "$output,");
    }
    
    foreach my $nextMetric (@metricInfo) {
      my @ar = split /,/, $nextMetric;
      
      if ($ar[0] eq "PE") {
        my ($rc, $value) = streamsManagement::getPEMetricValue($domain, $instance, $jobId, $ar[1], $ar[2]);
        print($monFH "$value,");
      }
      elsif ($ar[0] eq "Operator") {
        my ($rc, $value) = streamsManagement::getOperatorMetricValue($domain, $instance, $jobId, $ar[2], $ar[3]);
        print($monFH "$value,");
      }
      elsif ($ar[0] eq "InputPort") {
        my ($rc, $value) = streamsManagement::getOperatorInputPortMetricValue($domain, $instance, $jobId, $ar[2], $ar[3], $ar[4]);
        print($monFH "$value,");
      }
      elsif ($ar[0] eq "OutputPort") {
        my ($rc, $value) = streamsManagement::getOperatorOutputPortMetricValue($domain, $instance, $jobId, $ar[2], $ar[3], $ar[4]);
        print($monFH "$value,");
      }

    } 
    print($monFH "\n");
  
    if (checkStopFile($stopFile)) {
      my $curTime = time();
      my $sleepTime;
      if (defined($lastTime)) {
        $sleepTime = $interval - ($curTime - $lastTime);
      }
      else {
        $sleepTime = $interval;
      }
      sleep($sleepTime) if $sleepTime > 0;
      $lastTime = time();
    }
    
    if (!checkStopFile($stopFile)) {
      print("Stopfile $stopFile no longer exists.  Ending the metrics monitor.\n");
      last;
    }
    my ($rc, $output) = streamsManagement::loadSingleJobCache($domain, $instance, $jobId);
    if ($rc) {
      print("Error occurred retrieving job info.\n");
      return($rc);
    }    
  }
  
  close($monFH);
  return(0);
}

######################################################
sub checkForPatternMatch($$) {
  my ($pattern, $string) = (@_);
  return(1) if !defined($pattern);
  if ($string =~ /$pattern/) {
    return(1);
  }
  else {
    return(0);
  }
}

######################################################
# Return 1 if file exist, 0 if it doesn't
sub checkStopFile($) {
  my ($stopFile) = (@_);
  
  if (-f $stopFile) {
    return(1);
  }
  else {
    return(0);
  }
  
}
###############################################
sub testFilter($$) {
  my ($testFilter, $testString) = (@_);
  
  my $rc = checkForPatternMatch($testFilter, $testString);
  print("TEST STRING: $testString\n");
  print("TEST FILTER: $testFilter\n");
  if ($rc) {
    print("IS A MATCH.\n");
  }
  else {
    print("IS NOT A MATCH.\n");
  }
}
###############################################
sub usage() {
  my $pgm = basename($0);
  print("$pgm [-d <domain>] -i instance -jobid <jobID>|-jobname <jobName> -outFile <fileName> [-interval <intervalInSeconds>] [-metricFilter <filter>] [-operatorFilter <filter>] [-peFilter <filter>] [-stopFile <stopFile>]\n");
  print("  or\n");
  print("$pgm -testFilter <filter>  -testString <stringToTest.\n");
  print("  The -testFilter/-testString options are available to test filters because specifying these filters can be a little tricky.\n"); 
  print("  Environment variable STREAMS_DOMAIN_ID can be used in place of -d <domain>\n");  
}


my $rc = main();
exit($rc);
