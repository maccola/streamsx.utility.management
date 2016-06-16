#!/usr/bin/perl

#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************

# Test driver for testing all management routines

use strict;

use FindBin;
use lib "$FindBin::Bin/..";
use streamsManagement;

use Switch;
use File::Basename;


# globals
my $colCount;

sub main {

  mainMenu();  
  return 0;
}

sub mainMenu() {
  my $choice = -1;
  while (999 != $choice) {
    print("\nMain Menu:\n");
    print("-----------------\n");
    $colCount = 0;
    printChoice(2, "clearCache");
    printChoice(3, "loadDomainCache");
    printChoice(4, "loadInstanceCache");
    printChoice(5, "loadSingleJobCache");
    printChoice(6, "loadAllJobsCache");
    printChoice(11, "getDomainStatus");
    printChoice(12, "getDomainInstances");
    printChoice(21, "getInstanceStatus");
    printChoice(22, "getInstanceJobs");
    printChoice(41, "getJobStatusInfoJSONString");
    printChoice(42, "getJobMetricInfoJSONString");
    printChoice(43, "getJobStatus");     
    printChoice(44, "getJobHealth");
    printChoice(45, "getJobPEs");
    printChoice(46, "getJobName");
    printChoice(47, "getJobIdByName");
    printChoice(48, "getJobOperators");
    printChoice(61, "getPEStatus");
    printChoice(62, "checkIfAllPEsAreRunning");    
    printChoice(63, "getPEHealth");
    printChoice(64, "getPEPid");
    printChoice(65, "getPEResource");
    printChoice(66, "getPEOperators");
    printChoice(67, "getPEIdFromOperatorName");
    printChoice(71, "getOperatorOutputPorts");   
    printChoice(72, "getOperatorOutputPortConnections");
    printChoice(73, "getOperatorOutputPortConnectionSourceJob");
    printChoice(74, "getOperatorOutputPortConnectionSourcePE");
    printChoice(75, "getOperatorOutputPortConnectionSourceOperator");
    printChoice(76, "getOperatorOutputPortConnectionTargetJob");
    printChoice(77, "getOperatorOutputPortConnectionTargetPE");
    printChoice(78, "getOperatorOutputPortConnectionTargetOperator");
    printChoice(79, "checkIfOperatorOutputPortHasExport");
    printChoice(80, "getOperatorOutputPortExportOperatorName");
    printChoice(81, "getOperatorInputPorts"); 
    printChoice(82, "getOperatorInputPortConnections");
    printChoice(83, "getOperatorInputPortConnectionSourceJob");
    printChoice(84, "getOperatorInputPortConnectionSourcePE");
    printChoice(85, "getOperatorInputPortConnectionSourceOperator");
    printChoice(86, "getOperatorInputPortConnectionTargetJob");
    printChoice(87, "getOperatorInputPortConnectionTargetPE");
    printChoice(88, "getOperatorInputPortConnectionTargetOperator");    
    printChoice(89, "checkIfOperatorInputPortHasImport");
    printChoice(90, "getOperatorInputPortImportOperatorName");
    printChoice(91, "getPELastTimeRetrievedMetrics");
    printChoice(92, "getPEMetricNames");
    printChoice(93, "getPEMetricValue");
    printChoice(94, "getOperatorMetricNames");
    printChoice(95, "getOperatorMetricValue");
    printChoice(96, "getOperatorOutputPortMetricNames");
    printChoice(97, "getOperatorOutputPortMetricValue");
    printChoice(98, "getOperatorInputPortMetricNames");
    printChoice(99, "getOperatorInputPortMetricValue");
    printChoice(101, "submitJob");
    printChoice(102, "cancelJob");
    printChoice(103, "addDomainHost");
    printChoice(104, "removeDomainHost");
    printChoice(105, "getDomainHosts");
    printChoice(106, "addTagToHost");
    printChoice(107, "removeTagFromHost");
    printChoice(108, "getHostTags");
    printChoice(109, "makeInstance");
    printChoice(110, "removeInstance");
    printChoice(111, "startInstance");
    printChoice(112, "stopInstance");
    printChoice(113, "makeDomain");
    printChoice(114, "genKey");
    printChoice(115, "removeDomain");
    printChoice(116, "startDomain");
    printChoice(117, "stopDomain");
    printChoice(121, "getJobLogs");
    printChoice(122, "getDomainLogs");               
    printChoice(998, "dumpCache");     
    printChoice(999, "Exit");
    print("-----------------\n");
    print("Enter choice:  ");
    $choice = readSTDIN();

    switch($choice) {

      case 2  { clearCache()}
      case 3  { loadDomainCache()}
      case 4  { loadInstanceCache()}
      case 5  { loadSingleJobCache()}
      case 6  { loadAllJobsCache()}
      case 11 { getDomainStatus()}
      case 12 { getDomainInstances()}
      case 21 { getInstanceStatus()}
      case 22 { getInstanceJobs()}
      case 41 { getJobStatusInfoJSONString()}
      case 42 { getJobMetricInfoJSONString()}
      case 43 { getJobStatus()}      
      case 44 { getJobHealth()}
      case 45 { getJobPEs()}
      case 46 { getJobName()}
      case 47 { getJobIdByName()}
      case 48 { getJobOperators()}
      case 61 { getPEStatus()}
      case 62 { checkIfAllPEsAreRunning() }      
      case 63 { getPEHealth()}
      case 64 { getPEPid()}
      case 65 { getPEResource()} 
      case 66 { getPEOperators()}
      case 67 { getPEIdFromOperatorName()}
      case 71 { getOperatorOutputPorts()}
      case 72 { getOperatorOutputPortConnections()}
      case 73 { getOperatorOutputPortConnectionSourceJob()}
      case 74 { getOperatorOutputPortConnectionSourcePE()}
      case 75 { getOperatorOutputPortConnectionSourceOperator()}
      case 76 { getOperatorOutputPortConnectionTargetJob()}
      case 77 { getOperatorOutputPortConnectionTargetPE()}
      case 78 { getOperatorOutputPortConnectionTargetOperator()}
      case 79 { checkIfOperatorOutputPortHasExport()}
      case 80 { getOperatorOutputPortExportOperatorName()}
      case 81 { getOperatorInputPorts()}
      case 82 { getOperatorInputPortConnections()}
      case 83 { getOperatorInputPortConnectionSourceJob()}
      case 84 { getOperatorInputPortConnectionSourcePE()}
      case 85 { getOperatorInputPortConnectionSourceOperator()}
      case 86 { getOperatorInputPortConnectionTargetJob()}
      case 87 { getOperatorInputPortConnectionTargetPE()}
      case 88 { getOperatorInputPortConnectionTargetOperator()}      
      case 89 { checkIfOperatorInputPortHasImport()}
      case 90 { getOperatorInputPortImportOperatorName()}
      case 91 { getPELastTimeRetrievedMetrics()};      
      case 92 { getPEMetricNames()}
      case 93 { getPEMetricValue()}
      case 94 { getOperatorMetricNames()}
      case 95 { getOperatorMetricValue()}  
      case 96 { getOperatorOutputPortMetricNames()}
      case 97 { getOperatorOutputPortMetricValue()}
      case 98 { getOperatorInputPortMetricNames()}
      case 99 { getOperatorInputPortMetricValue()}
      case 101 { submitJob()}
      case 102 { cancelJob()}
      case 103 { addDomainHost()}
      case 104 { removeDomainHost()}
      case 105 { getDomainHosts()}
      case 106 { addTagToHost()}
      case 107 { removeTagFromHost()}
      case 108 { getHostTags()}
      case 109 { makeInstance()}
      case 110 { removeInstance()}
      case 111 { startInstance()}
      case 112 { stopInstance()}
      case 113 { makeDomain()}
      case 114 { genKey()}
      case 115 { removeDomain()}
      case 116 { startDomain()}
      case 117 { stopDomain()}
      case 121 { getJobLogs()}
      case 122 { getDomainLogs()}                    
      case 998 { dumpCache()}               
      case 999 {}
      else { print("Invalid choice ($choice) try again\n");}
    }  # end switch
  } # end while
}

################################
# printChoice
# print a column
################################
sub printChoice($$) {
  my ($num, $text) = (@_);
  my $numColumns = 2;
  printf("%-2d.  %-40s", $num, $text);
  if (0 == ++$colCount % $numColumns) {
    print("\n");
  }
}


################################
# Test clearCache
################################
sub clearCache() {
  streamsManagement::clearCache();
}

################################
# Test loadDomainCache
################################
sub loadDomainCache() {
  my $domainName = promptDomain();
  my ($rc, $errMsg) = streamsManagement::loadDomainCache($domainName);
  if ($rc) { print("Error running loadDomainCache:  $errMsg\n"); }
}

################################
# Test loadInstanceCache
################################
sub loadInstanceCache() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();
  my ($rc, $errMsg) = streamsManagement::loadInstanceCache($domainName, $instanceName);
  if ($rc) { print("Error running loadInstanceCache:  $errMsg\n"); }
}

################################
# Test loadSingleJobCache
################################
sub loadSingleJobCache() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();
  my $jobId = promptJobId();
  my ($rc, $errMsg) = streamsManagement::loadSingleJobCache($domainName, $instanceName, $jobId);
  if ($rc) { print("Error running loadSingleJobCache:  $errMsg\n"); }
}


################################
# Test loadAllJobsCache
################################
sub loadAllJobsCache() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();
  my ($rc, $errMsg) = streamsManagement::loadAllJobsCache($domainName, $instanceName);
  if ($rc) { print("Error running loadAllJobsCache:  $errMsg\n"); }
}

################################
# Test getDomainStatus
################################
sub getDomainStatus() {
  my $domainName = promptDomain();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getDomainStatus($domainName, $reloadCache);
  if ($rc) { print("Error running getDomainStatus:  $output\n"); }
  else { print("Domain status:  $output\n"); }
}

################################
# Test getDomainInstances
################################
sub getDomainInstances() {
  my $domainName = promptDomain();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getDomainInstances($domainName, $reloadCache);
  if ($rc) { print("Error running getDomainInstances:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getInstanceStatus
################################
sub getInstanceStatus() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();
  my $reloadCache = promptReloadCache();  
  my ($rc, $output) = streamsManagement::getInstanceStatus($domainName, $instanceName, $reloadCache);
  if ($rc) { print("Error running getInstanceStatus:  $output\n"); }
  else { print("Instance status:  $output\n"); }  
}

################################
# Test getInstanceJobs
################################
sub getInstanceJobs() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getInstanceJobs($domainName, $instanceName, $reloadCache);
  if ($rc) { print("Error running getInstanceJobs:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getJobStatusInfoJSONString
################################
sub getJobStatusInfoJSONString() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobStatusInfoJSONString($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobInfoJSONString:  $output\n"); }
  else { print("Job Status Info JSON:  $output\n"); } 
}

################################
# Test getJobMetricInfoJSONString
################################
sub getJobMetricInfoJSONString() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobMetricInfoJSONString($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobInfoJSONString:  $output\n"); }
  else { print("Job Metric Info JSON:  $output\n"); } 
}

################################
# Test getJobStatus
################################
sub getJobStatus() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobStatus($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobStatus:  $output\n"); }
  else { print("Job Status:  $output\n"); } 
}

################################
# Test getJobHealth
################################
sub getJobHealth() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobHealth($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobHealth:  $output\n"); }
  else { print("Job Health:  $output\n"); } 
}

################################
# Test getJobPEs
################################
sub getJobPEs() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobPEs($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobPEs:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getJobName
################################
sub getJobName() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobName($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobName:  $output\n"); }
  else { print("Job Name:  $output\n"); } 
}

################################
# Test getJobIdByName
################################
sub getJobIdByName() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobName = promptJobName();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobIdByName($domainName, $instanceName, $jobName, $reloadCache);
  if ($rc) { print("Error running getJobIdByName:  $output\n"); }
  else { print("Job ID:  $output\n"); } 
}

################################
# Test getJobOperators
################################
sub getJobOperators() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getJobOperators($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) { print("Error running getJobOperators:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getPEStatus
################################
sub getPEStatus() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEStatus($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPEStatus:  $output\n"); }
  else { print("PE Status:  $output\n"); } 
}

################################
# Test checkIfAllPEsAreRunning
################################
sub checkIfAllPEsAreRunning() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::checkIfAllPEsAreRunning($domainName, $instanceName, $jobId, $reloadCache);
  if ($rc) {
    print("Error running checkIfAllPEsAreRunning:  $output\n"); 
  }
  else { 
    if ($output == 1) {
      print("YES, all PEs are running.\n");
    }
    else {
      print("NO, all PEs are NOT running.\n");
    }
  } 
}

################################
# Test getPEHealth
################################
sub getPEHealth() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEHealth($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPEHealth:  $output\n"); }
  else { print("PE Health:  $output\n"); } 
}

################################
# Test getPEPid
################################
sub getPEPid() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEPid($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPEPid:  $output\n"); }
  else { print("PE Pid:  $output\n"); } 
}

################################
# Test getPEResource
################################
sub getPEResource() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEResource($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPEResource:  $output\n"); }
  else { print("PE Pid:  $output\n"); } 
}

################################
# Test getPEOperators
################################
sub getPEOperators() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEOperators($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPEOperators:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getPEIdFromOperatorName
################################
sub getPEIdFromOperatorName() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEIdFromOperatorName($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) { print("Error running getPEIdFromOperatorName:  $output\n"); }
  else { print("PE ID:  $output\n"); } 
}

################################
# Test getOperatorOutputPorts
################################
sub getOperatorOutputPorts() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPorts($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPorts:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorOutputPortConnections
################################
sub getOperatorOutputPortConnections() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnections($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPorts:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorOutputPortConnectionSourceJob
################################
sub getOperatorOutputPortConnectionSourceJob() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $operatorOutputPortConnection = promptOperatorOutputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnectionSourceJob($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $operatorOutputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortSourceJob:  $output\n"); }
  else { print("Operator output port connection source job:  $output\n"); } 
}

################################
# Test getOperatorOutputPortConnectionSourcePE
################################
sub getOperatorOutputPortConnectionSourcePE() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $operatorOutputPortConnection = promptOperatorOutputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnectionSourcePE($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $operatorOutputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortSourcePE:  $output\n"); }
  else { print("Operator output port connection source PE:  $output\n"); } 
}

################################
# Test getOperatorOutputPortConnectionSourceOperator
################################
sub getOperatorOutputPortConnectionSourceOperator() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $operatorOutputPortConnection = promptOperatorOutputPortConnection();
  my $reloadCache = promptReloadCache();
  
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnectionSourceOperator($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $operatorOutputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortSourceOperator:  $output\n"); }
  else { print("Operator output port connection source operator:  $output\n"); } 
}

################################
# Test getOperatorOutputPortConnectionTargetJob
################################
sub getOperatorOutputPortConnectionTargetJob() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $operatorOutputPortConnection = promptOperatorOutputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnectionTargetJob($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $operatorOutputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortTargetJob:  $output\n"); }
  else { print("Operator output port connection target job:  $output\n"); } 
}

################################
# Test getOperatorOutputPortConnectionTargetPE
################################
sub getOperatorOutputPortConnectionTargetPE() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $operatorOutputPortConnection = promptOperatorOutputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnectionTargetPE($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $operatorOutputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortTargetPE:  $output\n"); }
  else { print("Operator output port connection target pe:  $output\n"); } 
}

################################
# Test getOperatorOutputPortConnectionTargetOperator
################################
sub getOperatorOutputPortConnectionTargetOperator() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();  
  my $operatorOutputPortConnection = promptOperatorOutputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortConnectionTargetOperator($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $operatorOutputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortTargetOperator:  $output\n"); }
  else { print("Operator output port connection target operator:  $output\n"); } 
}

################################
# Test checkIfOperatorOutputPortHasExport
################################
sub checkIfOperatorOutputPortHasExport() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::checkIfOperatorOutputPortHasExport($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $reloadCache);
  if ($rc) { print("Error running checkIfOperatorOutputPortHasExport:  $output\n"); }
  else {
    if ($output) {
      print("Yes, output port has export.\n");
    }
    else {
      print("No, output port does not have export.\n");    
    } 
  }   
}

################################
# Test getOperatorOutputPortExportOperatorName
################################
sub getOperatorOutputPortExportOperatorName() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortExportOperatorName($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorOutputPort, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortExportOperatorName:  $output\n"); }
  else { print("Operator output port export operator name:  $output\n"); } 
}

################################
# Test getOperatorInputPorts
################################
sub getOperatorInputPorts() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPorts($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPorts:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorInputPortConnections
################################
sub getOperatorInputPortConnections() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnections($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPort connections:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorInputPortConnectionSourceJob
################################
sub getOperatorInputPortConnectionSourceJob() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $operatorInputPortConnection = promptOperatorInputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnectionSourceJob($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $operatorInputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortSourceJob:  $output\n"); }
  else { print("Operator input port connection source job:  $output\n"); } 
}

################################
# Test getOperatorInputPortConnectionSourcePE
################################
sub getOperatorInputPortConnectionSourcePE() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorOutputPort();  
  my $operatorInputPortConnection = promptOperatorInputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnectionSourcePE($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $operatorInputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortSourcePE:  $output\n"); }
  else { print("Operator Input port connection source PE:  $output\n"); } 
}

################################
# Test getOperatorInputPortConnectionSourceOperator
################################
sub getOperatorInputPortConnectionSourceOperator() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $operatorInputPortConnection = promptOperatorInputPortConnection();
  my $reloadCache = promptReloadCache();
  
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnectionSourceOperator($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $operatorInputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortSourceOperator:  $output\n"); }
  else { print("Operator input port connection source operator:  $output\n"); } 
}

################################
# Test getOperatorInputPortConnectionTargetJob
################################
sub getOperatorInputPortConnectionTargetJob() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $operatorInputPortConnection = promptOperatorInputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnectionTargetJob($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $operatorInputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortTargetJob:  $output\n"); }
  else { print("Operator input port connection target job:  $output\n"); } 
}

################################
# Test getOperatorInputPortConnectionTargetPE
################################
sub getOperatorInputPortConnectionTargetPE() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $operatorInputPortConnection = promptOperatorInputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnectionTargetPE($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $operatorInputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortTargetPE:  $output\n"); }
  else { print("Operator input port connection target pe:  $output\n"); } 
}

################################
# Test getOperatorInputPortConnectionTargetOperator
################################
sub getOperatorInputPortConnectionTargetOperator() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $operatorInputPortConnection = promptOperatorInputPortConnection();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortConnectionTargetOperator($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $operatorInputPortConnection, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortTargetOperator:  $output\n"); }
  else { print("Operator input port connection target operator:  $output\n"); } 
}

################################
# Test checkIfOperatorInputPortHasImport
################################
sub checkIfOperatorInputPortHasImport() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::checkIfOperatorInputPortHasImport($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) { print("Error running checkIfOperatorInputPortHasImport:  $output\n"); }
  else {
    if ($output) {
      print("Yes, input port has import.\n");
    }
    else {
      print("No, input port does not have import.\n");    
    } 
  }   
}

################################
# Test getOperatorInputPortImportOperatorName
################################
sub getOperatorInputPortImportOperatorName() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortImportOperatorName($domainName, $instanceName, $jobId, $operator, 
                                                                               $operatorInputPort, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortImportOperatorName:  $output\n"); }
  else { print("Operator input port import operator name:  $output\n"); } 
}

################################
# Test getPELastTimeRetrievedMetrics
################################
sub getPELastTimeRetrievedMetrics() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPELastTimeRetrievedMetrics($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPELastTimeRetrievedMetrics:  $output\n"); }
  else { print("timestamp last retrieved:  $output\n"); } 
}

################################
# Test getPEMetricNames
################################
sub getPEMetricNames() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEMetricNames($domainName, $instanceName, $jobId, $peId, $reloadCache);
  if ($rc) { print("Error running getPEMetricNames:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getPEMetricValue
################################
sub getPEMetricValue() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $peId = promptPEId();
  my $metricName = promptMetricName();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getPEMetricValue($domainName, $instanceName, $jobId, $peId, $metricName, $reloadCache);
  if ($rc) { print("Error running getPEMetricName:  $output\n"); }
  else { print("PE Metric value:  $output\n"); } 
}

################################
# Test getOperatorMetricNames
################################
sub getOperatorMetricNames() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();  
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorMetricNames($domainName, $instanceName, $jobId, $operator, $reloadCache);
  if ($rc) { print("Error running getOperatorMetricNames:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorMetricValue
################################
sub getOperatorMetricValue() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();    
  my $metricName = promptMetricName();
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorMetricValue($domainName, $instanceName, $jobId, $operator, $metricName, $reloadCache);
  if ($rc) { print("Error running getOperatorMetricName:  $output\n"); }
  else { print("Operator Metric value:  $output\n"); } 
}

################################
# Test getOperatorOutputPortMetricNames
################################
sub getOperatorOutputPortMetricNames() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();    
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortMetricNames($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortMetricNames:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorOutputPortMetricValue
################################
sub getOperatorOutputPortMetricValue() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorOutputPort = promptOperatorOutputPort();
  my $metricName = promptMetricName();    
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorOutputPortMetricValue($domainName, $instanceName, $jobId, $operator, $operatorOutputPort, $metricName, $reloadCache);
  if ($rc) { print("Error running getOperatorOutputPortMetricValue:  $output\n"); }
  else { print("Operator Output Port Metric value:  $output\n"); } 
}

################################
# Test getOperatorInputPortMetricNames
################################
sub getOperatorInputPortMetricNames() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();    
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortMetricNames($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortMetricNames:  $output\n"); }
  else { printArray($output); }
}

################################
# Test getOperatorInputPortMetricValue
################################
sub getOperatorInputPortMetricValue() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $operator = promptOperator();
  my $operatorInputPort = promptOperatorInputPort();
  my $metricName = promptMetricName();    
  my $reloadCache = promptReloadCache();
  my ($rc, $output) = streamsManagement::getOperatorInputPortMetricValue($domainName, $instanceName, $jobId, $operator, $operatorInputPort, $metricName, $reloadCache);
  if ($rc) { print("Error running getOperatorInputPortMetricValue:  $output\n"); }
  else { print("Operator Input Port Metric value:  $output\n"); } 
}

################################
# Test submitJob
################################
sub submitJob() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $bundle = promptBundle();
  my $jobGroup = promptJobGroupAllowNull();
  my $jobName = promptJobNameAllowNull();
  my $jobParms = promptJobParms();
  my ($rc, $output) = streamsManagement::submitJob($domainName, $instanceName, $bundle, $jobParms, $jobGroup, $jobName);
  if ($rc) { print("Error running submitJob  $output\n"); }
  else { print("Submit job ID:  $output\n"); } 
}

################################
# Test cancelJob
################################
sub cancelJob() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $jobId = promptJobId();
  my $force = promptForce();
  my ($rc, $output) = streamsManagement::cancelJob($domainName, $instanceName, $jobId, $force);
  if ($rc) { print("Error running cancelJob  $output\n"); }
  else { print("Job $jobId cancelled\n"); } 
}

################################
# Test addDomainHost
################################
sub addDomainHost() {
  my $domainName = promptDomain();
  my $hostName = promptHostName();  
  my ($rc, $output) = streamsManagement::addDomainHost($domainName, $hostName);
  if ($rc) { print("Error running addDomainHost  $output\n"); }
  else { print("Host added\n"); } 
}

################################
# Test removeDomainHost
################################
sub removeDomainHost() {
  my $domainName = promptDomain();
  my $hostName = promptHostName();  
  my ($rc, $output) = streamsManagement::removeDomainHost($domainName, $hostName);
  if ($rc) { print("Error running removeDomainHost  $output\n"); }
  else { print("Host removed\n"); } 
}

################################
# Test getDomainHosts
################################
sub getDomainHosts() {
  my $domainName = promptDomain(); 
  my ($rc, $output) = streamsManagement::getDomainHosts($domainName);
  if ($rc) { print("Error running getDomainHosts  $output\n"); }
  else { printArray($output); }
}

################################
# Test addTagToHost
################################
sub addTagToHost() {
  my $domainName = promptDomain();
  my $hostName = promptHostName(); 
  my $tagName = promptTag(); 
  my ($rc, $output) = streamsManagement::addTagToHost($domainName, $hostName, $tagName);
  if ($rc) { print("Error running addTagToHost  $output\n"); }
  else { print("Tag added\n"); } 
}

################################
# Test removeTagFromHost
################################
sub removeTagFromHost() {
  my $domainName = promptDomain();
  my $hostName = promptHostName(); 
  my $tagName = promptTag(); 
  my ($rc, $output) = streamsManagement::removeTagFromHost($domainName, $hostName, $tagName);
  if ($rc) { print("Error running removeTagFromHost  $output\n"); }
  else { print("Tag removed\n"); } 
}

################################
# Test getHostTags
################################
sub getHostTags() {
  my $domainName = promptDomain();
  my $hostName = promptHostName(); 
  my ($rc, $output) = streamsManagement::getHostTags($domainName, $hostName);
  if ($rc) { print("Error running getHostTags  $output\n"); }
  else { printArray($output); }
}

################################
# Test makeInstance
################################
sub makeInstance() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my $adminGroup = promptAdminGroup();
  my $userGroup = promptUserGroup();
  my $properties = promptProperties();
  my $resourceSpecs = promptResourceSpecs();  
  my ($rc, $output) = streamsManagement::makeInstance($domainName, $instanceName, $adminGroup, $userGroup, $properties, $resourceSpecs);
  if ($rc) { print("Error running makeInstance  $output\n"); }
  else { print("Instance created\n"); } 
}

################################
# Test removeInstance
################################
sub removeInstance() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my ($rc, $output) = streamsManagement::removeInstance($domainName, $instanceName);
  if ($rc) { print("Error running cancelJob  $output\n"); }
  else { print("Instance $instanceName removed\n"); } 
}

################################
# Test startInstance
################################
sub startInstance() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();  
  my ($rc, $output) = streamsManagement::startInstance($domainName, $instanceName);
  if ($rc) { print("Error starting instance  $output\n"); }
  else { print("Instance $instanceName started\n"); } 
}

################################
# Test stopInstance
################################
sub stopInstance() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();
  my $force = promptForce();  
  my ($rc, $output) = streamsManagement::stopInstance($domainName, $instanceName, $force);
  if ($rc) { print("Error stopping instance  $output\n"); }
  else { print("Instance $instanceName stopped\n"); } 
}

################################
# Test makeDomain
################################
sub makeDomain() {
  my $domainName = promptDomain();
  my $hosts = promptHosts();   
  my ($rc, $output) = streamsManagement::makeDomain($domainName, $hosts);
  if ($rc) { print("Error running makeDomain  $output\n"); }
  else { print("Domain $domainName created\n"); } 
}

################################
# Test genKey
################################
sub genKey() {
  my $domainName = promptDomain();
  my ($rc, $output) = streamsManagement::genKey($domainName);
  if ($rc) { print("Error running genKey  $output\n"); }
  else { print("Keys generated\n"); } 
}

################################
# Test removeDomain
################################
sub removeDomain() {
  my $domainName = promptDomain(); 
  my ($rc, $output) = streamsManagement::removeDomain($domainName);
  if ($rc) { print("Error running removeDomain  $output\n"); }
  else { print("Domain $domainName removed\n"); } 
}

################################
# Test startDomain
################################
sub startDomain() {
  my $domainName = promptDomain(); 
  my ($rc, $output) = streamsManagement::startDomain($domainName);
  if ($rc) { print("Error running startDomain  $output\n"); }
  else { print("Domain $domainName started\n"); } 
}

################################
# Test stopDomain
################################
sub stopDomain() {
  my $domainName = promptDomain();
  my $force = promptForce();  
  my ($rc, $output) = streamsManagement::stopDomain($domainName, $force);
  if ($rc) { print("Error stopping domain  $output\n"); }
  else { print("Domain $domainName stopped\n"); } 
}

################################
# Test getJobLogs
################################
sub getJobLogs() {
  my $domainName = promptDomain();
  my $instanceName = promptInstance();
  my $jobId = promptJobId();
  my $logFile = promptLogFile(); 
  my ($rc, $output) = streamsManagement::getJobLogs($domainName, $instanceName, $jobId, $logFile);
  if ($rc) { print("Error retrieving logs  $output\n"); }
  else { print("Log files retieved to $logFile\n"); } 
}

################################
# Test getDomainLogs
################################
sub getDomainLogs() {
  my $domainName = promptDomain();
  my $logFile = promptLogFile(); 
  my ($rc, $output) = streamsManagement::getDomainLogs($domainName, $logFile);
  if ($rc) { print("Error retrieving logs  $output\n"); }
  else { print("Log files retieved to $logFile\n"); } 
}

################################
# Test dumpCache
################################
sub dumpCache() {
  my $dump = streamsManagement::dumpCache();
  my $filename = "dump.txt";
  #my $cmd = "echo \"$dump\" > dump.txt";
  #system($cmd);
  open(my $fh, '>', $filename) or die "Could not open file $filename' $!";
  print $fh "$dump\n";
  close $fh;
  print("Cache info put in file dump.txt\n");
}

################################
################################
# Common utility functions
################################
################################

################################
# Read line from STDIN
################################
sub readSTDIN() {
  my $line = <STDIN>;
  chomp $line;
  return($line);
}


################################
# promptDomain
################################
sub promptDomain() {
  my $domain = "";
  my $default = $ENV{STREAMS_DOMAIN_ID};
  while (0 == length($domain)) {
    if (defined($default)) {
      print("Enter domain ($default):  ");    
    }
    else {
      print("Enter domain:  ");
    }
    $domain = readSTDIN();
    if (0 == length($domain)) {
      if (defined($default)) {
        $domain = $default; 
      }
      else {
        print("Invalid domain.  Try again\n");
      }        
    }
  }
  return($domain);
}

################################
# promptInstance
################################
sub promptInstance() {
  my $instance = "";
  while (0 == length($instance)) {
    print("Enter instance:  ");
    $instance = readSTDIN();
    if (0 == length($instance)) {
      print("Invalid instance.  Try again\n");        
    }
  }
  return($instance);
}

################################
# promptJobId
################################
sub promptJobId() {
  my $jobId = "";
  my $invalid = 1;
  while ($invalid) {
    print("Enter job ID:  ");
    $jobId = readSTDIN();
    
    $invalid = checkWholeNumber($jobId);
    if ($invalid) {
      print("Invalid job ID.  Try again\n");        
    }
  }
  return($jobId);
}

################################
# promptJobName
################################
sub promptJobName() {
  my $jobName = "";
  while (0 == length($jobName)) {
    print("Enter job name:  ");
    $jobName = readSTDIN();
    if (0 == length($jobName)) {
      print("Invalid job name.  Try again\n");        
    }
  }
  return($jobName);
}

################################
# promptJobParms
################################
sub promptJobParms() {
  my @jobParms;
  my $jobParm = "";
  do {
    print("Enter job parm name=value: (default: no more parms)  ");
    $jobParm = readSTDIN();
    if (0 != length($jobParm)) {
      # does it look ok?
      if ($jobParm !~ /.=./) {
        print("Parameter does not look ok.  Should be name=value.  Try again.\n");
      }
      else {
        push(@jobParms, $jobParm);
      }        
    }
  #}
  } while (0 != length($jobParm));
  return(\@jobParms);
}

################################
# promptJobGroupAllowNull
################################
sub promptJobGroupAllowNull() {
  my $jobGroup = "";
  print("Enter job group(default=undefined):  ");
  $jobGroup = readSTDIN();
  if (0 == length($jobGroup)) {
    return(undef)
  }
  else {
    return($jobGroup);
  }
}


################################
# promptJobNameAllowNull
################################
sub promptJobNameAllowNull() {
  my $jobName = "";
  print("Enter job name(default=undefined):  ");
  $jobName = readSTDIN();
  if (0 == length($jobName)) {
    return(undef)
  }
  else {
    return($jobName);
  }
}

################################
# promptPEId
################################
sub promptPEId() {
  my $peId = "";
  my $invalid = 1;
  while ($invalid) {
    print("Enter PE ID:  ");
    $peId = readSTDIN();
    
    $invalid = checkWholeNumber($peId);
    if ($invalid) {
      print("Invalid PE ID.  Try again\n");        
    }
  }
  return($peId);
}

################################
# promptOperator
################################
sub promptOperator() {
  my $operator = "";
  while (0 == length($operator)) {
    print("Enter operator:  ");
    $operator = readSTDIN();
    if (0 == length($operator)) {
      print("Invalid operator.  Try again\n");        
    }
  }
  return($operator);
}

################################
# promptOperatorOutputPort
################################
sub promptOperatorOutputPort() {
  my $operatorOutputPort = "";
  while (0 == length($operatorOutputPort)) {
    print("Enter operator output port:  ");
    $operatorOutputPort = readSTDIN();
    if (0 == length($operatorOutputPort)) {
      print("Invalid operator output port.  Try again\n");        
    }
  }
  return($operatorOutputPort);
}

################################
# promptOperatorInputPort
################################
sub promptOperatorInputPort() {
  my $operatorInputPort = "";
  while (0 == length($operatorInputPort)) {
    print("Enter operator input port:  ");
    $operatorInputPort = readSTDIN();
    if (0 == length($operatorInputPort)) {
      print("Invalid operator input port.  Try again\n");        
    }
  }
  return($operatorInputPort);
}

################################
# promptOperatorOutputPortConnection
################################
sub promptOperatorOutputPortConnection() {
  my $operatorOutputPortConnection = "";
  while (0 == length($operatorOutputPortConnection)) {
    print("Enter operator output port connection:  ");
    $operatorOutputPortConnection = readSTDIN();
    if (0 == length($operatorOutputPortConnection)) {
      print("Invalid operator output port connection.  Try again\n");        
    }
  }
  return($operatorOutputPortConnection);
}

################################
# promptOperatorInputPortConnection
################################
sub promptOperatorInputPortConnection() {
  my $operatorInputPortConnection = "";
  while (0 == length($operatorInputPortConnection)) {
    print("Enter operator input port connection:  ");
    $operatorInputPortConnection = readSTDIN();
    if (0 == length($operatorInputPortConnection)) {
      print("Invalid operator input port connection.  Try again\n");        
    }
  }
  return($operatorInputPortConnection);
}

################################
# promptMetricName
################################
sub promptMetricName() {
  my $metricName = "";
  while (0 == length($metricName)) {
    print("Enter metric name:  ");
    $metricName = readSTDIN();
    if (0 == length($metricName)) {
      print("Invalid metric name.  Try again\n");        
    }
  }
  return($metricName);
}

################################
# promptBundle
################################
sub promptBundle() {
  my $bundle = "";
  while (0 == length($bundle)) {
    print("Enter bundle path:  ");
    $bundle = readSTDIN();
    if (0 == length($bundle)) {
      print("Invalid bundle name.  Try again\n");        
    }
  }
  return($bundle);
}

################################
# promptForce
################################
sub promptForce() {
  my $response = "";
  while ("n" ne $response && "y" ne $response) {
    print("Force : n or y(n):  ");
    $response = readSTDIN();
    if (0 == length($response)) {
      $response = "n";
    }
    if ("n" ne $response && "y" ne $response) {
      print("Invalid choice ($response).  Try again\n");
    }
  }
  if ("y" eq $response) {
    return(1);
  }
  else {
    return(0);
  }
}

################################
# promptHostName
################################
sub promptHostName() {
  my $hostName = "";
  while (0 == length($hostName)) {
    print("Enter host name:  ");
    $hostName = readSTDIN();
    if (0 == length($hostName)) {
      print("Invalid host name.  Try again\n");        
    }
  }
  return($hostName);
}

################################
# promptTag
################################
sub promptTag() {
  my $tag = "";
  while (0 == length($tag)) {
    print("Enter tag name:  ");
    $tag = readSTDIN();
    if (0 == length($tag)) {
      print("Invalid tag.  Try again\n");        
    }
  }
  return($tag);
}

################################
# promptAdminGroup
################################
sub promptAdminGroup() {
  my $adminGroup = "";
  print("Enter admin group(default=undefined):  ");
  $adminGroup = readSTDIN();
  if (0 == length($adminGroup)) {
    return(undef)
  }
  else {
    return($adminGroup);
  }
}

################################
# promptUserGroup
################################
sub promptUserGroup() {
  my $userGroup = "";
  print("Enter user group(default=undefined):  ");
  $userGroup = readSTDIN();
  if (0 == length($userGroup)) {
    return(undef)
  }
  else {
    return($userGroup);
  }
}

################################
# promptProperties
################################
sub promptProperties() {
  my @properties;
  my $property = "";
  do {
    print("Enter property name=value: (default: no more properties)  ");
    $property = readSTDIN();
    if (0 != length($property)) {
      # does it look ok?
      if ($property !~ /.=./) {
        print("Property does not look ok.  Should be name=value.  Try again.\n");
      }
      else {
        push(@properties, $property);
      }        
    }
  } while (0 != length($property));
  return(\@properties);
}


################################
# promptResourceSpecs
################################
sub promptResourceSpecs() {
  my $resourceSpecs = streamsManagement::initResourceSpecs();
  #my $property = "";
  my $ok;
  do {
    $ok = 0;
    my $tags;
    my $exclusive;
  
    my $count = promptResourceSpecCount();
    if (defined($count)) {
    
      $tags = promptResourceSpecTags();
      if (defined($tags)) {
      
        $exclusive = promptResourceSpecExclusive();
        $ok = 1;
      }
    }
    if ($ok) {
      streamsManagement::addToResourceSpecs($resourceSpecs, $count, $tags, $exclusive);
    }
  } while (0 != $ok);
  return($resourceSpecs);
}

################################
# promptResourceSpecCount
################################
sub promptResourceSpecCount() {
  my $resourceSpecCount;
  my $invalid = 1;
  while ($invalid) {
    print("Enter resource spec count(default:  no more resource specs):  ");
    $resourceSpecCount = readSTDIN();
    if (0 == length($resourceSpecCount)) {
      return(undef)
    }
    $invalid = checkWholeNumber($resourceSpecCount);
    if ($invalid) {
      print("Invalid resource spec count.  Try again\n");
    }
  }
  return($resourceSpecCount);
}

################################
# promptResourceSpecTags
################################
sub promptResourceSpecTags() {
  my @tags = ();
  my $tag;
  do {
    print("Enter tag(default:  no more tags):  ");
    $tag = readSTDIN();  
    if (0 != length($tag)) {
      push(@tags, $tag);
    }
  } while (0 != length($tag));

  return(\@tags);
}

################################
# promptResourceSpecExclusive
################################
sub promptResourceSpecExclusive() {
  my $response = "";
  while ("n" ne $response && "y" ne $response) {
    print("Exclusive Resource Spec: n or y(n):  ");
    $response = readSTDIN();
    if (0 == length($response)) {
      $response = "n";
    }
    if ("n" ne $response && "y" ne $response) {
      print("Invalid choice ($response).  Try again\n");
    }
  }
  if ("y" eq $response) {
    return(1);
  }
  else {
    return(0);
  }
}

################################
# promptReloadCache
################################
sub promptReloadCache() {
  my $response = "";
  while ("n" ne $response && "y" ne $response) {
    print("Reload cache: n or y(n):  ");
    $response = readSTDIN();
    if (0 == length($response)) {
      $response = "n";
    }
    if ("n" ne $response && "y" ne $response) {
      print("Invalid choice ($response).  Try again\n");
    }
  }
  if ("y" eq $response) {
    return(1);
  }
  else {
    return(0);
  }
}

################################
# promptHosts
################################
sub promptHosts() {
  my @hosts = ();
  my $host;
  do {
    print("Enter host(default:  no more hosts):  ");
    $host = readSTDIN();  
    if (0 != length($host)) {
      push(@hosts, $host);
    }
  } while (0 != length($host));

  return(\@hosts);
}

################################
# promptLogFile
################################
sub promptLogFile() {
  my $logFile = "";
  while (0 == length($logFile)) {
    print("Enter log file:  ");
    $logFile = readSTDIN();
    if (0 == length($logFile)) {
      print("Invalid log file.  Try again\n");        
    }
  }
  return($logFile);
}

################################
# checkWholeNumber
################################
sub checkWholeNumber($) {
  my ($num) = (@_);
  if (length($num) < 1) {
    return(1);
  }
  if ($num =~ /^\d+$/) {
    return(0);
  }
  else {
    return(1);
  }
}

################################
# printArray
################################
sub printArray($) {
  my ($array) = (@_);
  foreach my $nextEntry (@$array) {
    print("$nextEntry\n");
  }
}

################################
my $rc = main();
exit($rc);

