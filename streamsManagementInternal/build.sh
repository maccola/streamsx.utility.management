#!/bin/sh


#*******************************************************************************
#  Copyright (C) 2016, International Business Machines Corporation
#  All Rights Reserved
#*******************************************************************************                          

THISDIR=$(dirname $0)
INTERNALDIR=${THISDIR}
SRCDIR=${INTERNALDIR}/java/src
BINDIR=${INTERNALDIR}/java/bin
SRC_FILE=com/ibm/streamsx/management/StreamsManagementWrapper.java
WHOLE_SRC_FILE=${SRCDIR}/${SRC_FILE}


CP=${STREAMS_INSTALL}/lib/com.ibm.streams.management.jmxmp.jar:${STREAMS_INSTALL}/lib/com.ibm.streams.management.mx.jar:${STREAMS_INSTALL}/ext/lib/JSON4J.jar:${STREAMS_INSTALL}/system/impl/lib/com.ibm.streams.platform.jar
CP=${CP}:${STREAMS_INSTALL}/system/impl/lib/com.ibm.streams.management.mx.util.jar

mkdir -p $BINDIR
javac -sourcepath $SRCDIR -cp $CP -d $BINDIR $WHOLE_SRC_FILE
#javac -Xlint:unchecked -sourcepath $SRCDIR -cp $CP -d $BINDIR $WHOLE_SRC_FILE

