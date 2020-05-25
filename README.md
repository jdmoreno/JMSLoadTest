# JMSLoadTest

## Description
Utility produce JMS messages to TIBCO ems.
The tool has three usages:
- JMS producer
- JMS consumer
- CSV loader and trigger a set JMS producers and JMS consumers bese on the information in the CSV

<pre><code>
## Usage
:: Name:    Reference Data Validator - Test7
:: Purpose: Validate reference tables in CSV format

REM @ECHO OFF
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set JAR-FOLDER=..\..\..\target
SET JAR-FILE=JMSLoadTest-0.0.1-SNAPSHOT.jar
SET PRODUCER-CLASS=eps.platform.tools.jms.JMSProducer
SET CONSUMER-CLASS=eps.platform.tools.jms.JMSConsumer

:: Launch Producer
start "JMS Producer" CMD /K java -classpath %JAR-FOLDER%\%JAR-FILE%;tibjms.jar %PRODUCER-CLASS% ^
-server tcp://ems.test:7222 ^
-user admin ^
-queue sample.in ^
-size 256 ^
-count 1000000 ^
-time 60 ^
-threads 3 ^
-connections 1 ^
-delivery NON_PERSISTENT ^
-rate 200

:: Launch Consumer
start "JMS Consumer" CMD /K java -classpath %JAR-FOLDER%\%JAR-FILE%;tibjms.jar %CONSUMER-CLASS% ^
-server tcp://ems.test:7222 ^
-user admin ^
-queue sample.in ^
-count 1000000 ^
-time 60 ^
-threads 1 ^
-connections 1
</code></pre>

## Configuration
