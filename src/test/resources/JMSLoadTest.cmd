:: Name:    Reference Data Validator - Test7
:: Purpose: Validate reference tables in CSV format

@ECHO OFF
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set JAR-FOLDER=..\..\..\target
SET JAR-FILE=JMSLoadTest-0.0.1-SNAPSHOT.jar
SET PRODUCER-CLASS=eps.platform.tools.jms.JMSProducer
SET CONSUMER-CLASS=eps.platform.tools.jms.JMSConsumer

:: Launch Producer
:PRODUCER
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
:CONSUMER
start "JMS Consumer" CMD /K java -classpath %JAR-FOLDER%\%JAR-FILE%;tibjms.jar %CONSUMER-CLASS% ^
-server tcp://ems.test:7222 ^
-user admin ^
-queue sample.in ^
-count 1000000 ^
-time 60 ^
-threads 1 ^
-connections 1

:END
ENDLOCAL
ECHO ON
@EXIT /B %ERRORLEVEL%