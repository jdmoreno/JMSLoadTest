:: Name:    Reference Data Validator - Test7
:: Purpose: Validate reference tables in CSV format

REM @ECHO OFF
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set JAR-FOLDER=..\..\..\target
SET JAR-FILE=JMSLoadTest-0.0.1-SNAPSHOT.jar
SET CSV-CLASS=eps.platform.tools.jms.csv.CSVReader

:: Launch CSV reader
start "JMS Producer" CMD /K java -classpath %JAR-FOLDER%\%JAR-FILE%;tibjms.jar %CSV-CLASS% EPS-EMS-Test.csv

:END
ENDLOCAL
ECHO ON
@EXIT /B %ERRORLEVEL%