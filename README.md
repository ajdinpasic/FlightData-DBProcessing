# FlightData-MapReduce
## Description 📜

This is a project that performs map-reduce on big data from course "Advanced Topics in Database Systems"

## Technologies 👨‍💻

Java
Hadoop
MSSQL

## Installation 🏧
Prerequisites: Java JDK and Hadoop downloaded and set up
1. Git clone <project-path>
2. Navigate to .../hadoop.3.0/sbin
3. Add database connection
3. start-all.cmd
4. Navigate to .../FlightData-MapReduce
5. mvn clean > mvn install
6. hadoop jar target/<jar-snapshot> org.flightdata/FlightRunner

## Features 🎫

1. Calculation of sum values
2. Calculation of min values
3. Calculation of max values
4. Calculation of average values
5. Splitting the data
6. Mapping the data 
7. Reducing and merging the data
8. Fetching the data from MSSQL via JDBC Driver
9. Generating output data to local file report
10. Setting up the job(s)
11. Measuring the job(s) time