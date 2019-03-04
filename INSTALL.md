Building the Data Chef
======================

The Data Chef source code uses the Maven build system in order to easily build a jar package of Data Chef.

Installation step by step
-------------------------

1. Clone the GitHub areto Data Chef repository on your local system

2. Download https://github.com/areto-consulting-gmbh/Data-Chef/releases/download/3.2-rc/datachef-3.2-rc.zip and extract the archive into the Data Chef folder. There should be a target folder inside your Data Chef folder afterwards.

(optional) For using Snowflake, download http://central.maven.org/maven2/net/snowflake/snowflake-jdbc/3.6.10/snowflake-jdbc-3.6.10.jar and move it into the drivers subdirectory inside your Data Chef folder.

3. Install Docker and Docker Compose, tested versions are 18.09.1; 1.17.1

4. Copy either .env-exa.sample or .env-sf.sample to .env and do needed modifications

5. Execute `docker-compose up`

6. Open `http://localhost:4567` in your local browser

Installation from source
------------------------

These steps replace the second step of the above step by step instructions. 

1. Install Java SE Development Kit (JDK), at least version 8

2. Install Maven, tested version: 3.5.2

3. Execute `mvn -Dmaven.test.skip=true package`


Configuration
-------------

The whole configuration is done in the .env file. docker-compose propagates the environment variables to the container where they are used to create the configuration files.
