Building the Data Chef
======================

The Data Chef source code uses the Maven build system in order to easily build a jar package of Data Chef.

Installation step by step
-------------------------

1. Clone the GitHub areto Data Chef repository to your local system

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

Run locally
------------

Steps:

1. Get and run mariadb on your local system  
    `docker pull mariadb`  
    `docker run --name mariadb-local -e MYSQL_ROOT_PASSWORD=areto -e MYSQL_DATABASE=datachef -e MYSQL_USER=datachef -e MYSQL_PASSWORD=datachef -p 3306:3306 -d mariadb`
2. Open project in Intellj
    1. Install plugin `lombok` (for annotations). Preferences -> Plugins -> Search for `lombok`
    2. Build -> Build Project
3. Modify `datachef-startup-dev.sh`, set `LOWERCASE_DBNAME`, `LOWERCASE_DATAWAREHOUSE_NAME`, `USERNAME` and `PASSWORD`.  
    1. Execute the script. 
    2. Check `config/*.config.properties` files are updated with the environment variables.
4. Compile: `mvn compile`
5. To package: `mvn -Dmaven.test.skip=true package`
6. Run `Application.java`
7. Open `http://localhost:4567` in your local browser
8. You should see `Setup DWH` and `Setup Repo` in orange color. For the first time, you'll need to setup data warehouse and the repository(`mariadb`). So click first on `Setup DWH` (should turn green), then click `Setup Repo` (should turn green) 

Configuration
-------------

The whole configuration is done in the .env file. docker-compose propagates the environment variables to the container where they are used to create the configuration files.
