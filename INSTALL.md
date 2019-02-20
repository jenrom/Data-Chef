Building the Data Chef
======================

The Data Chef source code uses the Maven build system in order to easily build a jar package of Data Chef.

Installation step by step
-------------------------

1. Install Java SE Development Kit (JDK), at least version 8

2. Install Maven, tested version: 3.5.2

3. Install Docker and Docker Compose, tested versions 18.09.1; 1.17.1

4. Execute `mvn -Dmaven.test.skip=true package`

5. Copy either .env-exa.sample or .env-sf.sample to .env and do needed modifications

6. Execute `docker-compose up`

7. Open `http://localhost:4567` in your local browser

Configuration
-------------

The whole configuration is done in the .env file. docker-compose propagates the environment variables to the container where they are used to create the configuration files.
