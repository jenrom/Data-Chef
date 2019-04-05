#!/bin/bash
# export $(grep -v '^#' .env  | perl -ne 'print "ENV_$_"' | xargs)

VERSION=3.2
RAMMB=1536
DWH_DBNAME=<LOWERCASE_DBNAME>
DWH_JDBCCONNECTIONSTRING="jdbc:snowflake://wk41990.eu-west-1.snowflakecomputing.com/?warehouse=<LOWERCASE_WAREHOUSE_NAME>&db=<LOWERCASE_DBNAME>"
DWH_JDBCDRIVERURL="jar:file:./driver/snowflake-jdbc-3.6.10.jar!/"
DWH_CATALOG=<LOWERCASE_DBNAME>
DWH_DBTYPE=SNOWFLAKE
DWH_JDBCDRIVERCLASS=net.snowflake.client.jdbc.SnowflakeDriver
DWH_SCANSUPPORTEDDATATYPES=false
DWH_USERNAME=<USERNAME>
DWH_PASSWORD=<PASSWORD>
REPOSITORY_JDBCCONNECTIONSTRING="jdbc:mysql://localhost:3306/datachef?useSSL=false"
REPOSITORY_USERNAME=datachef
REPOSITORY_PASSWORD=datachef
TEMPLATE_TEMPLATEPATH=templates/snowflake
TEMPLATE_ENABLEPRERUNSCRIPT=true
TEMPLATE_ENABLEPOSTRUNSCRIPT=false

export ENV_VERSION=${VERSION}
export ENV_RAMMB=${RAMMB}
export ENV_DWH_dbType=${DWH_DBTYPE}
export ENV_DWH_jdbcDriverURL=${DWH_JDBCDRIVERURL}
export ENV_DWH_jdbcDriverClass=${DWH_JDBCDRIVERCLASS}
export ENV_DWH_jdbcConnectionString=${DWH_JDBCCONNECTIONSTRING}
export ENV_DWH_catalog=${DWH_CATALOG}
export ENV_DWH_username=${DWH_USERNAME}
export ENV_DWH_password=${DWH_PASSWORD}
export ENV_DWH_dbName=${DWH_DBNAME}
export ENV_REPOSITORY_jdbcConnectionString=${REPOSITORY_JDBCCONNECTIONSTRING}
export ENV_REPOSITORY_username=${REPOSITORY_USERNAME}
export ENV_REPOSITORY_password=${REPOSITORY_PASSWORD}
export ENV_TEMPLATE_templatePath=${TEMPLATE_TEMPLATEPATH}
export ENV_TEMPLATE_enablePreRunScript=${TEMPLATE_ENABLEPRERUNSCRIPT}
export ENV_TEMPLATE_enablePostRunScript=${TEMPLATE_ENABLEPOSTRUNSCRIPT}



CONFIG_DIR=config
SINK_DIR=sink
DWH_CONFIG=$CONFIG_DIR/dwh.config.properties
REPOSITORY_CONFIG=$CONFIG_DIR/repository.config.properties
TEMPLATE_CONFIG=$CONFIG_DIR/template.config.properties
rm -rf $CONFIG_DIR
mkdir $CONFIG_DIR

printenv | grep ^ENV_DWH | while read line
do
  envname=`echo $line | cut -f1 -d"="`
  varname=`echo $envname | cut -f3 -d"_"`
  content=`echo $line | cut -f2- -d"="`
  if ! [ -z ${content} ]; then echo ${varname}=${content} >> $DWH_CONFIG; fi
done

printenv | grep ^ENV_REPOSITORY | while read line
do
  envname=`echo $line | cut -f1 -d"="`
  varname=`echo $envname | cut -f3 -d"_"`
  content=`echo $line | cut -f2- -d"="`
  if ! [ -z ${content} ]; then echo ${varname}=${content} >> $REPOSITORY_CONFIG; fi
done

printenv | grep ^ENV_TEMPLATE | while read line
do
  envname=`echo $line | cut -f1 -d"="`
  varname=`echo $envname | cut -f3 -d"_"`
  content=`echo $line | cut -f2- -d"="`
  if ! [ -z ${content} ]; then echo ${varname}=${content} >> $TEMPLATE_CONFIG; fi
done

echo "Configs created"
COUNT=`find ${SINK_DIR} -maxdepth 1 -type f -printf '%f\n' | wc -l`
echo "# files in sink: $COUNT"
if [$COUNT -gt 0 ] ; then
	echo "Files in sink directory detected, aborting startup!";
	exit 1;
fi