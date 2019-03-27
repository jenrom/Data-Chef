#!/bin/sh
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
java -Xmx${ENV_RAMMB}m -Xms${ENV_RAMMB}m -jar /DataChef/data-chef-app-ng-${ENV_VERSION}.jar /tmp
