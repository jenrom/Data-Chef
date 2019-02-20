# Dockerfile for Data Chef
FROM openjdk:11-jre-slim
WORKDIR /DataChef
COPY driver /DataChef/driver
COPY target/lib /DataChef/lib
COPY target/data-chef*.jar /DataChef
COPY public /DataChef/public
COPY templates /DataChef/templates
COPY datachef-startup.sh /DataChef
ENTRYPOINT ["/DataChef/datachef-startup.sh"]
CMD ["-c"]
EXPOSE 4567
