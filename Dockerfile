FROM freedomkk/tomcat-maven

MAINTAINER Langens Jonathan <flowofcontrol.com>

COPY server.xml /usr/local/tomcat/conf/server.xml

ADD . /app

WORKDIR /app

RUN mvn clean install

RUN rm -rf /usr/local/tomcat/webapps/ROOT

RUN cp target/*.war /usr/local/tomcat/webapps/ROOT.war

HEALTHCHECK CMD curl -f -d 'query=ASK%20%7B%7D' http://localhost:8890/sparql || exit 1

EXPOSE 8080

CMD ["sh", "build-and-run"]
