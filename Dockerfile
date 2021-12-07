FROM openjdk:11
# copy the packaged jar file into our docker image
COPY BankBalance.jar /BankBalance.jar
#COPY BankBalance.class /BankBalance.class
COPY lib/*.jar /lib/
COPY config.kafkastream.properties /config.kafkastream.properties
COPY log4j.properties /log4j.properties
COPY file1.txt /file1.txt
COPY es-cert.p12 /es-cert.p12
# set the startup command to execute the jar
#CMD ["java","-classpath","lib/*:","BankBalance"]
CMD ["java","-jar","BankBalance.jar"]