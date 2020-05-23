FROM adoptopenjdk/openjdk8
COPY build/libs/adjustments-streams.jar streams.jar
CMD java ${JAVA_OPTS} -jar streams.jar
