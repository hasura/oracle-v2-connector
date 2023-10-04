FROM registry.access.redhat.com/ubi9-minimal:9.2-717 AS root

ARG JAVA_PACKAGE=java-17-openjdk-headless

ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en'

RUN microdnf install -y ${JAVA_PACKAGE} which shadow-utils \
    && microdnf update -y \
    && microdnf clean all

FROM root AS build

ARG RUN_JAVA_VERSION=1.3.8

WORKDIR /build

# Copy only the files required to use gradle such that the build can be run
# and all dependencies retrieved and cached in a layer before the app source
# is included and built. This ensures the dependencies don't need to be
# redownloaded every time the app source changes
COPY gradle/ gradle/
COPY gradlew gradlew
COPY gradle.properties gradle.properties
COPY settings.gradle.kts settings.gradle.kts
COPY app/build.gradle.kts app/build.gradle.kts
COPY gdc-ir/build.gradle.kts gdc-ir/build.gradle.kts
COPY sql-gen/build.gradle.kts sql-gen/build.gradle.kts
COPY buildSrc/ buildSrc/
COPY lib/ lib/

RUN ./gradlew build

# Copy everything, including the app source and build it. Dependencies should
# have already been downloaded before, so this should be relatively quick
COPY . .

RUN ./gradlew build \
    && curl https://repo1.maven.org/maven2/io/fabric8/run-java-sh/${RUN_JAVA_VERSION}/run-java-sh-${RUN_JAVA_VERSION}-sh.sh -o app/build/quarkus-app/run-java.sh \
    && chmod 540 app/build/quarkus-app/run-java.sh

FROM root AS runtime

RUN  <<EOF
set -ex
groupadd -g 1001 hasura
useradd -m -u 1001 -g hasura hasura
mkdir /app
chown 1001 /app
chmod "g+rwX" /app
chown 1001:1001 /app
EOF

COPY --chown=1001:1001 --from=build /build/app/build/quarkus-app/run-java.sh /app/
COPY --chown=1001:1001 --from=build /build/app/build/quarkus-app/lib/ /app/lib/
COPY --chown=1001:1001 --from=build /build/app/build/quarkus-app/quarkus-run.jar /app/super-connector.jar
COPY --chown=1001:1001 --from=build /build/app/build/quarkus-app/app/ /app/app/
COPY --chown=1001:1001 --from=build /build/app/build/quarkus-app/quarkus/ /app/quarkus/

EXPOSE 8081 5005
USER 1001
ENV JAVA_OPTIONS="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
ENV JAVA_APP_JAR="/app/super-connector.jar"
ENV DATASETS_ENABLED="false"
ENV OTEL_PROPAGATORS="tracecontext,baggage,b3multi"

ENTRYPOINT ["/app/run-java.sh"]
