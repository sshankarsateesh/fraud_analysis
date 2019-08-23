FROM python:3.6

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn

RUN set -e; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        software-properties-common \
    ; \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9; \
    apt-add-repository 'deb http://repos.azulsystems.com/debian stable main'; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        zulu-8 \
    ; \
    apt-get clean; \
    rm -rf /var/tmp/* /tmp/* /var/lib/apt/lists/*

RUN set -e; \
    pip install JPype1




ADD ./build/requirements.txt /tmp/requirements.txt
RUN pip install --trusted-host pypi.python.org -r /tmp/requirements.txt

RUN mkdir -p /tmp/src
COPY ./src/** /tmp/src/
RUN ls -lrt /tmp/src/

#ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk"

# Setup JAVA_HOME -- useful for docker commandline
#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
#RUN export JAVA_HOME=$JAVA_HOME
#FROM java:8-jdk
#ENV JAVA_VER 8
#RUN export  JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN chmod 755 /tmp/src/entry.sh

ENTRYPOINT ["sh" ,"/tmp/src/entry.sh"]
#CMD ["sh" ,"/tmp/src/entry.sh"]
