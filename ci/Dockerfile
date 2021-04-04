FROM ubuntu:bionic
MAINTAINER aiyangkun <yangkun.ayk@alibaba-inc.com>

# install essential package.
RUN apt update; \
    apt install sudo -y; \
    apt install openjdk-11-jdk -y; \
    apt install wget -y; \
    apt install unzip -y; \
    apt install vim -y

# add user for rocketmq.
RUN useradd -m rocketmq
RUN adduser rocketmq sudo
RUN echo 'rocketmq:Rocketmq!666' | chpasswd

USER rocketmq
ARG HOME=/home/rocketmq

# declare the package version.
ARG MAVEN_VERSION=3.6.3
ARG GRADLE_VERSION=6.8.3
ARG GRPC_VERSION=1.35.0
ARG PROTOBUFFER_VERSION=3.12.0

# install maven.
RUN mkdir -p $HOME/maven; \
    wget -P $HOME/maven https://mirrors.bfsu.edu.cn/apache/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz; \
    tar -zxf $HOME/maven/apache-maven-$MAVEN_VERSION-bin.tar.gz -C $HOME/maven

# install gradle.
RUN mkdir -p $HOME/gradle; \
    wget -P $HOME/gradle https://services.gradle.org/distributions/gradle-$GRADLE_VERSION-bin.zip; \
    unzip $HOME/gradle/gradle-$GRADLE_VERSION-bin.zip -d $HOME/gradle

# add maven and gradle to PATH.
ENV PATH=$HOME/maven/apache-maven-$MAVEN_VERSION/bin:$HOME/gradle/gradle-$GRADLE_VERSION/bin:$PATH

# download grpc-java.
RUN wget -P $HOME https://github.com/aaron-ai/grpc-java/archive/refs/tags/rocketmq-v$GRPC_VERSION.tar.gz; \
    tar -zxf $HOME/rocketmq-v$GRPC_VERSION.tar.gz -C $HOME && rm $HOME/rocketmq-v$GRPC_VERSION.tar.gz

# compile and install customized grpc-java.
RUN chmod +x $HOME/grpc-java-rocketmq-v$GRPC_VERSION/prepare_for_rmq.sh
RUN $HOME/grpc-java-rocketmq-v$GRPC_VERSION/prepare_for_rmq.sh

# download customized protobuffer-java.
RUN wget -P $HOME https://github.com/aaron-ai/protobuf/archive/refs/tags/rocketmq-v$PROTOBUFFER_VERSION.tar.gz; \
    tar -zxf $HOME/rocketmq-v$PROTOBUFFER_VERSION.tar.gz -C $HOME && rm $HOME/rocketmq-v$PROTOBUFFER_VERSION.tar.gz

# install protoc.
RUN mkdir -p $HOME/protoc; \
    wget -P $HOME/protoc https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOBUFFER_VERSION/protoc-$PROTOBUFFER_VERSION-linux-x86_64.zip; \
    unzip $HOME/protoc/protoc-$PROTOBUFFER_VERSION-linux-x86_64.zip -d $HOME/protoc; \
    cp $HOME/protoc/bin/protoc $HOME/protobuf-rocketmq-v$PROTOBUFFER_VERSION/src/

# compile and install customized protobuffer-java.
RUN mvn clean install -DskipTests -f $HOME/protobuf-rocketmq-v$PROTOBUFFER_VERSION/java/pom.xml