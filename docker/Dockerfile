#
# Docker file for the Karma API
#

FROM ubuntu

#
# Install software...
#
RUN \
    apt-get update && \
    apt-get install -y \
      git \
      curl \
      apt-transport-https \
      software-properties-common

#
# Install Anaconda3
#
RUN apt-get update --fix-missing && apt-get install -y wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 \
        git mercurial subversion

RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    wget --quiet https://repo.continuum.io/archive/Anaconda3-4.2.0-Linux-x86_64.sh -O ~/anaconda.sh && \
        /bin/bash ~/anaconda.sh -b -p /opt/conda && \
            rm ~/anaconda.sh

RUN apt-get install -y curl grep sed dpkg && \
    TINI_VERSION=`curl https://github.com/krallin/tini/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'` && \
        curl -L "https://github.com/krallin/tini/releases/download/v${TINI_VERSION}/tini_${TINI_VERSION}.deb" > tini.deb && \
            dpkg -i tini.deb && \
                rm tini.deb && \
                    apt-get clean

ENV PATH /opt/conda/bin:$PATH

#
# Install Java 1.8...
#
RUN \
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
    add-apt-repository -y ppa:webupd8team/java && \
    apt-get update && \
    apt-get install -y \
      oracle-java8-installer

#
# Set the java path...
#
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

#
# Next we create a karma user...
#
ENV username karma
RUN useradd -ms /bin/bash $username && echo "$username:$username" | chpasswd && adduser $username sudo
RUN mkdir -p /home/$username && chown -R $username:$username /home/$username
WORKDIR /home/$username

#
# clean up...
#
RUN apt-get clean autoclean && \
    apt-get autoremove -y && \
        rm -rf /var/lib/{apt,dpkg,cache,log}/
#
# Switch to the user...
#
#USER $username

#
# Download spark
#
RUN \
    curl -L -O http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz && \
    tar -xvf spark-2.1.0-bin-hadoop2.7.tgz && \
    cd spark-2.1.0-bin-hadoop2.7/python && \
    pip install -e .

#
# Download python packages...
#
RUN \
    pip install py4j && \
    pip install elasticsearch && \
    pip install gensim

USER $username

#
# Download elastic search
#
RUN \
  curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.1.tar.gz && \
  tar -xvf elasticsearch-5.1.1.tar.gz


#
# Download this repo
#
RUN \
    git clone https://github.com/NICTA/iswc-2016-semantic-labeling.git && git checkout docker

#
# Expose the default port
#
EXPOSE 8000

#
# Launch the server...
#
CMD \
    eval 'elasticsearch-5.1.1/bin/elasticsearch &' && \
    cd iswc-2016-semantic-labeling && \
    python server.py -h 0.0.0.0
