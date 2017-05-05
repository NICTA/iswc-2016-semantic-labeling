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
# Install python 3.5
#
#
#RUN apt-get update --fix-missing && apt-get install -y wget bzip2 ca-certificates \
#    libglib2.0-0 libxext6 libsm6 libxrender1 \
#    git mercurial subversion

RUN apt-get install -y --no-install-recommends \
        python3.5 \
        python3.5-dev

RUN apt-get install -y curl grep sed dpkg && \
    TINI_VERSION=`curl https://github.com/krallin/tini/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'` && \
        curl -L "https://github.com/krallin/tini/releases/download/v${TINI_VERSION}/tini_${TINI_VERSION}.deb" > tini.deb && \
            dpkg -i tini.deb && \
                rm tini.deb && \
                    apt-get clean

# make some useful symlinks that are expected to exist
RUN cd /usr/local/bin/ \
#	&& { [ -e easy_install ] || ln -s easy_install-* easy_install; } \
	&& ln -s idle3 idle \
	&& ln -s pydoc3 pydoc \
	&& ln -s python3 python \
	&& ln -s python3-config python-config

# symlink to make python3 default
RUN rm -f /usr/bin/python && ln -s /usr/bin/python3 /usr/bin/python

# get pip
RUN curl -O https://bootstrap.pypa.io/get-pip.py && \
    python3 get-pip.py && \
    rm get-pip.py


RUN pip3 --no-cache-dir install \
        flask \
        flask-admin \
        flask-login \
        flask-restful \
        flask-uploads \
        matplotlib \
        numpy \
        requests \
        sklearn \
        pandas \
        Pillow
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
# Set pyspark version of python to 3.5
#
ENV PYSPARK_PYTHON=python3.5
ENV PYSPARK_DRIVER_PYTHON=python3.5

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
    pip3 install -e .

#
# Download python packages...
#
RUN \
    pip3 install py4j && \
    pip3 install elasticsearch && \
    pip3 install gensim

USER $username

#
# Download elastic search
#
RUN \
  curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.1.tar.gz && \
  tar -xvf elasticsearch-5.1.1.tar.gz

# remove archives
RUN rm elasticsearch-5.1.1.tar.gz
RUN rm spark-2.1.0-bin-hadoop2.7.tgz

#
# Download this repo
#
RUN git clone https://github.com/NICTA/iswc-2016-semantic-labeling.git

#
# Swith to the folder where the project is
#
WORKDIR iswc-2016-semantic-labeling
RUN git checkout docker

WORKDIR /home/$username

#
# Expose the default port
#
EXPOSE 8000

#
# Launch the server...
#
CMD \
    eval 'elasticsearch-5.1.1/bin/elasticsearch &' && \
    cd iswc-2016-semantic-labeling/ &&\
    python3 server.py

