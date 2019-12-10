FROM centos:7.7.1908
RUN yum groupinstall -y "Development Tools"
RUN yum install -y epel-release
RUN yum install -y openssl-devel \
   wget \
   supervisor
RUN wget https://download.pureftpd.org/pub/pure-ftpd/releases/pure-ftpd-1.0.47.tar.gz
RUN tar zxvf pure-ftpd-1.0.47.tar.gz
RUN cd pure-ftpd-* && ./configure \
  --prefix=/usr/local/pureftpd \
  --without-inetd \
  --with-altlog \
  --with-puredb \
  --with-throttling \
  --with-peruserlimits \
  --with-tls \
  --without-capabilitie && \
  make && \
  make install

RUN ln -s /usr/local/pureftpd/bin/* /usr/bin
RUN ln -s /usr/local/pureftpd/sbin/* /usr/sbin

# change user from root to ohara
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

COPY ftpd.sh /home/$USER
COPY pure-ftpd.conf /usr/local/pureftpd/etc/pure-ftpd.conf
CMD ["/bin/bash", "/home/ohara/ftpd.sh"]