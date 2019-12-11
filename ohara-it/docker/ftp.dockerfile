FROM centos:7.7.1908
RUN yum groupinstall -y "Development Tools"
RUN yum install -y epel-release
RUN yum install -y openssl-devel \
   wget
RUN wget http://ftp.ntu.edu.tw/pure-ftpd/releases/pure-ftpd-1.0.47.tar.gz
RUN tar zxvf pure-ftpd-1.0.47.tar.gz
RUN cd pure-ftpd-* && ./configure \
  --prefix=/opt/pureftpd \
  --without-inetd \
  --with-altlog \
  --with-puredb \
  --with-throttling \
  --with-peruserlimits \
  --with-tls \
  --without-capabilitie && \
  make && \
  make install

RUN ln -s /opt/pureftpd/bin/* /usr/bin
RUN ln -s /opt/pureftpd/sbin/* /usr/sbin

# change user from root to ohara
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

COPY ftpd.sh /usr/sbin
COPY pure-ftpd.sh /opt/pureftpd/bin/pure-ftpd.sh
RUN chmod +x /usr/sbin/ftpd.sh
RUN chown -R ohara:ohara /opt/pureftpd

# copy Tini
COPY --from=oharastream/ohara:deps /tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "ftpd.sh"]