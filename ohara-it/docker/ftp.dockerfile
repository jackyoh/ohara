#
# Copyright 2019 is-land
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
RUN chmod +x /usr/sbin/ftpd.sh
RUN chown -R ohara:ohara /opt/pureftpd

# copy Tini
COPY --from=oharastream/ohara:deps /tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "ftpd.sh"]