FROM centos:7.7.1908

RUN yum install -y \
  samba \
  samba-client \
  samba-common

COPY samba.sh /usr/sbin
RUN chmod +x /usr/sbin/samba.sh

# copy Tini
COPY --from=oharastream/ohara:deps /tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--", "samba.sh"]
