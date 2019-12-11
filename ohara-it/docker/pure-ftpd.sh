#!/bin/bash
echo "PureDB                       /opt/pureftpd/etc/pureftpd.pdb"
echo "PassivePortRange             30000 30009"
echo "ForcePassiveIP               ${FORCE_PASSIVE_IP}"