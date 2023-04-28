FROM alpine

WORKDIR /root

COPY bin/IceFireDB-PubSub /root/IceFireDB-PubSub
COPY config/config.yaml /root/config/config.yaml


CMD /root/IceFireDB-PubSub -c /root/config/config.yaml
