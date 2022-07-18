FROM alpine

WORKDIR /root

COPY bin/redisproxy /root/redisproxy
COPY config/config.yaml /root/config/config.yaml


CMD /root/redisproxy -c /root/config/config.yaml
