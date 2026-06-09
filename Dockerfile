FROM alpine:latest as builder
ARG TARGETPLATFORM
RUN echo "I'm building for $TARGETPLATFORM"

RUN apk add --no-cache gzip && \
    mkdir /nautilus-core-config && \
    wget -O /nautilus-core-config/geoip.metadb https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/geoip.metadb && \
    wget -O /nautilus-core-config/geosite.dat https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/geosite.dat && \
    wget -O /nautilus-core-config/geoip.dat https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/geoip.dat

COPY docker/file-name.sh /nautilus-core/file-name.sh
WORKDIR /nautilus-core
COPY bin/ bin/
RUN FILE_NAME=`sh file-name.sh` && echo $FILE_NAME && \
    FILE_NAME=`ls bin/ | egrep "$FILE_NAME.gz"|awk NR==1` && echo $FILE_NAME && \
    mv bin/$FILE_NAME nautilus-core.gz && gzip -d nautilus-core.gz && chmod +x nautilus-core && echo "$FILE_NAME" > /nautilus-core-config/test
FROM alpine:latest
LABEL org.opencontainers.image.source="https://github.com/slow-craft/nautilus-core"

RUN apk add --no-cache ca-certificates tzdata iptables

VOLUME ["/root/.config/mihomo/"]

COPY --from=builder /nautilus-core-config/ /root/.config/mihomo/
COPY --from=builder /nautilus-core/nautilus-core /nautilus-core
ENTRYPOINT [ "/nautilus-core" ]
