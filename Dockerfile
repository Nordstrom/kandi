FROM alpine

COPY bin/release/kandi /kandi
ENTRYPOINT ["/kandi"]