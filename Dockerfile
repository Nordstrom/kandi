FROM scratch

COPY bin/kandi /kandi
ENTRYPOINT ["/kandi"]