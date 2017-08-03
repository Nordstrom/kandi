FROM scratch

COPY bin/release/kandi /kandi
ENTRYPOINT ["/kandi"]