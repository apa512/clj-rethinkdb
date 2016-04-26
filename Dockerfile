FROM rethinkdb:2.3.0

COPY ./test-resources/key.pem /data
COPY ./test-resources/cert.pem /data

CMD ["rethinkdb", "--bind", "all", "--driver-tls-key", "key.pem",  "--driver-tls-cert", "cert.pem"]
