FROM registry.redhat.io/amq7/amq-broker-rhel9@sha256:ec6a178d76a1521ea68ef312b43db6b952802d02431ad81f1b9bd107015c621d
# Add the PostgreSQL JDBC driver to the broker classpath (/opt/amq/lib/*)
COPY postgresql.jar /opt/amq/lib/postgresql.jar
