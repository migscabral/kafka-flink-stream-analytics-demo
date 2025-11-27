# STEP 1: Build Stage - Get Flink libraries and home directory
FROM flink:1.17.2-scala_2.12 AS flink-base

# STEP 2: Final Stage - Build Zeppelin image with Flink dependencies
FROM apache/zeppelin:0.11.2

# Find the user defined in the base image (often 'zeppelin')
# If the base image defines a user, we must switch back to it at the end.
# We'll assume the user is 'zeppelin' for security, but the best approach is to temporarily use root.

# --- FIX START ---
# Temporarily switch to root to perform installation/copying to system folders
USER root

# Set FLINK_HOME environment variable for the Zeppelin Flink Interpreter
ENV FLINK_HOME=/opt/flink

# # Create the necessary directory structure inside the Zeppelin image
# RUN mkdir -p ${FLINK_HOME} \
#     && mkdir -p ${FLINK_HOME}/lib

# Copy Flink client libraries and distribution files from the flink-base stage.
COPY --from=flink-base /opt/flink /opt/flink
# COPY --from=flink-base /opt/flink/lib ${FLINK_HOME}/lib
# COPY --from=flink-base /opt/flink/conf ${FLINK_HOME}/conf
# COPY --from=flink-base /opt/flink/bin ${FLINK_HOME}/bin
# COPY --from=flink-base /opt/flink/opt ${FLINK_HOME}/opt

COPY --from=flink-base /opt/flink/opt/*.jar ${FLINK_HOME}/lib/

COPY ./jars/flink-table-api-scala-bridge_2.12-1.17.2.jar ${FLINK_HOME}/lib/

RUN rm -f /opt/flink/lib/flink-table-planner-loader-1.17.2.jar

ENV FLINK_VERSION=1.17.2
ENV SCALA_VERSION=2.12

# Define the URL for the flink-table-api-scala-bridge JAR file
# Get the exact download URL from the Maven repository link (see step 2 for details)
# ENV SCALA_BRIDGE_URL=repo.maven.apache.org{SCALA_VERSION}/${FLINK_VERSION}/flink-table-api-scala-bridge_${SCALA_VERSION}-${FLINK_VERSION}.jar

# Download the JAR file and place it in the Flink lib directory
# RUN apt-get update && apt-get install -y wget --no-install-recommends && \
#     wget "$SCALA_BRIDGE_URL" -O $FLINK_HOME/lib/flink-table-api-scala-bridge_${SCALA_VERSION}-${FLINK_VERSION}.jar && \
#     apt-get purge -y wget && \
#     rm -rf /var/lib/apt/lists/*

# Set correct permissions and ownership (the Zeppelin user likely needs access)
# We change ownership to the 'zeppelin' user if it exists, otherwise leave as root.
# For simplicity, we just ensure permissions are open enough for the non-root user.
RUN chown -R 1000:1000 ${FLINK_HOME} || true # 1000 is a common user ID
RUN chmod -R a+rwx ${FLINK_HOME}

# Switch back to the non-root user defined by the base image for security
# The 'zeppelin' user is likely ID 1000. We'll rely on the base image's final USER instruction.
# --- FIX END ---