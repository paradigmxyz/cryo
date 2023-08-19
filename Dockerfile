# syntax=docker/dockerfile:1.4

# Use rust:bookworm as the build environment
FROM rust:bookworm as build-environment

# Set non-interactive mode for apt so it doesn't ask for user input during the build
ENV DEBIAN_FRONTEND=noninteractive

# # Install required dependencies
# RUN apt-get update && apt-get install -y clang lld curl build-essential linux-generic git \
#     && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup.sh \
#     && chmod +x ./rustup.sh \
#     && ./rustup.sh -y

RUN apt-get update && apt-get install cmake -y
    
ARG TARGETARCH
WORKDIR /opt


# Add the required CFLAGS if the TARGETARCH matches
RUN [[ "$TARGETARCH" = "x86_64-unknown-linux-gnu" ]] && echo "export CFLAGS=-mno-outline-atomics" >> $HOME/.profile || true

WORKDIR /opt/cryo
COPY . .

RUN . $HOME/.profile && cargo build --bin cryo --release --locked \
    && mkdir out \
    && mv target/release/cryo  out/cryo \
    && strip out/cryo 

# Use debian:bookworm-slim for the client
FROM debian:bookworm-slim as cryo-client

ENV DEBIAN_FRONTEND=noninteractive

# Install required dependencies
# RUN apt-get update -y && apt-get install  linux-generic  git -y

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the build environment
COPY --from=build-environment /opt/cryo/out/cryo /usr/local/bin/cryo

# Add a user for cryo
RUN useradd -ms /bin/bash cryo

ENTRYPOINT ["cryo"]
