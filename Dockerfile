FROM rust as builder
WORKDIR /app

# Build deps. We replace our main.rs with a dummy.rs to avoid rebuilding the
# main executable, creating a cached layer for the dependencies.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src/
RUN echo "fn main() {}" > dummy.rs
RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
RUN cargo build --release --bin block-submission-service

# Build executable.
RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml
COPY src ./src
RUN cargo build --release --bin block-submission-service

# Build runtime image.
FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/block-submission-service /app/block-submission-service
ENTRYPOINT ["/app/block-submission-service"]
