FROM ubuntu:26.04

ENV GO_VERSION=1.26.0 \
    APP_PORT=3000

RUN apt-get update && apt-get install -y \
    wget \
    git \
    make \
    gcc \
    musl-tools \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm go${GO_VERSION}.linux-amd64.tar.gz

ENV PATH=$PATH:/usr/local/go/bin \
    GOPATH=/go \
    GO111MODULE=on \
    CGO_ENABLED=0

WORKDIR /app

COPY go.* ./

RUN go mod download && go mod tidy

COPY . .

RUN go build -tags musl -ldflags="-s -w" -o main .

EXPOSE ${APP_PORT}

CMD ["./main"]
