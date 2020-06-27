FROM golang:latest

WORKDIR $GOPATH/src/tail-based-sampling
COPY . $GOPATH/src/tail-based-sampling
#RUN go build ./src

RUN chmod +x ./start.sh
ENTRYPOINT ["/bin/bash", "./start.sh"]