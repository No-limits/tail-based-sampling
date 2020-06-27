FROM golang:latest

WORKDIR $GOPATH/src/tail-based-sampling
COPY . $GOPATH/src/tail-based-sampling

RUN go get -u -v ./src/...

#RUN go build -o tail-based-sampling ./src
#RUN chmod +x ./tail-based-sampling

RUN chmod +x ./start.sh
ENTRYPOINT ["/bin/bash", "./start.sh"]