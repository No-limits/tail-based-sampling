FROM golang:latest

WORKDIR $GOPATH/src/tail-based-sampling
COPY . $GOPATH/src/tail-based-sampling

RUN mkdir -p $GOPATH/src/golang.org/x/ \
     && mv ./src/time/ $GOPATH/src/golang.org/x/ \
     && cd $GOPATH/src/golang.org/x/ \
     && go install time \
     && cd - \
     && go get -u -v ./src/...

#RUN go build -o tail-based-sampling ./src
#RUN chmod +x ./tail-based-sampling

RUN chmod +x ./start.sh
ENTRYPOINT ["/bin/bash", "./start.sh"]