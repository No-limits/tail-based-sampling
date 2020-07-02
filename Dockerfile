FROM golang:latest

WORKDIR $GOPATH/src/tail-based-sampling
COPY . $GOPATH/src/tail-based-sampling

#RUN mkdir -p $GOPATH/src/golang.org/x/ \
#     && mv ./src/time/ $GOPATH/src/golang.org/x/ \
#     && cd $GOPATH/src/golang.org/x/ \
#     && ls -al  $GOPATH/src/golang.org/x/ \
#     && go install time \
#     && cd -

#RUN go get -u -v ./src/...

# set GOOS=linux
#RUN go build -o tail-based-sampling ./src
#RUN go build -o easyjson github.com/mailru/easyjson/easyjson
RUN chmod +x ./tail-based-sampling

RUN chmod +x ./start.sh
ENTRYPOINT ["/bin/bash", "./start.sh"]