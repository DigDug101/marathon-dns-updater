FROM golang:1.9-alpine

RUN apk --update add ca-certificates git
WORKDIR /go/src/app
COPY . .
RUN go-wrapper download && go-wrapper install

CMD ["go-wrapper", "run"]
