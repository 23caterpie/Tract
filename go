WORKSPACE=/go/src/${PWD#"$GOPATH/src/"}
docker run --rm -v "$PWD":$WORKSPACE -w $WORKSPACE golang:1.18beta1 go $@
# docker run --rm -it -v "$PWD":$WORKSPACE -w $WORKSPACE golang:1.18beta1 bash

# example: go test
# ./go test -workfile=/go/src/github.com/23caterpie/Tract/go.local.work -v --race ./... ./urfavtract/...
