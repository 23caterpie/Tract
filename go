WORKSPACE=/go/src/${PWD#"$GOPATH/src/"}
docker run --rm -v "$PWD":$WORKSPACE -w $WORKSPACE ccurrin23/gotip:latest gotip $@
