SET CGO_ENABLED=0
SET GOOS=linux
SET GOARCH=amd64
go build -o inferServer
SET CGO_ENABLED=0 SET GOOS=linux SET GOARCH=amd64 go build

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o inferServer