package main

import(
	"github.com/go-ini/ini"
	"log"
	"strconv"
)

var (
	Port int
	Rank int
	CubeIps [4]string
)

func init() {
	Cfg,err := ini.Load("cube.ini")
	if err!=nil{
		log.Fatalf("Fail to parse 'cube.ini': %v", err)
	}
	Port, _ = Cfg.Section("").Key("HTTP_PORT").Int()
	Rank, _ = Cfg.Section("").Key("RANK").Int()
	loadConf(Cfg)
}

func loadConf(Cfg *ini.File) {
	sec,err := Cfg.GetSection("cube")
	if err!=nil {
		log.Fatalf("Fail to get section 'cube': %v", err)
	}
	for i:=0;i<4;i++ {
		CubeIps[i] = sec.Key("CUBE" + strconv.Itoa(i)).String()
	}
}