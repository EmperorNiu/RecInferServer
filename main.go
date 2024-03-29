package main

import (
	"bufio"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
)


var srv http.Server
var Our_partition map[int]int
var Our_hot_cache map[int]map[int]bool
var Het_partition map[int]int
var Het_hot_cache map[int]map[int]bool
var Local_emb map[int][]float32
var Our_no_cache_partition map[int]int
//var Our_partition_avazu map[int]int
//var Our_hot_cache_avazu map[int]map[int]bool
//var Het_partition_avazu map[int]int
//var Het_hot_cache_avazu map[int]map[int]bool

func loadPartition(file_name string, n_part int) (map[int]int, map[int]map[int]bool){
	partition := make(map[int]int)
	hot_cache := make(map[int]map[int]bool)
	for i:=0; i<n_part; i++{
		hot_cache[i] = make(map[int]bool)
	}
	path := "./partition_data/" + file_name + "/" + file_name + "_p.txt"
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		key, _ := strconv.Atoi(line[0])
		value, _ := strconv.Atoi(line[1])
		partition[key] = value
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	for i:=0; i<n_part; i++ {
		f_path := "./partition_data/" + file_name + "/" + file_name + "_h" + strconv.Itoa(i) + ".txt"
		data, err := os.ReadFile(f_path)
		if err != nil {
			log.Fatal(err)
		}
		hot_data := strings.Split(string(data), " ")
		for hot_i := range hot_data {
			key, _ := strconv.Atoi(hot_data[hot_i])
			hot_cache[i][key] = true
		}
	}
	return partition, hot_cache
}


func StartServer(bind string) {
	log.Println("port: ", bind)
	h := &handle{rank: Rank, ips: CubeIps}
	srv.Addr = bind
	srv.Handler = h
	//go func() {
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalln("ListenAndServe: ", err)
	}
	//}()
}

func StopServer() {
	if err := srv.Shutdown(nil) ; err != nil {
		log.Println(err)
	}
}

func main() {
	//cmd = parseCmd()
	n_part := 4
	Our_partition, Our_hot_cache = loadPartition("our", n_part)
	Het_partition, Het_hot_cache = loadPartition("het", n_part)
	//Our_partition_avazu, Our_hot_cache_avazu = loadPartition("our", n_part)
	//Het_partition_avazu, Het_hot_cache_avazu = loadPartition("het", n_part)
	Our_no_cache_partition, _ = loadPartition("our", n_part)
	log.Println("Rank: ", Rank)
	//log.Println(len(Our_partition))
	//key:=31
	//pos, ok := Our_partition[key]
	//if ok {
	//	log.Println(pos)
	//} else {
	//	log.Println("unknown key ", key)
	//}

	Local_emb = make(map[int][]float32) // 本机保存的嵌入参数
	for i:=0;i<1000000;i++ {
		for j:=0;j<16;j++ {
			//Local_emb[i] += strconv.FormatFloat(float64(rand.Float32()), 'f', 10, 32)
			Local_emb[i] = append(Local_emb[i], rand.Float32())
		}
	}
	StartServer("0.0.0.0:" + strconv.Itoa(Port))
}