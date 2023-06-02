package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type handle struct {
	rank int
	ips [4]string
	//reverseProxy string
}


func calLocalizationPerformanceTest(){
	n_part := 4
	our_partition, our_hot_cache := loadPartition("our", n_part)
	start := time.Now()
	var aa [100000]int
	var bb [100000]bool
	for i:= 0;i<100000;i++ {
		a:=our_partition[i]
		aa[i]=a
		b:=our_hot_cache[0][i]
		bb[i]=b
	}
	elapsed := time.Since(start)
	fmt.Println("该函数执行完成耗时：", elapsed)
}


func (this *handle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.Path)
	if r.URL.Path == "/DictService/seek" && r.Method == "POST" {
		embedRequest(w,r)
		return
	} else if r.URL.Path == "/profile" && r.Method == "POST" {
		batchProfile(w,r)
		return
	} else if r.URL.Path == "/calculate" && r.Method == "POST" {
		requestServing(w, r)
		return
	}
}

func embedRequest(w http.ResponseWriter, r *http.Request){
	body, _ := ioutil.ReadAll(r.Body)
	type req_data struct {
		Keys    []int `json:"keys"`
		Pattern string   `json:"pattern"`
	}
	var data req_data
	//var data map[string][]int
	_ = json.Unmarshal(body, &data)
	keys := data.Keys
	pattern := data.Pattern
	var partition map[int]int
	var hot_cache map[int]map[int]bool
	if pattern == "our" {
		partition = Our_partition
		hot_cache = Our_hot_cache
	} else {
		partition = Het_partition
		hot_cache = Het_hot_cache
	}
	//log.Println(keys)
	//var remote_data map[int][]int
	remote_data := make(map[int][]int)
	//remote_result := make(map[int][]string)
	var local_data []int
	for i := range keys {
		_, ok := hot_cache[Rank][keys[i]]
		if partition[keys[i]]==Rank || ok {
			local_data = append(local_data, keys[i])
		} else {
			pos, ok := partition[keys[i]]
			if ok {
				remote_data[pos] = append(remote_data[pos], keys[i])
			} else {
				log.Println("unknown key")
			}
		}
	}
	//log.Println(local_data)
	//json.NewEncoder(w).Encode(tmp)
	//newData, err := json.Marshal(post)
	var wg sync.WaitGroup
	wg.Add(len(remote_data))
	var local_result []string
	go func() {
		for _, lk := range local_data {
			if lv, ok := Local_emb[lk]; ok{
				local_result = append(local_result, lv)
			}
		}
	}()
	for k,_ := range remote_data {
		go func(k int) {
			defer wg.Done()
			tmp := make(map[string][]int)
			tmp["keys"] = remote_data[k]
			newData, err := json.Marshal(tmp)
			if err != nil {
				log.Println(err)
			} else {
				//r.Body = ioutil.NopCloser(bytes.NewBuffer(newData))
				//req, err := http.Post(remote.Host, bytes.NewReader(newData))
				if resp, err := http.Post(CubeIps[k]+"/DictService/seek", "application/json", bytes.NewReader(newData)); err == nil {
					ioutil.ReadAll(resp.Body)
					//fmt.Println(string(body))
				} else {
					log.Println("error")
				}
			}
		}(k)
	}
	wg.Wait()
	//tmp := make(map[string]string)
	//tmp["result"] = "success"
	json.NewEncoder(w).Encode(local_result)
}

func batchProfile(w http.ResponseWriter, r *http.Request) {
	if body, err := ioutil.ReadAll(r.Body); err != nil{
		fmt.Fprintf(w, "ParseForm() err: %v", err)
		return
	} else {
		type profile_data struct {
			Keys    []int `json:"keys"`
			Pattern int   `json:"pattern"`
		}
		var data profile_data
		start := time.Now()
		_ = json.Unmarshal(body, &data)
		//log.Println(data.Pattern)
		//log.Println(data.Keys)
		keys := data.Keys
		pattern := data.Pattern
		var local_result []string
		if pattern == 0 {

			for _, lk := range keys {
				if lv, ok := Local_emb[lk]; ok{
					local_result = append(local_result, lv)
				}
			}
			result := make(map[string]interface{})
			elapsed := time.Since(start)
			result["time"] = elapsed
			log.Println(result)
			json.NewEncoder(w).Encode(result)
		} else if pattern == 1 {
			//var local_data []int
			for i:=0;i<len(keys)/2;i++{
				if lv, ok := Local_emb[keys[i]]; ok{
					local_result = append(local_result, lv)
				}
			}
			tmp := make(map[string][]int)
			tmp["keys"] = keys[len(keys)/2:]
			newData, _ := json.Marshal(tmp)
			if resp, err := http.Post(CubeIps[1]+"/DictService/seek", "application/json", bytes.NewReader(newData)); err == nil {
				body,_ := ioutil.ReadAll(resp.Body)

				fmt.Println(string(body))
			} else {
				log.Println("error")
			}
			result := make(map[string]interface{})
			elapsed := time.Since(start)
			result["time"] = elapsed
			log.Println(result)
			json.NewEncoder(w).Encode(result)
			//var wg sync.WaitGroup
			//wg.Add(len(remote_data))
			//go func() {
			//	for _, lk := range local_data {
			//		if lv, ok := Local_emb[lk]; ok{
			//			local_result = append(local_result, lv)
			//		}
			//	}
			//}()
			//for k,_ := range remote_data {
			//	go func(k int) {
			//		defer wg.Done()
			//		tmp := make(map[string][]int)
			//		tmp["keys"] = remote_data[k]
			//		newData, err := json.Marshal(tmp)
			//		if err != nil {
			//			log.Println(err)
			//		} else {
			//			//log.Println(string(newData))
			//			//r.Body = ioutil.NopCloser(bytes.NewBuffer(newData))
			//			//req, err := http.Post(remote.Host, bytes.NewReader(newData))
			//			if resp, err := http.Post(CubeIps[k]+"/DictService/seek", "application/json", bytes.NewReader(newData)); err == nil {
			//				ioutil.ReadAll(resp.Body)
			//				//fmt.Println(string(body))
			//			} else {
			//				log.Println("error")
			//			}
			//		}
			//		//proxy := httputil.NewSingleHostReverseProxy(remote)
			//		//r.Host = remote.Host
			//		//proxy.ServeHTTP(w, r)
			//	}(k)
			//}
			//wg.Wait()
		} else if  pattern >= 2{
			var wg sync.WaitGroup
			wg.Add(pattern)
			for i:=0;i<len(keys)/2;i++{
				if lv, ok := Local_emb[keys[i]]; ok{
					local_result = append(local_result, lv)
				}
			}
			for i:=0;i<pattern;i++ {
				go func(i int) {
					defer wg.Done()
					tmp := make(map[string][]int)
					tmp["keys"] = keys[:len(keys)/2/pattern]
					newData, err := json.Marshal(tmp)
					if err != nil {
						log.Println(err)
					} else {
						//r.Body = ioutil.NopCloser(bytes.NewBuffer(newData))
						//req, err := http.Post(remote.Host, bytes.NewReader(newData))
						//log.Println(CubeIps[i]+"/DictService/seek")
						if resp, err := http.Post(CubeIps[i]+"/DictService/seek", "application/json", bytes.NewReader(newData)); err == nil {
							body,_ := ioutil.ReadAll(resp.Body)
							fmt.Println(string(body))
						} else {
							log.Println("error")
						}
					}
				}((i+1)%4)
			}
			wg.Wait()
			result := make(map[string]interface{})
			elapsed := time.Since(start)
			result["time"] = elapsed
			log.Println(result)
			json.NewEncoder(w).Encode(result)
		} else {
			return
		}
	}
}

func requestServing(w http.ResponseWriter, r *http.Request) {
	log.Println(123)
	batch := 8
	emb_num := 416
	var data [][]float32
	for i:=0;i<batch;i++ {
		var data_i []float32
		for j:=0;j<emb_num;j++ {
			data_i = append(data_i, rand.Float32())
		}
		data = append(data, data_i)
	}
	log.Println(data)
	keys := make(map[string][][]float32)
	keys["instances"] = data
	newData, _ := json.Marshal(keys)
	if resp, err := http.Post("http://127.0.0.1:8501/v1/models/wdl:predict", "application/json", bytes.NewReader(newData)); err == nil {
		body,_ := ioutil.ReadAll(resp.Body)
		fmt.Println(string(body))
	} else {
		log.Println("error")
	}
}
