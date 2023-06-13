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
	} else if r.URL.Path == "/profile2" && r.Method == "POST" {
		batchProfile2(w,r)
		return
	} else if r.URL.Path == "/calculate" && r.Method == "POST" {
		requestServing(w, r)
		return
	} else if r.URL.Path == "/lookup" && r.Method == "POST" {
		localEmbed(w, r)
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
				log.Println("unknown key ", keys[i])
			}
		}
	}
	//log.Println(local_data)
	//json.NewEncoder(w).Encode(tmp)
	//newData, err := json.Marshal(post)
	var wg sync.WaitGroup
	wg.Add(len(remote_data)+1)
	var local_result [][]float32
	go func() {
		defer wg.Done()
		for _, lk := range local_data {
			if lv, ok := Local_emb[lk]; ok{
				local_result = append(local_result, lv)
			}
		}
	}()
	var remote_result sync.Map
	//remote_result := make(map[int][][]float32)
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
				//if resp, err := http.Post(CubeIps[k]+"/DictService/seek", "application/json", bytes.NewReader(newData)); err == nil {
				if resp, err := http.Post(CubeIps[k]+"/lookup", "application/json", bytes.NewReader(newData)); err == nil {
					result, _ := ioutil.ReadAll(resp.Body)
					var res_data [][]float32
					_ = json.Unmarshal(result, &res_data)
					//fmt.Println(res_data)
					//remote_result[k] = res_data
					remote_result.Store(k, res_data)
				} else {
					log.Println("error")
				}
			}
		}(k)
	}
	wg.Wait()
	tmp := make(map[string][][]float32)
	for k,_ := range remote_data {
		//local_result = append(local_result, remote_result[k]...)
		data, _ := remote_result.Load(k)
		var fdata [][]float32
		fdata = data.([][]float32)
		local_result = append(local_result, fdata...)
	}
	tmp["result"] = local_result
	err := json.NewEncoder(w).Encode(local_result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func localEmbed(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	type reqData struct {
		Keys    []int `json:"keys"`
	}
	var data reqData
	_ = json.Unmarshal(body, &data)
	keys := data.Keys
	var localResult [][]float32
	for _, lk := range keys {
		if lv, ok := Local_emb[lk%1000000]; ok{
			localResult = append(localResult, lv)
		}
	}
	err := json.NewEncoder(w).Encode(localResult)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
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
		var local_result [][]float32
		if pattern == 0 {
			for _, lk := range keys {
				if lv, ok := Local_emb[lk]; ok{
					local_result = append(local_result, lv)
				}
			}
			result := make(map[string]interface{})
			elapsed := time.Since(start)
			result["time"] = elapsed
			//log.Println(result)
			json.NewEncoder(w).Encode(result)
		//} else if pattern == 1 {
		//	//var wg sync.WaitGroup
		//	//wg.Add(pattern+1)
		//	//var local_data []int
		//	for i:=0;i<len(keys)/2;i++{
		//		if lv, ok := Local_emb[keys[i]]; ok{
		//			local_result = append(local_result, lv)
		//		}
		//	}
		//	tmp := make(map[string][]int)
		//	tmp["keys"] = keys[len(keys)/2:]
		//	newData, _ := json.Marshal(tmp)
		//	if resp, err := http.Post(CubeIps[1]+"/lookup", "application/json", bytes.NewReader(newData)); err == nil {
		//		body,_ := ioutil.ReadAll(resp.Body)
		//		fmt.Println(string(body))
		//	} else {
		//		log.Println("error")
		//	}
		//	result := make(map[string]interface{})
		//	elapsed := time.Since(start)
		//	result["time"] = elapsed
		//	log.Println(result)
		//	json.NewEncoder(w).Encode(result)
		} else if  pattern >= 1{
			var remote_result sync.Map
			var wg sync.WaitGroup
			wg.Add(pattern+1)
			go func() {
				defer wg.Done()
				for i:=0;i<len(keys)/3*2;i++{
					if lv, ok := Local_emb[keys[i]]; ok{
						local_result = append(local_result, lv)
					}
				}
			}()
			for i:=1;i<=pattern;i++ {
				go func(i int) {
					defer wg.Done()
					tmp := make(map[string][]int)
					tmp["keys"] = keys[:len(keys)/3/pattern]
					newData, err := json.Marshal(tmp)
					if err != nil {
						log.Println(err)
					} else {
						//r.Body = ioutil.NopCloser(bytes.NewBuffer(newData))
						//req, err := http.Post(remote.Host, bytes.NewReader(newData))
						//log.Println(CubeIps[i]+"/DictService/seek")
						if resp, err := http.Post(CubeIps[i]+"/lookup", "application/json", bytes.NewReader(newData)); err == nil {
							result, _ := ioutil.ReadAll(resp.Body)
							var res_data [][]float32
							_ = json.Unmarshal(result, &res_data)
							//fmt.Println(res_data)
							//remote_result[k] = res_data
							remote_result.Store(i, res_data)
						} else {
							log.Println("error")
						}
					}
				}(i%4)
			}
			wg.Wait()
			result := make(map[string]interface{})
			elapsed := time.Since(start)
			result["time"] = elapsed
			//log.Println(result)
			json.NewEncoder(w).Encode(result)
		} else {
			return
		}
	}
}

func batchProfile2(w http.ResponseWriter, r *http.Request) {
	if body, err := ioutil.ReadAll(r.Body); err != nil{
		fmt.Fprintf(w, "ParseForm() err: %v", err)
		return
	} else {
		type profile_data struct {
			Keys    []int `json:"keys"`
			Pattern float32   `json:"pattern"`
		}
		var data profile_data
		start := time.Now()
		_ = json.Unmarshal(body, &data)
		//log.Println(data.Pattern)
		//log.Println(data.Keys)
		keys := data.Keys
		pattern := data.Pattern
		var local_result [][]float32
		var remote_result sync.Map
		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			for i:=0;i<int(float32(len(keys))*pattern);i++{
				if lv, ok := Local_emb[keys[i]]; ok{
					local_result = append(local_result, lv)
				}
			}
		}()
		for i:=1;i<=3;i++ {
			go func(i int) {
				defer wg.Done()
				tmp := make(map[string][]int)
				tmp["keys"] = keys[:int(float32(len(keys))*(1-pattern)/3)]
				newData, err := json.Marshal(tmp)
				if err != nil {
					log.Println(err)
				} else {
					//r.Body = ioutil.NopCloser(bytes.NewBuffer(newData))
					//req, err := http.Post(remote.Host, bytes.NewReader(newData))
					//log.Println(CubeIps[i]+"/DictService/seek")
					if resp, err := http.Post(CubeIps[i]+"/lookup", "application/json", bytes.NewReader(newData)); err == nil {
						result, _ := ioutil.ReadAll(resp.Body)
						var res_data [][]float32
						_ = json.Unmarshal(result, &res_data)
						//fmt.Println(res_data)
						//remote_result[k] = res_data
						remote_result.Store(i, res_data)
					} else {
						log.Println("error")
					}
				}
			}(i%4)
		}
		wg.Wait()
		result := make(map[string]interface{})
		elapsed := time.Since(start)
		result["time"] = elapsed
		//log.Println(result)
		json.NewEncoder(w).Encode(result)

	}
}

func requestServing(w http.ResponseWriter, r *http.Request) {
	log.Println(123)
	batch := 8
	embNum := 416
	var data [][]float32
	for i:=0;i<batch;i++ {
		var data_i []float32
		for j:=0;j< embNum;j++ {
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
