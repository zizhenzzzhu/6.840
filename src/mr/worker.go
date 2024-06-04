package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

/*
第一部分解读，创建KeyValue类，ByKey类是一个KeyValue类型的数组
定义了三个接口，返回数组长度，交换数组元素位置和返回小于指定位数的数组
*/
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

/*
第二部分：哈希函数，通过FNV哈希算法计算出字符串转换的哈希值，用于确定这个KV应该分配到哪个Reduce任务
*/

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/*
第三部分，Worker函数主体。循环从主节点获取任务根据任务类型（Map或者Reduce）调用相应的函数doMap或者doReduce
*/
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(ioutil.Discard)
	for {
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}
		log.Println("Calling Coordinator.GetTask...")
		call("Coordinator.GetTask", args, reply)
		job := reply.Job
		if job.JobType == NoJob {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Println("Get a Map job, working...")
			log.Printf("File names include: %v", job.FileNames[0])
			doMap(job, reply.TaskId, mapf)
		case ReduceJob:
			log.Println("Get a Reduce job, working...")
			log.Printf("File names include: %v, %v, %v", job.FileNames[0], job.FileNames[1], job.FileNames[2])
			doReduce(reply.Job, reply.TaskId, reducef)
		}
	}
}

/*
第四部分：doMap函数，如果任务是Map。打开读取文件调用函数mapf生成KV切片kva。将KV按K顺序排序
将KV发送到临时文件（根据哈希值和Reduce任务数量）重命名临时文件标识他们属于哪个Map和Reduce
通知主节点任务完成
*/
func doMap(job Job, taskId string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(job.FileNames[0])
	if err != nil {
		log.Fatalf("cannot open %v", job.FileNames)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.FileNames)
	}
	file.Close()
	kva := mapf(job.FileNames[0], string(content))
	sort.Sort(ByKey(kva))
	tmps := make([]*os.File, job.NReduce)
	for i := 0; i < job.NReduce; i++ {
		tmps[i], err = ioutil.TempFile("./", "temp_map_")
		if err != nil {
			log.Fatal("cannot create temp file")
		}
	}
	defer func() {
		for i := 0; i < job.NReduce; i++ {
			tmps[i].Close()
		}
	}()

	for _, kv := range kva {
		hash := ihash(kv.Key) % job.NReduce
		fmt.Fprintf(tmps[hash], "%v %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < job.NReduce; i++ {
		taskIdentifier := strings.Split(job.FileNames[0], "-")[1]
		os.Rename(tmps[i].Name(), "mr-"+taskIdentifier+"-"+strconv.Itoa(i))
	}

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finishes, calling Coordinator.ReportSuccess")
	log.Printf("JobType is %v", job.JobType)
	call("Coordinator.ReportSuccess", newArgs, newReply)
}

/*
第五部分doReduce函数。执行Reduce任务，打开读取所有Map任务的输出文件，将文件内容解析为KV对并存放在kvas切片中
多路归并处理KV，调用reducef生成最终结果，将结果写入临时文件。重命名临时文件标识Reduce任务，通知主节点完成
*/
func doReduce(job Job, taskId string, reducef func(string, []string) string) {
	kvas := make([][]KeyValue, len(job.FileNames))
	for i, fileName := range job.FileNames {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			tokens := strings.Split(scanner.Text(), " ")
			kvas[i] = append(kvas[i], KeyValue{tokens[0], tokens[1]})
		}
	}
	ids := make([]int, len(job.FileNames))
	ofile, _ := ioutil.TempFile("./", "temp_reduce_")
	defer ofile.Close()
	values := []string{}
	prevKey := ""
	for {
		findNext := false
		var nextI int
		for i, kva := range kvas {
			if ids[i] < len(kva) {
				if !findNext {
					findNext = true
					nextI = i
				} else if strings.Compare(kva[ids[i]].Key, kvas[nextI][ids[nextI]].Key) < 0 {
					nextI = i
				}
			}
		}
		if findNext {
			nextKV := kvas[nextI][ids[nextI]]
			if prevKey == "" {
				prevKey = nextKV.Key
				values = append(values, nextKV.Value)
			} else {
				if nextKV.Key == prevKey {
					values = append(values, nextKV.Value)
				} else {
					fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
					prevKey = nextKV.Key
					values = []string{nextKV.Value}
				}
			}
			ids[nextI]++
		} else {
			break
		}
	}
	if prevKey != "" {
		fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
	}
	taskIdentifier := strings.Split(job.FileNames[0], "-")[2]
	os.Rename(ofile.Name(), "mr-out-"+taskIdentifier)

	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finishes, calling Coordinator.ReportSuccess")
	log.Printf("JobType is %v", job.JobType)
	call("Coordinator.ReportSuccess", newArgs, newReply)
}

/*
最后一步call函数建立与主节点的RPC通信
*/
func call(rpcname string, args interface{}, reply interface{}) bool {

	sockname := coordinateSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
