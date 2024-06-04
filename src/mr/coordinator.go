package mr

import (
	"errors"
	"github.com/google/uuid"
	_ "github.com/google/uuid"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*
解读第一步：定义结构体
*/
type Coordinator struct {
	jobs                []Job           //保存所有任务的切片
	rawFiles            []string        //输入文件列表
	reportChannelByUUID sync.Map        //通过UUID映射报告通道
	blockingJobNum      int             //正在执行的任务数
	availableJobs       chan Job        //Job类通道存放可用的任务
	successJobs         chan Job        //Job类通道存放成功完成的任务
	nReduce             int             //Reduce 任务的数量
	successJobsSet      map[string]bool //记录成功完成的任务集合
	isSuccess           bool            //表示所有任务是否成功完成
	mutex               sync.Mutex      //互斥锁，用于保护共享资源
	addReduce           bool            //标记是否已经添加了 Reduce 任务
}

/*
第二部分：处理成功任务,持续监听successJobs通道，处理成功完成的Map和Reduce任务，对于Map任务，判断所有的Map任务是否完成
如果完成那么执行Reduce。对于Reduce判断所有任务是否完成，如果完成则关闭响应通道并标记任务成功
*/
func (c *Coordinator) handleSuccessJobs() {
	for {
		job, ok := <-c.successJobs
		if !ok {
			break
		}
		switch job.JobType {
		case MapJob:
			// 处理 Map 任务完成的逻辑
		case ReduceJob:
			// 处理 Reduce 任务完成的逻辑
		}
	}
}

/*
第三步，获取任务的RPC处理。持续从avalJobs通道中获取任务分配给请求的Worker
为每一个任务生成一个唯一的UUID并创建一个报告通道
启动一个协程等待任务的完成报告或者超时重新加入队列
*/
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for {
		job, ok := <-c.availableJobs
		if !ok {
			*reply = GetTaskReply{Job: Job{JobType: NoJob, NReduce: c.nReduce}}
			return nil
		}
		reportChannel := make(chan Job)
		id := uuid.New().String()
		c.reportChannelByUUID.Store(id, reportChannel)
		*reply = GetTaskReply{Job: job, TaskId: id}
		go func() {
			select {
			case job := <-reportChannel:
				c.successJobs <- job
			case <-time.After(10 * time.Second):
				c.availableJobs <- job
			}
		}()
		return nil
	}
}

/*
第四步，报告任务成功的RPC处理，处理Worker报告任务成功的RPC调用，更具TaskID获取对应的报告通道并把任务报告发送到通道中
*/
func (c *Coordinator) ReportSuccess(args *ReportSuccessArgs, reply *ReportSuccessReply) error {
	value, ok := c.reportChannelByUUID.Load(args.TaskId)
	if !ok {
		return errors.New("cannot read given uuid")
	}
	reportChannel := value.(chan Job)
	reportChannel <- args.Job
	return nil
}

/*
第五步，注册 RPC 服务并启动 HTTP 服务器监听 Unix 套接字。
*/
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinateSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

/*
第六步检查任务是否完成
*/
func (c *Coordinator) Done() bool {

	return c.isSuccess

}

/*
第七步创建一个实例对象
*/
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		rawFiles:       files,
		blockingJobNum: nReduce,
		availableJobs:  make(chan Job, 100),
		successJobs:    make(chan Job, 100),
		nReduce:        nReduce,
		isSuccess:      false,
		successJobsSet: make(map[string]bool),
		addReduce:      false,
	}

	for _, fileName := range files {
		m.availableJobs <- Job{
			JobType:   MapJob,
			FileNames: []string{fileName},
			NReduce:   nReduce,
		}
	}
	go m.handleSuccessJobs()
	m.server()
	return &m
}
