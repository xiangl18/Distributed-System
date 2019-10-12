package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	taskDone := make(chan bool)
	for id := 0; id < ntasks; id++ {
		id := id
		go func() {
			doTaskArgs := DoTaskArgs{mr.jobName, mr.files[id], phase, id, nios}
			reply := new(struct{})
			flag := false
			var regWorker string
			for flag != true {
				regWorker = <- mr.registerChannel
				rpcName := "Worker.DoTask"
				flag = call(regWorker, rpcName, doTaskArgs, reply)
			}
			taskDone <- true
			mr.registerChannel <- regWorker
		}()
	}
	for i := 0; i < ntasks; i++ {
		<- taskDone
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
