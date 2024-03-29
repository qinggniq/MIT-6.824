package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	var wg sync.WaitGroup
	workers := make([]string, 0, 2)
	doneChan := make([]chan struct{}, 0, 2)
	var mutex sync.Mutex
	newCond := *sync.NewCond(&mutex)
	go func() {

		for {
			worker := <-registerChan
			mutex.Lock()
			workers = append(workers, worker)
			done := make(chan struct{}, 1)
			doneChan = append(doneChan, done)
			doneChan[len(doneChan)-1] <- (struct{}{})
			if len(workers) == 1 {
				newCond.Broadcast()
			}
			mutex.Unlock()
		}
	}()

	for i := 0; i < ntasks; i++ {
		var args DoTaskArgs
		if phase == mapPhase {
			args = DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}
		} else {
			args = DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}
		}
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			var worker string
			var lenWork int
			var done chan struct{}
			mutex.Lock()
			if len(workers) == 0 {
				newCond.Wait()
			}
			lenWork = len(workers)
			worker = workers[idx%len(workers)]
			done = doneChan[idx%lenWork]
			mutex.Unlock()

			for {
				<-done
				if call(worker, "Worker.DoTask", args, nil) {
					done <- (struct{}{})
					break
				} else {
					done <- (struct{}{})
					idx++
					mutex.Lock()
					lenWork = len(workers)
					worker = workers[idx%len(workers)]
					done = doneChan[idx%lenWork]
					mutex.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
