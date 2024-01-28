package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

const N = 3 // Number of processes
const secondsToAwaitPerProcess = 3

type Message struct {
	SN int // Sequence Number
	Pi int // Process ID
}

type Request struct {
	Message
}

//Priority queue implementation for requests

type PriorityQueue []Request

var messagesExchanged int = 0
var messagesExchangedMutex = sync.Mutex{}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// Higher priority requests will have a greater priority value
	return getGreaterPriorityRequest(pq[i], pq[j]) == pq[i]
}

func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(Request)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type Process struct {
	id                    int
	mySequenceNumber      int
	mySequenceNumberMutex sync.Mutex
	RV                    []bool
	RVMutex               sync.Mutex
	LRQ                   PriorityQueue
	LRQMutex              sync.Mutex
	currentRequest        Request
	currentRequestMutex   sync.Mutex
	LastRequestSatisfied  Request
	LastRequestMutex      sync.Mutex
	highestSequenceNumber int
	highestSNMutex        sync.Mutex
	requesting            bool
	requestingMutex       sync.Mutex
	deferredRequests      []Request
	deferredRequestsMutex sync.Mutex
	requestChan           chan Request
	replyChan             chan Request
	flushChan             chan Request
}

func getGreaterPriorityRequest(request1 Request, request2 Request) Request {
	if request1.SN < request2.SN {
		return request1
	} else if request1.SN > request2.SN {
		return request2
	} else {
		if request1.Pi < request2.Pi {
			return request1
		} else {
			return request2
		}
	}
}
func (p *Process) initialize() {
	p.mySequenceNumber = 1
	p.mySequenceNumberMutex = sync.Mutex{}
	p.RV = make([]bool, N)
	p.RVMutex = sync.Mutex{}
	p.LRQ = PriorityQueue{}
	p.LRQMutex = sync.Mutex{}
	p.LastRequestSatisfied = Request{}
	p.LastRequestMutex = sync.Mutex{}
	p.highestSequenceNumber = 1
	p.highestSNMutex = sync.Mutex{}
	p.requesting = false
	heap.Init(&p.LRQ)
	p.requestingMutex = sync.Mutex{}
	p.deferredRequests = make([]Request, 0)
	p.deferredRequestsMutex = sync.Mutex{}
	p.requestChan = make(chan Request)
	p.replyChan = make(chan Request)
	p.flushChan = make(chan Request)
}

func (p *Process) invokeMutualExclusion() {
	//fmt.Printf("Process %d is invoking mutual exclusion\n", p.id)
	p.requestingMutex.Lock()
	p.requesting = true
	p.requestingMutex.Unlock()
	//empty each other position in RV
	p.RVMutex.Lock()
	for i := range p.RV {
		if i == p.id {
			p.RV[i] = true
		} else {
			p.RV[i] = false
		}
	}
	p.RVMutex.Unlock()

	p.mySequenceNumberMutex.Lock()
	p.mySequenceNumber = p.highestSequenceNumber + 1
	p.mySequenceNumberMutex.Unlock()
	//empty LRQ
	p.LRQMutex.Lock()
	for p.LRQ.Len() > 0 {
		heap.Pop(&p.LRQ)
	}
	p.LRQMutex.Unlock()

	//make new request and insert it into queue
	p.mySequenceNumberMutex.Lock()
	request := Request{Message: Message{SN: p.mySequenceNumber, Pi: p.id}}
	p.mySequenceNumberMutex.Unlock()
	p.LRQMutex.Lock()
	heap.Push(&p.LRQ, request)
	p.LRQMutex.Unlock()

	p.currentRequestMutex.Lock()
	p.currentRequest = request
	p.currentRequestMutex.Unlock()
	// send request to all other processes
	for _, process := range processes {
		if process.id != p.id {
			p.sendRequest(process.id, request)
		}
	}
}
func (p *Process) sendRequest(destination int, request Request) {
	//fmt.Printf("Process %d is sending request to process %d\n", p.id, destination)
	messagesExchangedMutex.Lock()
	messagesExchanged++
	messagesExchangedMutex.Unlock()
	processes[destination].requestChan <- request
}

func (p *Process) receiveRequest(req Request) {
	//fmt.Printf("Process %d is receiving request from process %d\n", p.id, req.Pi)
	p.highestSNMutex.Lock()
	p.highestSequenceNumber = max(p.highestSequenceNumber, req.SN)
	p.highestSNMutex.Unlock()

	p.requestingMutex.Lock()
	if p.requesting {
		p.requestingMutex.Unlock()
		p.RVMutex.Lock()
		if !p.RV[req.Pi] {
			p.RVMutex.Unlock()
			p.LRQMutex.Lock()
			//push into LRQ in sorted order
			heap.Push(&p.LRQ, req)
			p.LRQMutex.Unlock()
			p.RVMutex.Lock()
			p.RV[req.Pi] = true
			p.RVMutex.Unlock()
			if p.checkExecuteCS() {
				p.executeCriticalSection()
			}
		} else {
			p.RVMutex.Unlock()
			p.deferredRequestsMutex.Lock()
			p.deferredRequests = append(p.deferredRequests, req)
			p.deferredRequestsMutex.Unlock()
		}
	} else {
		p.requestingMutex.Unlock()
		p.LastRequestMutex.Lock()
		p.sendReply(req.Pi, p.LastRequestSatisfied)
		p.LastRequestMutex.Unlock()
	}
}

func (p *Process) sendReply(destination int, lastRequestSatisfied Request) {
	//fmt.Printf("Process %d is sending reply to process %d\n", p.id, destination)
	messagesExchangedMutex.Lock()
	messagesExchanged++
	messagesExchangedMutex.Unlock()
	processes[destination].replyChan <- lastRequestSatisfied
}

func (p *Process) receiveReply(lastRequestSatisfied Request) {
	//fmt.Printf("Process %d is handling reply from process %d\n", p.id, reply.Pi)
	p.RVMutex.Lock()
	p.RV[lastRequestSatisfied.Pi] = true
	p.RVMutex.Unlock()
	// Remove all requests from LRQ that have priority greater than or equal to request
	p.LRQMutex.Lock()
	for p.LRQ.Len() > 0 {
		HighestPriorityRequest := heap.Pop(&p.LRQ).(Request)
		if !(getGreaterPriorityRequest(HighestPriorityRequest, lastRequestSatisfied) == HighestPriorityRequest || (HighestPriorityRequest.Pi == lastRequestSatisfied.Pi && HighestPriorityRequest.SN == lastRequestSatisfied.SN)) {
			heap.Push(&p.LRQ, HighestPriorityRequest)
			break
		}
	}
	p.LRQMutex.Unlock()
	if p.checkExecuteCS() {
		p.executeCriticalSection()
	}
}

func (p *Process) executeCriticalSection() {
	p.requestingMutex.Lock()
	p.requesting = false
	p.requestingMutex.Unlock()
	fmt.Printf("Process %d is executing critical section\n", p.id)
	time.Sleep(2 * time.Second) // Simulating critical section execution
	p.finishCriticalSection()
}

func (p *Process) finishCriticalSection() {
	fmt.Printf("Process %d has finished executing CS\n", p.id)
	p.currentRequestMutex.Lock()
	p.LastRequestMutex.Lock()
	p.LastRequestSatisfied = p.currentRequest
	p.currentRequestMutex.Unlock()
	LRQcopy := PriorityQueue{}
	heap.Init(&LRQcopy)
	p.LRQMutex.Lock()
	//send flush to next request in LRQ other than itself
	for p.LRQ.Len() > 0 {
		nextRequest := heap.Pop(&p.LRQ).(Request)
		LRQcopy.Push(nextRequest)
		if !(nextRequest.Pi == p.id && nextRequest.SN == p.LastRequestSatisfied.SN) {
			p.sendFlush(nextRequest.Pi, p.LastRequestSatisfied)
			break
		}
	}
	for LRQcopy.Len() > 0 {
		heap.Push(&p.LRQ, heap.Pop(&LRQcopy))
	}

	p.LRQMutex.Unlock()
	p.LastRequestMutex.Unlock()
	//send reply to deferred requests
	p.deferredRequestsMutex.Lock()
	for _, request := range p.deferredRequests {
		p.sendReply(request.Pi, p.LastRequestSatisfied)
	}
	p.deferredRequests = nil
	p.deferredRequestsMutex.Unlock()
}

func (p *Process) receiveFlush(request Request) {
	//fmt.Printf("Process %d is receiving flush from process %d\n", p.id, request.Pi)
	p.RVMutex.Lock()
	p.RV[request.Pi] = true
	p.RVMutex.Unlock()
	p.LRQMutex.Lock()
	for p.LRQ.Len() > 0 {
		//Remove all requests from LRQ that have priority greater than or equal to last request satisfied
		HighestPriorityRequest := heap.Pop(&p.LRQ).(Request)
		if !(getGreaterPriorityRequest(HighestPriorityRequest, request) == HighestPriorityRequest || (HighestPriorityRequest.Pi == request.Pi && HighestPriorityRequest.SN == request.SN)) {
			heap.Push(&p.LRQ, HighestPriorityRequest)
			break
		}
	}
	p.LRQMutex.Unlock()
	if p.checkExecuteCS() {
		p.executeCriticalSection()
	}
}

func (p *Process) sendFlush(destination int, request Request) {
	//fmt.Printf("Process %d is sending flush to process %d\n", p.id, destination)
	messagesExchangedMutex.Lock()
	messagesExchanged++
	messagesExchangedMutex.Unlock()
	processes[destination].flushChan <- request
}

func (p *Process) checkExecuteCS() bool {
	//fmt.Printf("Process %d is checking if it can execute CS\n", p.id)
	p.RVMutex.Lock()
	for k := range p.RV {
		if !p.RV[k] {
			p.RVMutex.Unlock()
			return false
		}
	}
	p.RVMutex.Unlock()

	p.LRQMutex.Lock()
	if len(p.LRQ) > 0 && p.LRQ[0].Pi == p.id {
		p.LRQMutex.Unlock()
		return true
	}
	p.LRQMutex.Unlock()
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var processes []*Process

func main() {
	processes = make([]*Process, N)
	fmt.Printf("Creating %d processes\n", N)

	for i := 0; i < N; i++ {
		processes[i] = &Process{id: i}
		processes[i].initialize()
	}

	// Simulate invocation of critical section for all processes
	for i := 0; i < N; i++ {
		go processes[i].invokeMutualExclusion()
	}
	for i := 0; i < N; i++ {
		//continuously process requests sent through these channels, in order of receiving
		i := i
		go func() {
			for {
				select {
				case request := <-processes[i].requestChan:
					processes[i].receiveRequest(request)
				case request := <-processes[i].replyChan:
					processes[i].receiveReply(request)
				case request := <-processes[i].flushChan:
					processes[i].receiveFlush(request)
				}
			}
		}()
	}

	// wait for all processes to finish executing critical section; timer strictly for demo purposes
	time.Sleep(N * secondsToAwaitPerProcess * time.Second)
	fmt.Printf("Total messages exchanged: %d\n", messagesExchanged)
}
