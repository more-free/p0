// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
)

const (
	WORKER_OUT_BUFFER = 100
)

type worker struct {
	id              int
	conn            net.Conn
	clientReadChan  chan []byte
	clientWriteChan chan []byte // buffered
	closeChan       chan bool
}

type multiEchoServer struct {
	workerClose chan int      // receive id when a worker is closed
	workerAdd   chan *worker  // receive id when a worker is created
	workerCnt   chan chan int // receive query about worker count
	workerMsg   chan []byte   // receive message when a worker receives a message from its client
	masterClose chan bool
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	return &multiEchoServer{
		make(chan int),
		make(chan *worker),
		make(chan chan int),
		make(chan []byte),
		make(chan bool),
	}
}

func (mes *multiEchoServer) Start(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		fmt.Errorf("Error on listening", err)
		return err
	}
	fmt.Printf("Start listening on port %v\n", port)

	go mes.listenWorker(ln)
	go mes.listenClient(ln)
	return nil
}

// receive either data or status change from worker
func (mes *multiEchoServer) listenWorker(ln net.Listener) {
	workers := make(map[int]*worker)

	for {
		select {
		case id := <-mes.workerClose:
			delete(workers, id)

		case worker := <-mes.workerAdd:
			workers[worker.id] = worker

		case cntChan := <-mes.workerCnt:
			cntChan <- len(workers)

		case msg := <-mes.workerMsg:
			// echo to all workers
			for id, worker := range workers {
				select {
				case worker.clientWriteChan <- msg:
				default:
					fmt.Errorf("Out buffer is full on worker %v", id)
				}
			}

		case <-mes.masterClose:
			ln.Close()
			for _, worker := range workers {
				worker.closeChan <- true
			}
			return
		}
	}
}

func (mes *multiEchoServer) listenClient(ln net.Listener) {
	for id := 0; ; id++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Exiting client listener")
			break
		}

		worker := &worker{
			id,
			conn,
			make(chan []byte),
			make(chan []byte, WORKER_OUT_BUFFER),
			make(chan bool),
		}

		mes.workerAdd <- worker

		go worker.start(mes)
	}
}

func (w *worker) start(mes *multiEchoServer) {
	go w.readConn()
	go w.writeConn() // running in another go routine makes it non-blocking by slow clients

	closeSelf := func() {
		close(w.clientWriteChan)
		mes.workerClose <- w.id
	}

	for {
		select {
		case msg, more := <-w.clientReadChan:
			if !more {
				closeSelf()
				return
			}
			mes.workerMsg <- msg

		case <-w.closeChan:
			w.conn.Close()
			closeSelf()
			return
		}
	}
}

func (w *worker) readConn() {
	reader := bufio.NewReader(w.conn)
	for {
		line, err := reader.ReadBytes('\n')

		// either client closes itself actively or worker closes the conn
		if err != nil {
			close(w.clientReadChan)
			break
		} else {
			w.clientReadChan <- line
		}
	}
}

func (w *worker) writeConn() {
	for {
		select {
		case msg, more := <-w.clientWriteChan:
			if !more {
				return
			}
			_, err := w.conn.Write(msg)
			if err != nil {
				fmt.Errorf("Error on writing data %v", err)
			}
		}
	}
}

func (mes *multiEchoServer) Close() {
	mes.masterClose <- true
}

func (mes *multiEchoServer) Count() int {
	cntChan := make(chan int)
	mes.workerCnt <- cntChan
	return <-cntChan
}
