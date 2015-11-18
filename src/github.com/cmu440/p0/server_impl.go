package p0

import (
	"bufio"
	"fmt"
	"net"
)

const (
	CLIENT_WRITE_BUFFER = 100
)

type worker struct {
	id              int
	conn            net.Conn
	clientWriteChan chan []byte // buffered
}

type multiEchoServer struct {
	workerClose chan int     // receive id when a worker is closed
	workerAdd   chan *worker // receive id when a worker is created
	workerCnt   chan int     // receive query about worker count
	workerMsg   chan []byte  // receive message when a worker receives a message from its client
	closeChan   chan bool    // receive signal to close the server
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	return &multiEchoServer{
		make(chan int),
		make(chan *worker),
		make(chan int),
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

	go mes.handleEvents(ln)
	go mes.handleConn(ln)
	return nil
}

// main event handler for the master
func (mes *multiEchoServer) handleEvents(ln net.Listener) {
	workers := make(map[int]*worker)

	for {
		select {
		case id := <-mes.workerClose:
			delete(workers, id)

		case worker := <-mes.workerAdd:
			workers[worker.id] = worker

		case <-mes.workerCnt:
			mes.workerCnt <- len(workers)

		case msg := <-mes.workerMsg:
			// echo to all workers
			for id, worker := range workers {
				select {
				case worker.clientWriteChan <- msg:
				default:
					fmt.Errorf("Out buffer is full on worker %v", id)
				}
			}

		case <-mes.closeChan:
			ln.Close()
			for _, worker := range workers {
				worker.conn.Close()
				close(worker.clientWriteChan)
			}
			return
		}
	}
}

func (mes *multiEchoServer) handleConn(ln net.Listener) {
	for id := 0; ; id++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Exiting connection listener")
			break
		}

		worker := &worker{
			id,
			conn,
			make(chan []byte, CLIENT_WRITE_BUFFER),
		}

		mes.workerAdd <- worker

		go worker.readConn(mes)
		go worker.writeConn() // using a separate go routine to make it non-blocking for slow clients
	}
}

func (w *worker) readConn(mes *multiEchoServer) {
	reader := bufio.NewReader(w.conn)
	for {
		line, err := reader.ReadBytes('\n')

		// either client closes itself actively or master closes the connection
		if err != nil {
			fmt.Println("Connection is closing due to", err)
			w.closeSelf(mes)
			return
		} else {
			mes.workerMsg <- line
		}
	}
}

func (w *worker) writeConn() {
	for {
		select {
		case msg, more := <-w.clientWriteChan:
			if !more { // master close the connection (and so close the clientWriteChan)
				return
			}
			_, err := w.conn.Write(msg)
			if err != nil {
				fmt.Errorf("Error on writing data %v", err)
			}
		}
	}
}

func (w *worker) closeSelf(mes *multiEchoServer) {
	mes.workerClose <- w.id
	w.conn.Close()
	close(w.clientWriteChan)
}

func (mes *multiEchoServer) Close() {
	mes.closeChan <- true
}

func (mes *multiEchoServer) Count() int {
	mes.workerCnt <- -1
	return <-mes.workerCnt
}
