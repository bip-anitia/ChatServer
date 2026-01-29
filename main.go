package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func main() {
	list, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatalf("Failed to listen on port 8000: %v", err)
		panic(err)
	}
	go broadcaster()
	for {
		conn, err := list.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleConn(conn)
	}

}

type message struct {
	Text string
	Nick string
}

type client chan<- message

var (
	entering = make(chan client)
	leaving  = make(chan client)
	messages = make(chan message)
)

func broadcaster() {
	clients := make(map[client]bool)
	for {
		select {
		case msg := <-messages:
			for cli := range clients {
				cli <- msg
			}
		case cli := <-entering:
			clients[cli] = true
		case cli := <-leaving:
			delete(clients, cli)
			close(cli)
		}
	}
}

func handleConn(conn net.Conn) {
	ch := make(chan message)
	go clientWriter(conn, ch)
	who := conn.RemoteAddr().String()
	ch <- message{Nick: who, Text: "You are " + who}
	messages <- message{Nick: who, Text: who + " has arrived"}
	entering <- ch

	input := bufio.NewScanner(conn)
	for input.Scan() {
		messages <- message{Nick: who, Text: input.Text()}
	}
	leaving <- ch
	messages <- message{Nick: who, Text: who + " has left"}
	conn.Close()

}

func clientWriter(conn net.Conn, ch <-chan message) {
	for msg := range ch {
		fmt.Fprintf(conn, "%s: %s\n", msg.Nick, msg.Text)
	}
}
