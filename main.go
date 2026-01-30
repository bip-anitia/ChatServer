package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

func main() {
	list, err := net.Listen("tcp", "localhost:8000")
	if err != nil {
		log.Fatalf("Failed to listen on port 8000: %v", err)
		fmt.Println("Listening on localhost:8000")
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
	history := make([]message, 0)

	clients := make(map[client]bool)
	maxHistory := 10
	for {
		select {
		case msg := <-messages:
			for cli := range clients {
				cli <- msg
			}
			history = append(history, msg)
			if len(history) > maxHistory {
				history = history[len(history)-maxHistory:]
			}
		case cli := <-entering:
			for _, msg := range history {
				cli <- msg
			}
			clients[cli] = true
		case cli := <-leaving:
			delete(clients, cli)
			close(cli)
		}
	}

}

func handleConn(conn net.Conn) {
	nick := bufio.NewScanner(conn)

	ch := make(chan message)
	go clientWriter(conn, ch)
	fmt.Fprint(conn, "Enter your nickname: ")
	scan := nick.Scan()
	if scan == false {
		conn.Close()
		return
	}
	who := nick.Text()
	nickName := strings.TrimSpace(who)
	if nickName == "" {
		conn.Close()
		return
	}
	ch <- message{Nick: nickName, Text: "You are " + nickName}
	messages <- message{Nick: nickName, Text: nickName + " has arrived"}
	entering <- ch

	for nick.Scan() {
		messages <- message{Nick: nickName, Text: nick.Text()}
	}
	leaving <- ch
	messages <- message{Nick: nickName, Text: nickName + " has left"}
	conn.Close()

}

func clientWriter(conn net.Conn, ch <-chan message) {
	for msg := range ch {
		fmt.Fprintf(conn, "%s: %s\n", msg.Nick, msg.Text)
	}
}
