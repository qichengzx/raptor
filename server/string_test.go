package server

import (
	"bytes"
	"log"
	"net"
	"testing"
)

var (
	conn net.Conn
)

func init() {
	var err error
	conn, err = net.Dial("tcp", "127.0.0.1:6380")
	if err != nil {
		log.Fatal(err)
	}
}

func TestSet(t *testing.T) {
	if _, err := conn.Write([]byte("set xxx aaa\n")); err != nil {
		t.Error("could not write payload to TCP server:", err)
	}

	out := make([]byte, 1024)
	if _, err := conn.Read(out); err == nil {
		if bytes.Compare(out, []byte("+OK")) == 0 {
			t.Error("response did match expected output")
		}
	} else {
		t.Error("could not read from connection")
	}
}


func TestGet(t *testing.T) {
	if _, err := conn.Write([]byte("get xxx\n")); err != nil {
		t.Error("could not write payload to TCP server:", err)
	}

	out := make([]byte, 1024)
	if _, err := conn.Read(out); err == nil {
		if bytes.Compare(out, []byte("aaa")) == 0 {
			t.Error("response did match expected output")
		}
	} else {
		t.Error("could not read from connection")
	}
}
