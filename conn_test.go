package main

import (
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/sportsru/ios-sender/apns"
)

func TestConnect(t *testing.T) {
	hub := &Hub{}
	if hub != nil {
		t.Log("It's fine")
	}
}

func TestDisconnect(t *testing.T) {
	hub := &Hub{}
	if hub != nil {
		t.Log("It's fine")
	}
}

func TestErrorsPrint(t *testing.T) {
	var str, strU, strFmt string
	str = fmt.Sprintf("%s", apns.APNSErrorCode(0).String())
	if len(str) == 0 {
		t.Error("Expected non empty description for APNSErrorCode(0)")
	}
	t.Log("APNSErrorCode(0) non empty result")

	strU = str
	offset, count := 0, 0
	for len(strU) > 0 {
		count++
		r, size := utf8.DecodeRuneInString(strU)
		if r == utf8.RuneError {
			t.Errorf("Non unicode symbol in APNSErrorCode(0).String() result"+
				"offset=%v, symbol offset=%v", offset, count)
		}
		strU = strU[size:]
		offset += size
	}
	t.Log("APNSErrorCode(0) valid unicode")

	strFmt = fmt.Sprintf("%s", apns.APNSErrorCode(0))
	if strFmt != str {
		t.Error("Auto strigify not equal String() call")
	}
	t.Log("APNSErrorCode(0).String == fmt.Print(APNSErrorCode(0))")
}
