package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient   logTopic = "CLNT"
	dCommit   logTopic = "CMIT"
	dDrop     logTopic = "DROP"
	dError    logTopic = "ERRO"
	dInfo     logTopic = "INFO"
	dLeader   logTopic = "LEAD"
	dLog      logTopic = "LOG1"
	dLog2     logTopic = "LOG2"
	dPersist  logTopic = "PERS"
	dSnap     logTopic = "SNAP"
	dTerm     logTopic = "TERM"
	dTest     logTopic = "TEST"
	dTimer    logTopic = "TIMR"
	dTrace    logTopic = "TRCE"
	dVote     logTopic = "VOTE"
	dState    logTopic = "State"
	dVoteFail logTopic = "VoteFail"
	dHeart    logTopic = "HeartBeat"
	dWarn     logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var debugStartms int64

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	debugStartms = debugStart.UnixMilli()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	time := time.Since(debugStart).Milliseconds()
	//time /= 100
	prefix := fmt.Sprintf("%08d %v ", time, string(topic))
	format = prefix + format
	log.Printf(format, a...)
}

func Watch(topic logTopic, format string, t int64) {
	times := time.Since(debugStart).Milliseconds()
	delay := t - debugStartms
	//time /= 100
	prefix := fmt.Sprintf("%08d %08d %v ", times, delay, string(topic))
	format = prefix + format
	log.Printf(format)
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
