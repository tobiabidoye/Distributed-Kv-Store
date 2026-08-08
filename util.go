package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

type logTopic string

const (
	dVote    logTopic = "VOTE"
	dLeader  logTopic = "LEAD"
	dTerm    logTopic = "TERM"
	dTimer   logTopic = "TIMR"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dCommit  logTopic = "CMIT"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dDrop    logTopic = "DROP"
	dInfo    logTopic = "INFO"
	dError   logTopic = "ERRO"
	dClient  logTopic = "CLNT"
	dTest    logTopic = "TEST"
	dTrace   logTopic = "TRCE"
	dWarn    logTopic = "WARN"
)

// Set via environment variable: DEBUG=1 ./kvnode ...
var Debug = os.Getenv("DEBUG") == "1"

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if Debug {
		relativeTime := time.Since(ProgramStart).Microseconds() / 100

		debugString := fmt.Sprintf("%v %s ", relativeTime, topic)
		fullDebug := debugString + format
		//have to spread interface slices
		log.Printf(fullDebug, a...)
	}
}
