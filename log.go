package kv

type LogAction uint16

const (
	LogDelete LogAction = iota
	LogNew
	LogPut
)

type Log interface {
	Close() error
	First() (uint64, error)
	Last() (uint64, error)
	Read(index uint64) (LogEntry, error)
	Write(index uint64, entry LogEntry) error
}

type LogEntry struct {
	Action LogAction
	Meta   []KVPair
}

func NewLogEntry(action LogAction, meta []KVPair) LogEntry {
	return LogEntry{
		Action: action,
		Meta:   meta,
	}
}
