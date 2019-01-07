package Position

const (
	EVENTIDENTITY_SEGMENT = 3
	EVENTIDENTITY_SPLIT   = 5
)

type EntryPosition struct {
	TimePosition
	Included    bool
	JournalName string
	Position    int64
	ServerId    int64
}

func NewEntryPosition(journalName string, position int64, timestamp int64, serverId int64, Included bool) (*EntryPosition) {
	entryPosition := &EntryPosition{
		TimePosition: TimePosition{timestamp},
		Included:     false,
		JournalName:  journalName,
		Position:     position,
		ServerId:     serverId,
	}
	return entryPosition
}
