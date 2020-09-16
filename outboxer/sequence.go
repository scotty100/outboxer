package outboxer

// Sequence struct to control id sequence for an app
type Sequence struct {
	ID  string
	Seq int64
}
