package message

type Queue string

type Message interface {
	GetBody() []byte
	Ack() error
	Nack() error
	Requeue() error
}
