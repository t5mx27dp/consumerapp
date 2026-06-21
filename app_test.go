package consumerapp_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	appmock "github.com/t5mx27dp/app/mock"

	"github.com/t5mx27dp/consumerapp"
	"github.com/t5mx27dp/consumerapp/message"
	consumerappmock "github.com/t5mx27dp/consumerapp/mock"
)

type Message struct {
	Body []byte
}

var _ (message.Message) = (*Message)(nil)

func (m *Message) GetBody() []byte {
	return m.Body
}

func (m *Message) Ack() error {
	return nil
}

func (m *Message) Nack() error {
	return nil
}

func (m *Message) Requeue() error {
	return nil
}

const (
	Hello message.Queue = "hello"
)

func TestApp(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var rwch = make(chan message.Message)
		var rch <-chan message.Message = rwch

		logger := appmock.NewLogger(t)
		consumer := consumerappmock.NewConsumer(t)

		logger.On("Log", mock.Anything, mock.Anything, mock.Anything).Return()

		consumer.On("GetName").Return("consumer")
		consumer.On("Consume", mock.Anything, mock.Anything).Return(rch, nil)

		consumers := []consumerapp.Consumer{consumer}

		handlers := map[message.Queue]consumerapp.Handler{
			Hello: func(ctx context.Context, message message.Message) {
				require.Equal(t, "hello", string(message.GetBody()))
			},
		}

		app := consumerapp.New(ctx, logger, consumers, handlers)

		go func() {
			rwch <- &Message{
				Body: []byte("hello"),
			}

			close(rwch)
		}()

		err := app.Run()
		require.Nil(t, err)
	})
}
