package consumerapp

import (
	"context"
	"fmt"
	"sync"

	"github.com/t5mx27dp/app"

	"github.com/t5mx27dp/consumerapp/message"
)

type Consumer interface {
	GetName() string
	Consume(ctx context.Context, queue message.Queue) (<-chan message.Message, error)
}

type Handler func(ctx context.Context, message message.Message)

type App struct {
	wg sync.WaitGroup

	logger app.Logger

	consumers []Consumer

	handlers map[message.Queue]Handler
}

func New(logger app.Logger, consumers []Consumer, handlers map[message.Queue]Handler, opts ...Option) *App {
	a := &App{
		logger:    logger,
		consumers: consumers,
		handlers:  handlers,
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *App) Run(ctx context.Context) error {
	for _, consumer := range a.consumers {
		for queue, handler := range a.handlers {
			a.wg.Add(1)
			go a.consume(ctx, consumer, queue, handler)
		}
	}

	a.wg.Wait()

	return nil
}

func (a *App) consume(ctx context.Context, consumer Consumer, queue message.Queue, handler Handler) {
	defer a.wg.Done()

	ch, err := consumer.Consume(ctx, queue)
	if err != nil {
		a.logger.Error(ctx, err, nil)
		return
	}

	a.logger.Log(ctx, fmt.Sprintf("start %s consumer for queue %s", consumer.GetName(), queue), nil)
	defer a.logger.Log(ctx, fmt.Sprintf("stop %s consumer for queue %s", consumer.GetName(), queue), nil)

	for message := range ch {
		handler(ctx, message)
	}
}
