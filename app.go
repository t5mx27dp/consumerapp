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
	Consume(ctx context.Context, queue message.Queue) (chan message.Message, error)
}

type Handler func(ctx context.Context, message message.Message)

type App struct {
	ctx context.Context
	wg  sync.WaitGroup

	logger app.Logger

	consumers []Consumer

	handlers map[message.Queue]Handler
}

func New(ctx context.Context, logger app.Logger, consumers []Consumer, handlers map[message.Queue]Handler, opts ...Option) *App {
	a := &App{
		ctx:       ctx,
		logger:    logger,
		consumers: consumers,
		handlers:  handlers,
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *App) Run() error {
	a.run()
	a.wg.Wait()
	return nil
}

func (a *App) run() {
	for _, consumer := range a.consumers {
		for queue, handler := range a.handlers {
			a.wg.Add(1)
			go a.consume(consumer, queue, handler)
		}
	}
}

func (a *App) consume(consumer Consumer, queue message.Queue, handler Handler) {
	defer a.wg.Done()

	ch, err := consumer.Consume(a.ctx, queue)
	if err != nil {
		a.logger.Error(a.ctx, err, nil)
		return
	}

	a.logger.Log(a.ctx, fmt.Sprintf("start %s consumer for queue %s", consumer.GetName(), queue), nil)
	defer a.logger.Log(a.ctx, fmt.Sprintf("stop %s consumer for queue %s", consumer.GetName(), queue), nil)

	for message := range ch {
		handler(a.ctx, message)
	}
}
