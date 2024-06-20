package registry

import (
	"context"
	"github.com/pkg/errors"
	"time"
)

type Scheduler struct {
	NewTicker func() *time.Ticker
	OnTick    func(context.Context) error
	OnError   func(error)
	OnStop    func()
}

func (w *Scheduler) Schedule(stop <-chan struct{}) {
	ticker := w.NewTicker()
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithCancel(context.Background())
		// cancel is called at the end of the loop
		go func() {
			select {
			case <-stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		select {
		case <-ticker.C:
			select {
			case <-stop:
			default:
				if err := w.onTick(ctx); err != nil && !errors.Is(err, context.Canceled) {
					w.OnError(err)
				}
			}
		case <-stop:
			if w.OnStop != nil {
				w.OnStop()
			}
			// cancel will be called by the above goroutine
			return
		}
		cancel()
	}
}

func (w *Scheduler) onTick(ctx context.Context) error {
	defer func() {
		if cause := recover(); cause != nil {
			if w.OnError != nil {
				var err error
				switch typ := cause.(type) {
				case error:
					err = errors.WithStack(typ)
				default:
					err = errors.Errorf("%v", cause)
				}
				w.OnError(err)
			}
		}
	}()
	return w.OnTick(ctx)
}
