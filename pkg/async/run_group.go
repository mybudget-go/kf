package async

import (
	"errors"
	"fmt"
	"github.com/tryfix/log"
	"sync"
)

type Fn func(*Opts) error

type Opts struct {
	stopping  <-chan struct{}
	readyOnce sync.Once
	ready     chan struct{}
}

func (opts *Opts) Stopping() <-chan struct{} {
	return opts.stopping
}

func (opts *Opts) Ready() {
	opts.readyOnce.Do(func() {
		close(opts.ready)
	})
}

var ErrInterrupted = errors.New(`interrupted`)

type RunGroup struct {
	fns          []Fn
	wg           *sync.WaitGroup
	readyWg      *sync.WaitGroup
	stopping     chan struct{}
	stopped      chan struct{}
	shutDownOnce *sync.Once
	err          error
	logger       log.Logger
	shuttingDown bool
}

func NewRunGroup(logger log.Logger, fns ...Fn) *RunGroup {
	return &RunGroup{
		fns:          fns,
		wg:           new(sync.WaitGroup),
		readyWg:      new(sync.WaitGroup),
		stopping:     make(chan struct{}),
		stopped:      make(chan struct{}),
		shutDownOnce: &sync.Once{},
		logger:       logger.NewLog(log.Prefixed(`AsyncGroup`)),
	}
}

func (tg *RunGroup) Add(fn Fn) *RunGroup {
	tg.readyWg.Add(1)
	tg.fns = append(tg.fns, fn)
	return tg
}

func (tg *RunGroup) Run() error {
	notifyErrOnce := &sync.Once{}
	// run each task on a separate go-routine
	tg.wg.Add(len(tg.fns))

	for _, fn := range tg.fns {
		ready := make(chan struct{}, 1)
		go func() {
			<-ready
			tg.readyWg.Done()
		}()

		go func(fn Fn) {
			defer LogPanicTrace(tg.logger)

			opts := &Opts{
				stopping: tg.stopping,
				ready:    ready,
			}

			if err := fn(opts); err != nil {
				notifyErrOnce.Do(func() {
					tg.err = err
				})
				tg.notifyShutDown(err)
			}

			// when function returns make it ready anyway
			opts.Ready()
			tg.wg.Done()
		}(fn)
	}

	tg.wg.Wait()

	close(tg.stopped)

	return tg.err
}

func (tg *RunGroup) notifyShutDown(err error) {
	tg.shutDownOnce.Do(func() {
		if err != nil {
			tg.logger.Error(fmt.Sprintf(`Processes stopping due to %s`, err))
		} else {
			tg.logger.Info(`Interrupted, Processes stopping...`)
		}

		tg.shuttingDown = true
		close(tg.stopping)
	})
}

func (tg *RunGroup) Ready() error {
	tg.readyWg.Wait()
	if tg.err == nil && tg.shuttingDown {
		return ErrInterrupted
	}
	return tg.err
}

func (tg *RunGroup) Stop() {
	tg.notifyShutDown(nil)
	defer tg.logger.Info(`Processes stopped`)
	<-tg.stopped
}
