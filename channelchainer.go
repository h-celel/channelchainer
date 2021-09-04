package channelchainer

import (
	"context"
	"sort"
	"sync"
)

type Processor func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error
type ProcessFunc func(ctx context.Context, in interface{}) (interface{}, error)

type weightedProcessor struct {
	Processor
	Weight int8
}

type ChannelChainer struct {
	options    Options
	processors []weightedProcessor
}

func NewChannelChainer(baseOptions Options) *ChannelChainer {
	return &ChannelChainer{options: baseOptions}
}

type Options struct {
	BufferSize uint
}

func (c ChannelChainer) AddProcessor(processors ...Processor) ChannelChainer {
	return c.AddProcessorWithWeight(0, processors...)
}

func (c ChannelChainer) AddProcessorWithWeight(weight int8, processors ...Processor) ChannelChainer {
	for _, p := range processors {
		c.processors = append(c.processors, weightedProcessor{Weight: weight, Processor: p})
	}
	return c
}

func (c ChannelChainer) AddProcess(processes ...ProcessFunc) ChannelChainer {
	return c.AddProcessWithWeight(0, processes...)
}

func (c ChannelChainer) AddProcessWithWeight(weight int8, processes ...ProcessFunc) ChannelChainer {
	for _, p := range processes {
		f := func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case v, ok := <-in:
					if !ok {
						return nil
					}
					v, err := p(ctx, v)
					if err != nil {
						return err
					}
					out <- v
				}
			}
		}
		c.processors = append(c.processors, weightedProcessor{Weight: weight, Processor: f})
	}
	return c
}

func (c ChannelChainer) BufferSize(size uint) ChannelChainer {
	c.options.BufferSize = size
	return c
}

func (c ChannelChainer) Run(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
	if len(c.processors) == 0 {
		return nil
	}

	ps := make([]weightedProcessor, len(c.processors))
	copy(ps, c.processors)

	sort.SliceStable(ps, func(i, j int) bool {
		return ps[i].Weight < ps[j].Weight
	})
	psLen := len(ps)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(psLen)

	start := make(chan struct{})

	aErr := newAtomicError(nil)

	csIn := make([]<-chan interface{}, psLen)
	csIn[0] = in
	csOut := make([]chan<- interface{}, psLen)
	csOut[psLen-1] = out
	for i := 0; i < psLen-1; i++ {
		var ch chan interface{}
		if c.options.BufferSize == 0 {
			ch = make(chan interface{})
		} else {
			ch = make(chan interface{}, c.options.BufferSize)
		}
		csIn[i+1] = ch
		csOut[i] = ch
	}

	for i, p := range ps {
		go func(ctx context.Context, p Processor, in <-chan interface{}, out chan<- interface{}) {
			defer close(out)
			defer wg.Done()
			<-start
			err := p(ctx, in, out)
			if err != nil {
				aErr.Soft(err)
				cancel()
			}
			for range in {
			}
		}(ctx, p.Processor, csIn[i], csOut[i])
	}

	close(start)

	wg.Wait()
	return aErr.Get()
}

func (c ChannelChainer) RunAsync(ctx context.Context, in <-chan interface{}, out chan<- interface{}) <-chan error {
	err := make(chan error, 1)
	go func() {
		err <- c.Run(ctx, in, out)
	}()

	return err
}

type atomicError struct {
	m   sync.Mutex
	err error
}

func newAtomicError(err error) *atomicError {
	return &atomicError{err: err}
}

func (a *atomicError) Soft(err error) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.err == nil {
		a.err = err
	}
}

func (a *atomicError) Get() error {
	a.m.Lock()
	defer a.m.Unlock()
	return a.err
}
