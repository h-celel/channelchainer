package channelchainer

import (
	"context"
	"sort"
	"sync"
)

type ChannelChainer struct {
	options    Options
	processors []weightedProcessor
}

func NewChannelChainer(baseOptions Options) *ChannelChainer {
	return &ChannelChainer{options: baseOptions}
}

type Options struct {
	BufferSize uint
	SkipNil    bool
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
		f := c.generateDefaultProcessor(p)
		c.processors = append(c.processors, weightedProcessor{Weight: weight, Processor: f})
	}
	return c
}

func (c ChannelChainer) BufferSize(size uint) ChannelChainer {
	c.options.BufferSize = size
	return c
}

func (c ChannelChainer) SkipNil(b bool) ChannelChainer {
	c.options.SkipNil = b
	return c
}

func (c ChannelChainer) Run(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
	if !c.isRunnable() {
		return nil
	}

	ps := c.sortedProcessors()
	psLen := len(ps)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(psLen)

	start := make(chan struct{})
	aErr := newAtomicError(nil)

	csIn, csOut := c.generateChannels(psLen, in, out)

	for i, p := range ps {
		go processorRoutine(ctx, &wg, start, cancel, p, csIn[i], csOut[i], aErr)
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

func (c ChannelChainer) generateChannels(psLen int, in <-chan interface{}, out chan<- interface{}) ([]<-chan interface{}, []chan<- interface{}) {
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
	return csIn, csOut
}

func (c ChannelChainer) sortedProcessors() (res []Processor) {
	ps := make([]weightedProcessor, len(c.processors))
	copy(ps, c.processors)

	sort.SliceStable(ps, func(i, j int) bool {
		return ps[i].Weight < ps[j].Weight
	})

	res = make([]Processor, len(ps))
	for i, p := range ps {
		res[i] = p.Processor
	}

	return
}

func (c ChannelChainer) isRunnable() bool {
	return len(c.processors) > 0
}

func (c ChannelChainer) generateDefaultProcessor(p ProcessFunc) Processor {
	skipNil := c.options.SkipNil

	return func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
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
				if !skipNil || v != nil {
					out <- v
				}
			}
		}
	}
}

func processorRoutine(ctx context.Context, wg *sync.WaitGroup, start <-chan struct{}, cancel context.CancelFunc, p Processor, in <-chan interface{}, out chan<- interface{}, aErr *atomicError) {
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
}
