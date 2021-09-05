package channelchainer

import "context"

type Processor func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error

type ProcessFunc func(ctx context.Context, in interface{}) (interface{}, error)

type weightedProcessor struct {
	Processor
	Weight int8
}
