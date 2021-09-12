package channelchainer

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestChannelChainer(t *testing.T) {
	type args struct {
		options    Options
		processors []struct {
			Weight    int8
			Processor Processor
		}
		processes []struct {
			Weight  int8
			Process ProcessFunc
		}
		input []interface{}
	}
	type want struct {
		output  []interface{}
		wantErr bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{"No Input", args{options: Options{}}, want{wantErr: false}},

		{"Primitive input, processes", args{options: Options{}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return in.(int) + 1, nil
			}},
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return in.(int) + 1, nil
			}},
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return in.(int) + 1, nil
			}},
		}, input: []interface{}{
			0, 0, 0,
		}}, want{output: []interface{}{
			3, 3, 3,
		}, wantErr: false}},

		{"Primitive input, processors", args{options: Options{}, processors: []struct {
			Weight    int8
			Processor Processor
		}{
			{0, func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case v, ok := <-in:
						if !ok {
							return nil
						}
						out <- v.(int) + 1
					}
				}
			}},
			{0, func(ctx context.Context, in <-chan interface{}, out chan<- interface{}) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case v, ok := <-in:
						if !ok {
							return nil
						}
						out <- v.(int) + 1
					}
				}
			}},
		}, input: []interface{}{
			0, 0, 0,
		}}, want{output: []interface{}{
			2, 2, 2,
		}, wantErr: false}},

		{"Positive weights, processes", args{options: Options{}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{2, func(ctx context.Context, in interface{}) (interface{}, error) {
				return 2, nil
			}},
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return 0, nil
			}},
		}, input: []interface{}{
			1,
		}}, want{output: []interface{}{
			2,
		}, wantErr: false}},

		{"Negative weights, processes", args{options: Options{}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return 0, nil
			}},
			{-2, func(ctx context.Context, in interface{}) (interface{}, error) {
				return -2, nil
			}},
		}, input: []interface{}{
			1,
		}}, want{output: []interface{}{
			0,
		}, wantErr: false}},

		{"Don't skip nil, processes", args{options: Options{SkipNil: false}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return 1, nil
			}},
		}, input: []interface{}{
			nil,
		}}, want{output: []interface{}{
			1,
		}, wantErr: false}},

		{"Skip nil, processes", args{options: Options{SkipNil: true}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				if _, ok := in.(int); !ok {
					return nil, errors.New("")
				}
				return nil, nil
			}},
		}, input: []interface{}{
			nil, 1,
		}}, want{output: []interface{}{}, wantErr: false}},

		{"Context closing", args{options: Options{SkipNil: true}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				time.Sleep(time.Second)
				return nil, nil
			}},
		}, input: []interface{}{
			1, 1, 1, 1, 1,
		}}, want{output: []interface{}{}, wantErr: true}},

		{"Return error", args{options: Options{}, processes: []struct {
			Weight  int8
			Process ProcessFunc
		}{
			{0, func(ctx context.Context, in interface{}) (interface{}, error) {
				return nil, errors.New("")
			}},
		}, input: []interface{}{
			1, 1, 1, 1, 1,
		}}, want{output: []interface{}{}, wantErr: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.TODO(), time.Now().Add(time.Second*2))
			defer cancel()

			start := make(chan struct{})

			c := NewChannelChainer(tt.args.options)

			for _, p := range tt.args.processors {
				c = c.AddProcessorWithWeight(p.Weight, p.Processor)
			}

			for _, p := range tt.args.processes {
				c = c.AddProcessWithWeight(p.Weight, p.Process)
			}

			in := make(chan interface{})
			go func(start <-chan struct{}, input []interface{}, in chan<- interface{}) {
				defer close(in)
				<-start
				for _, i := range input {
					in <- i
				}
			}(start, tt.args.input, in)

			out := make(chan interface{})
			go func(ctx context.Context, out <-chan interface{}, output []interface{}) {
				var got []interface{}
				for v := range out {
					got = append(got, v)
				}

				if (len(got) != 0 || len(output) != 0) && !reflect.DeepEqual(got, output) {
					t.Errorf("output = %v, want %v", got, tt.want)
				}
			}(ctx, out, tt.want.output)

			close(start)

			if err := c.Run(ctx, in, out); (err != nil) != tt.want.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.want.wantErr)
			}
		})
	}
}
