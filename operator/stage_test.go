package operator

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/utils"
	"github.com/redis/go-redis/v9"
)

type InputValue struct {
	Key   string
	Value string
}

type MergeValue struct {
	In1 *InputValue
	In2 *InputValue
	In3 *InputValue
}

type StagedValue struct {
	Key    string
	Value1 string
	Value2 string
	Value3 string
}

type hook struct {
}

func (h hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmds redis.Cmder) error {
		fmt.Println("REDIS: ", cmds)
		return next(ctx, cmds)
	}
}

func (h hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		fmt.Println("REDIS: ", cmds)
		return next(ctx, cmds)
	}
}

func TestStage(t *testing.T) {

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	//rdb.AddHook(hook{})

	ob1 := observe.Array(
		[]InputValue{
			{Key: "1", Value: "ID1:In1"},
			{Key: "2", Value: "ID2:In1"},
			{Key: "3", Value: "ID3:In1"},
		},
	)

	ob2 := observe.Array(
		[]InputValue{
			{Key: "1", Value: "ID1:In2"},
			{Key: "2", Value: "ID2:In2"},
		},
	)

	ob3 := observe.Array(
		[]InputValue{
			{Key: "1", Value: "ID1:In3"},
		},
	)

	merge := observe.MergeMap3(ob1, ob2, ob3, func(in1 *InputValue, in2 *InputValue, in3 *InputValue) (MergeValue, error) {
		merge := MergeValue{}
		if in1 != nil {
			merge.In1 = in1
		}

		if in2 != nil {
			merge.In2 = in2
		}

		if in3 != nil {
			merge.In3 = in3
		}
		return merge, nil
	})

	staged := Stage[MergeValue, StagedValue](
		func(value MergeValue) []string {
			in := utils.Coalesce(value.In1, value.In2, value.In3).(*InputValue)
			return []string{in.Key}
		}, func(value MergeValue, state StagedValue) (*StagedValue, error) {
			if value.In1 != nil {
				state.Key = value.In1.Key
				state.Value1 = value.In1.Value
			}
			if value.In2 != nil {
				state.Key = value.In2.Key
				state.Value2 = value.In2.Value
			}
			if value.In3 != nil {
				state.Key = value.In3.Key
				state.Value3 = value.In3.Value
			}
			return &state, nil
		},
		NewRedisStateStore[StagedValue](rdb),
	)(merge)

	for _, item := range staged.ToResult() {
		fmt.Println(item.Value())
	}

}
