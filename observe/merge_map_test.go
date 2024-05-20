package observe_test

import (
	"fmt"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/utils"
)

func TestMergeMap(t *testing.T) {

	in1 := observe.Value(1)
	in2 := observe.Value(2)
	in3 := observe.Value(3)
	in4 := observe.Value(4)
	in5 := observe.Value(5)
	in6 := observe.Value(6)
	in7 := observe.Value(7)
	in8 := observe.Value(8)
	in9 := observe.Value(9)
	in10 := observe.Value(10)

	merged := observe.MergeMap10(
		in1,
		in2,
		in3,
		in4,
		in5,
		in6,
		in7,
		in8,
		in9,
		in10,
		func(in1, in2, in3, in4, in5, in6, in7, in8, in9, in10 *int) (string, error) {
			out := utils.Coalesce(in1, in2, in3, in4, in5, in6, in7, in8, in9, in10).(*int)

			return fmt.Sprintf("%d", *out), nil
		})

	out := merged.ToResult()

	for _, item := range out {
		fmt.Println(item.Value())
	}

}
