package observe

import (
	"fmt"
	"strconv"
	"testing"
)

type testInputItem struct {
	Id    int
	Value string
}

func (i testInputItem) GetKey() []string {
	return []string{strconv.Itoa(i.Id)}
}

type testOuputItem struct {
	Id      int
	Value1  string
	Value2  string
	Value3  string
	Value4  string
	Value5  string
	Value6  string
	Value7  string
	Value8  string
	Value9  string
	Value10 string
}

func TestStage(t *testing.T) {

	in1 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In1"},
			testInputItem{Id: 2, Value: "In1"},
			testInputItem{Id: 3, Value: "In1"},
		},
	)

	in2 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In2"},
			testInputItem{Id: 3, Value: "In2"},
		},
	)

	in3 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In3"},
		},
	)
	in4 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In4"},
		},
	)
	in5 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In5"},
		},
	)
	in6 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In6"},
		},
	)
	in7 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In7"},
		},
	)
	in8 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In8"},
		},
	)
	in9 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In9"},
		},
	)
	in10 := Array[testInputItem](
		[]testInputItem{
			testInputItem{Id: 1, Value: "In10"},
		},
	)

	sut := Stage10(
		in1, in2, in3, in4, in5, in6, in7, in8, in9, in10,
		func(in1, in2, in3, in4, in5, in6, in7, in8, in9, in10 *testInputItem, out testOuputItem) (*testOuputItem, error) {
			if in1 != nil {
				out.Value1 = in1.Value
				out.Id = in1.Id
			}
			if in2 != nil {
				out.Value2 = in2.Value
				out.Id = in2.Id
			}
			if in3 != nil {
				out.Value3 = in3.Value
				out.Id = in3.Id
			}
			if in4 != nil {
				out.Value4 = in4.Value
				out.Id = in4.Id
			}
			if in5 != nil {
				out.Value5 = in5.Value
				out.Id = in5.Id
			}
			if in6 != nil {
				out.Value6 = in6.Value
				out.Id = in6.Id
			}
			if in7 != nil {
				out.Value7 = in7.Value
				out.Id = in7.Id
			}
			if in8 != nil {
				out.Value8 = in8.Value
				out.Id = in8.Id
			}
			if in9 != nil {
				out.Value9 = in9.Value
				out.Id = in9.Id
			}
			if in10 != nil {
				out.Value10 = in10.Value
				out.Id = in10.Id
			}
			return &out, nil
		},
	)

	out := sut.ToResult()

	fmt.Println(out)
}
