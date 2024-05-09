package operator

//
//
//import (
//	"github.com/ducka/go-kayak/observe"
//)
//
//type (
//	SelectorFunc[T any] func(item T) string
//
//	JoinType string
//)
//
//const (
//	InnerJoin JoinType = "left"
//	LeftJoin  JoinType = "left"
//	RightJoin JoinType = "right"
//	FullJoin  JoinType = "full"
//)
//
//// Joinable is an interface that must be implemented by the types that are to be joined.
////type Joinable interface {
////	GetKey() []string
////}
//
//type DataSource interface {
//	Upsert(item chan Joinable)
//	Delete(item chan Joinable)
//}
//
//type JoinStore interface {
//
//}
//
//type InMemoryJoinStore[TLeft, TRight any] struct {
//	store map[string]JoinState[TLeft, TRight]
//}
//
//type JoinState[TLeft, TRight any] struct {
//	left []TLeft
//	right []TRight
//}
//
//
//
//
//// JoinByKey
//// TemporalJoin
//// MatchedJoin
//
//// TODO: OperatorFunc is an inappropriate return type for this. It has to be Observable
//func StagedJoin[TLeft, TRight Joinable, TOut any](
//	jointType JoinType,
//	left *observe.Observable[TLeft],
//	leftKey SelectorFunc[TLeft],
//	right *observe.Observable[TRight],
//	rightKey SelectorFunc[TRight]
//) *observe.Observable[TOut] {
//
//	return observe.Operation()
//}
