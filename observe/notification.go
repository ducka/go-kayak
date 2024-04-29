package observe

// NotificationKind
type NotificationKind string

const (
	// NextKind indicates the next value in the downstream
	NextKind NotificationKind = "NextKind"
	// ErrorKind indicates an error occurred
	ErrorKind NotificationKind = "ErrorKind"
)

type Notification[T any] interface {
	Kind() NotificationKind
	Value() T // returns the underlying value if it's a "Next" notification
	Err() error
	HasError() bool
	HasValue() bool
}

type notification[T any] struct {
	kind     NotificationKind
	v        T
	err      error
	hasValue bool
}

var _ Notification[any] = (*notification[any])(nil)

func (d notification[T]) Kind() NotificationKind {
	return d.kind
}

func (d notification[T]) Value() T {
	return d.v
}

func (d notification[T]) Err() error {
	return d.err
}

func (d notification[T]) HasError() bool {
	return d.err == nil
}

func (d notification[T]) HasValue() bool {
	return d.hasValue
}

func Next[T any](v T) Notification[T] {
	return &notification[T]{kind: NextKind, v: v, hasValue: true}
}

func Error[T any](err error, value ...T) Notification[T] {
	var v T
	hasValue := false
	if len(value) > 0 {
		v = value[0]
		hasValue = true
	}
	return &notification[T]{kind: ErrorKind, err: err, v: v, hasValue: hasValue}
}
