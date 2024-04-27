package kayak

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
}

type notification[T any] struct {
	kind NotificationKind
	v    T
	err  error
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

func Next[T any](v T) Notification[T] {
	return &notification[T]{kind: NextKind, v: v}
}

func Error[T any](err error) Notification[T] {
	return &notification[T]{kind: ErrorKind, err: err}
}
