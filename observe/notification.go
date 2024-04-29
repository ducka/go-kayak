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
	Ok() bool
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

func (d notification[T]) Ok() bool {
	return d.err == nil
}

func Next[T any](v T) Notification[T] {
	return &notification[T]{kind: NextKind, v: v}
}

func Error[T any](err error, value ...T) Notification[T] {
	var v T
	if len(value) > 0 {
		v = value[0]
	}
	return &notification[T]{kind: ErrorKind, err: err, v: v}
}
