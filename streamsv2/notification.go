package streamsv2

// NotificationKind
type NotificationKind string

const (
	// NextKind indicates the next value in the downstream
	NextKind NotificationKind = "NextKind"
	// ErrorKind indicates an error occurred
	ErrorKind NotificationKind = "ErrorKind"
	// CompleteKind indicates the downstream is complete
	CompleteKind NotificationKind = "CompleteKind"
)

type Notification[T any] interface {
	Kind() NotificationKind
	Value() T // returns the underlying value if it's a "Next" notification
	Err() error
	Done() bool
}

type notification[T any] struct {
	kind NotificationKind
	v    T
	err  error
	done bool
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

func (d notification[T]) Done() bool {
	return d.done
}

func Next[T any](v T) Notification[T] {
	return &notification[T]{kind: NextKind, v: v}
}

func Error[T any](err error) Notification[T] {
	return &notification[T]{kind: ErrorKind, err: err}
}

func Complete[T any]() Notification[T] {
	return &notification[T]{kind: CompleteKind, done: true}
}
