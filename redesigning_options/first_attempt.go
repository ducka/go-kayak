package redesigning_options

/*
// Unfortunately scoping options to individual functions on the client won't work because you can't define function level generics on the
// client without also defining generics on the struct type. This just won't work for go-kayak.

type MyOption func(options MyOptions)

type MyOptions struct {
	myValue string
}

type BatchingFuncType[TParam any] func(params1 TParam, options ...MyOption) bool

func (f BatchingFuncType[TParam]) WithMyOption(myValue string) MyOption {
	return func(options MyOptions) {
		options.myValue = myValue
	}
}

func batchingFuncImpl[TParam any](params1 TParam, options ...MyOption) bool {
	return true
}

type Client struct {
	BatchingFunc BatchingFuncType
}

func NewClient() *Client {
	return &Client{
		BatchingFunc: batchingFuncImpl,
	}
}*/
