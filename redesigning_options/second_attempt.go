package redesigning_options

// I like this design a lot more, but it means you lose the ability to use variadic paramters in the function signature.
// At least you can create options that are scoped to a particular function though.

type OptionsApi struct {
	option1 string
	option2 string
}

func (o OptionsApi) WithOption1(param1 string) OptionsApi {
	o.option1 = param1
	return o
}

func (o OptionsApi) WithOption2(param2 string) OptionsApi {
	o.option2 = param2
	return o
}

func OperatorFn[TParam any](params1 TParam, optionsCallback ...func(options OptionsApi)) bool {
	opts := OptionsApi{}
	for _, callback := range optionsCallback {
		callback(opts)
	}
	return true
}

func init() {
	OperatorFn("test", func(options OptionsApi) {
		options.
			WithOption1("test").
			WithOption2("test")

	})

	OperatorFn("test")
}
