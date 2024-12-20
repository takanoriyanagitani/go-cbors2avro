package util

import "context"

type IO[T any] func(context.Context) (T, error)

func Bind[T, U any](
	i IO[T],
	f func(T) IO[U],
) IO[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return u, e
		}
		return f(t)(ctx)
	}
}

func Lift[T, U any](
	pure func(T) (U, error),
) func(T) IO[U] {
	return func(t T) IO[U] {
		return func(_ context.Context) (U, error) {
			return pure(t)
		}
	}
}

type Void struct{}

var Empty Void = struct{}{}

func OfFn[T any](f func() T) IO[T] {
	return func(_ context.Context) (T, error) {
		return f(), nil
	}
}
