package main

import "fmt"

type Exception struct {
	what func() error
}

func (e *Exception) error() string {
	return e.what().Error()
}

func (e *Exception) Error() string {
	return e.error()
}

func (e *Exception) throw() {
	panic(e)
}

func (e *Exception) catch(cb func(*Exception)) {
	if e != nil {
		cb(e)
	}
}

func (e *Exception) asError() error {
	if e == nil {
		return nil
	}

	return e.what()
}

func newException(err error) *Exception {
	return &Exception{
		what: func() error {
			return err
		},
	}
}

func exceptionf(format string, args ...any) *Exception {
	return newException(fmt.Errorf(format, args...))
}

func throw(err error) {
	if err != nil {
		newException(err).throw()
	}
}

func throw2[T any](val T, err error) T {
	throw(err)

	return val
}

func throw3[T1, T2 any](v1 T1, v2 T2, err error) (T1, T2) {
	throw(err)

	return v1, v2
}

func throw4[T1, T2, T3 any](v1 T1, v2 T2, v3 T3, err error) (T1, T2, T3) {
	throw(err)

	return v1, v2, v3
}

func throwFmt(format string, args ...any) {
	exceptionf(format, args...).throw()
}

func try(cb func()) (err *Exception) {
	defer func() {
		if rec := recover(); rec != nil {
			if exc, ok := rec.(*Exception); ok {
				err = exc
			} else {
				panic(rec)
			}
		}
	}()

	cb()

	return nil
}
