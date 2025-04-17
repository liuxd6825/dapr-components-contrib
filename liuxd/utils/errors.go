package utils

import (
	"errors"
	"fmt"
)

type anyError struct {
	sourceErr any
}

func (e *anyError) Error() string {
	return fmt.Sprintf("%v", e.sourceErr)
}

func GetRecoverError(err error, rerr any) (resErr error) {
	if err != nil {
		return err
	}
	if rerr != nil {
		switch rerr.(type) {
		case string:
			{
				msg, _ := rerr.(string)
				resErr = errors.New(msg)
			}
		case error:
			{
				if e, ok := rerr.(error); ok {
					resErr = e
				}
			}
		default:
			{
				return &anyError{sourceErr: rerr}
			}
		}
	}
	return resErr
}
