package dyno

import "github.com/aws/aws-sdk-go/aws/awserr"

func isAwsErrorCode(err error, code string) bool {
	if err, ok := err.(awserr.Error); ok {
		return err.Code() == code
	}
	return false
}
