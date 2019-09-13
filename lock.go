package dyno

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/segmentio/ksuid"
)

type Lock struct {
	db            *dynamodb.DynamoDB
	tn            string
	pk            string
	sk            string
	name          string
	owned         *string
	local         sync.Mutex
	expiresAt     time.Time
	expiresAtName string
}

func NewLock(db *dynamodb.DynamoDB, tableName, primaryKey, sortKey, name string) *Lock {
	return &Lock{
		db:   db,
		tn:   tableName,
		pk:   primaryKey,
		sk:   sortKey,
		name: name,
	}
}

var (
	ErrLockAcquireTimeout       = errors.New("failed to acquire lock within timeout")
	ErrLockNotOwned             = errors.New("lock not owned by this lock")
	errLockAcquiredBeforeExpire = errors.New("lock was acquired before expiration")
)

func (l *Lock) Expiration(name string, at time.Time) {
	l.expiresAtName = name
	l.expiresAt = at
}

func (l *Lock) Acquire(lease time.Duration) error {
	return l.AcquireWithTimeout(lease, time.Duration(0))
}

func (l *Lock) AcquireWithTimeout(lease, duration time.Duration) error {
	l.local.Lock()
	defer l.local.Unlock()

	start := time.Now()
	lockID := ksuid.New().String()
	var lastLeaseID string

	item := l.key()
	item["Dyno_LockID"] = &dynamodb.AttributeValue{S: aws.String(lockID)}
	item["Dyno_Lease"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(int(lease / time.Second)))}
	if l.expiresAtName != "" {
		item[l.expiresAtName] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", l.expiresAt.Unix()))}
	}
	input := &dynamodb.PutItemInput{
		TableName:           aws.String(l.tn),
		ConditionExpression: aws.String("attribute_not_exists(#id)"),
		ExpressionAttributeNames: map[string]*string{
			"#id": aws.String("Dyno_LockID"),
		},
		Item: item,
	}

	for {
		sleep := true

		_, err := l.db.PutItem(input)
		if err == nil { // We own the lock
			l.owned = aws.String(lockID)
			return nil
		}

		sleep = true

		if isAwsErrorCode(err, dynamodb.ErrCodeConditionalCheckFailedException) { // Failed to acquire the lock. Owned by someone else
			context, err := l.getCurrentLeaseContext()
			if err != nil { // Unknown error
				return err
			}
			if context == nil { // The lock was released before we could fetch the current context.
				sleep = false
			} else {
				// The lock has expired by the person we expect it to be.
				if lastLeaseID == context.id && start.Add(context.duration).Before(time.Now()) {
					err := l.expireAndAcquire(context.id, lockID)
					if err == nil { // We own the lock
						return nil
					}
					// the error will be errLockAcquiredBeforeExpire if the lock was acquired by someone else
					// we can continue waiting
					if err != errLockAcquiredBeforeExpire {
						return err
					}
				}

				lastLeaseID = context.id
			}
		}

		// Lock wait timeout
		if start.Add(duration).Before(time.Now()) {
			return ErrLockAcquireTimeout
		}

		// Wait 25ms before trying to acquire the lock again.
		if sleep {
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// Release releases the lock back to be re-acquired
func (l *Lock) Release() error {
	l.local.Lock()
	defer l.local.Unlock()

	if l.owned == nil {
		return ErrLockNotOwned
	}

	input := &dynamodb.UpdateItemInput{
		TableName:           aws.String(l.tn),
		Key:                 l.key(),
		UpdateExpression:    aws.String("REMOVE #id, #ls"),
		ConditionExpression: aws.String("#id = :id"),
		ExpressionAttributeNames: map[string]*string{
			"#id": aws.String("Dyno_LockID"),
			"#ls": aws.String("Dyno_Lease"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": {S: l.owned},
		},
	}

	_, err := l.db.UpdateItem(input)
	if isAwsErrorCode(err, dynamodb.ErrCodeConditionalCheckFailedException) {
		l.owned = nil
		return nil
	}

	if err != nil {
		return err
	}

	l.owned = nil

	return nil
}

type leaseContext struct {
	id       string
	duration time.Duration
}

func (l *Lock) key() map[string]*dynamodb.AttributeValue {
	item := map[string]*dynamodb.AttributeValue{}
	item[l.pk] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("Dyno_Lock/%s", l.name))}

	if l.sk != "" {
		item[l.sk] = &dynamodb.AttributeValue{S: aws.String("Dyno_LockSortKeyValue")}
	}

	return item
}

func (l *Lock) getCurrentLeaseContext() (*leaseContext, error) {
	input := &dynamodb.GetItemInput{
		TableName:            aws.String(l.tn),
		Key:                  l.key(),
		ProjectionExpression: aws.String("Dyno_LockID, Dyno_Lease"),
	}

	result, err := l.db.GetItem(input)
	if err != nil {
		return nil, err
	}
	if len(result.Item) == 0 {
		return nil, nil
	}

	raw, err := strconv.ParseInt(aws.StringValue(result.Item["Dyno_Lease"].N), 10, 64)
	if err != nil {
		return nil, err
	}

	return &leaseContext{
		id:       aws.StringValue(result.Item["Dyno_LockID"].S),
		duration: time.Duration(raw) * time.Second,
	}, nil
}

func (l *Lock) expireAndAcquire(currentID, newID string) error {
	return errors.New("testing")
}
