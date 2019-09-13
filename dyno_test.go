package dyno

import (
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/segmentio/ksuid"
)

var (
	tableName  = fmt.Sprintf("dyno-test-table-%s", ksuid.New().String())
	testClient = dynamodb.New(session.New(&aws.Config{Region: aws.String("us-east-1")}), aws.NewConfig().WithEndpoint("http://localhost:8000/"))
)

func TestMain(m *testing.M) {
	os.Exit(testRunner(m))
}

func testRunner(m *testing.M) int {
	create := &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: aws.String("PROVISIONED"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: aws.String("S")},
			{AttributeName: aws.String("SK"), AttributeType: aws.String("S")},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: aws.String("HASH")},
			{AttributeName: aws.String("SK"), KeyType: aws.String("RANGE")},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	_, err := testClient.CreateTable(create)
	if err != nil {
		panic(err)
	}

	defer func() {
		delete := &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		}
		testClient.DeleteTable(delete)
	}()

	return m.Run()
}
