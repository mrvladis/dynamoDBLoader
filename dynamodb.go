package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type StationRecord struct {
	CrsCode         string
	StationName     string
	Lat             string
	Long            string
	IataAirportCode string
}

type DynamoTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func (basics DynamoTable) TableExists() (bool, error) {
	exists := true
	_, err := basics.DynamoDbClient.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(basics.TableName)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", basics.TableName)
			err = nil
		} else {
			log.Printf("Couldn't determine existence of table %v. Here's why: %v\n", basics.TableName, err)
		}
		exists = false
	}
	return exists, err
}

// Create a function that create DynamoDB table.
func (basics DynamoTable) CreateTable() error {
	// Create the DynamoDB table.
	log.Printf("Creating DynamoDB table %v...\n", basics.TableName)
	_, err := basics.DynamoDbClient.CreateTable(
		context.TODO(), &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("CrsCode"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("StationName"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("CrsCode"),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String("StationName"),
					KeyType:       types.KeyTypeRange,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
			// ProvisionedThroughput: &types.ProvisionedThroughput{
			// 	ReadCapacityUnits:  aws.Int64(5),
			// 	WriteCapacityUnits: aws.Int64(5),
			// },
			TableName: aws.String(basics.TableName),
		},
	)
	if err != nil {
		log.Printf("Couldn't create table %v. Here's why: %v\n", basics.TableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(basics.DynamoDbClient)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(basics.TableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
	}
	return err
}

func (basics DynamoTable) TableLoad(records [][]string) error {
	for _, record := range records {
		item := StationRecord{
			StationName:     record[0],
			Lat:             record[1],
			Long:            record[2],
			CrsCode:         record[3],
			IataAirportCode: record[4],
		}
		log.Printf("Processing record: %v\n", item)
		data, err := attributevalue.MarshalMap(item)
		log.Printf("Data: %v", data)
		if err != nil {
			log.Printf("failed to marshal Record, %v", err)
			return err
		}

		input := &dynamodb.PutItemInput{
			Item:      data,
			TableName: aws.String(basics.TableName),
		}

		_, err = basics.DynamoDbClient.PutItem(context.TODO(), input)
		if err != nil {
			log.Printf("failed to put Record, %v", err)
			//return err
		}
	}
	return nil
}

// Create a function to read csv files and load the data into DynamoDB.

// func loadCSV(svc *dynamodb.Client, tableName string, csvFile string) {
// 	file, err := os.Open(csvFile)
// 	if err != nil {
// 		fmt.Println(err.Error())
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	records, err := reader.ReadAll()
// 	if err != nil {
// 		fmt.Println(err.Error())
// 	}

// 	for _, record := range records {
// 		item := map[string]*dynamodb.AttributeValue{
// 			"id": {
// 				S: aws.String(record[0]),
// 			},
// 			"name": {
// 				S: aws.String(record[1]),
// 			},
// 			"email": {
// 				S: aws.String(record[2]),
// 			},
// 		}

// 		input := &dynamodb.PutItemInput{
// 			TableName: aws.String(tableName),
// 			Item:      item,
// 		}

// 		_, err := svc.PutItem(input)
// 		if err != nil {
// 			fmt.Println(err.Error())
// 		}
// 	}
// }
