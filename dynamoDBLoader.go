package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	//	"github.com/aws/aws-sdk-go-v2"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
)

func main() {
	fmt.Println("Hello World!")
	csvFile := flag.String("scvfilepath", "testdata/stations.csv", "Path to CSV file")
	dynamodbTableName := flag.String("tablename", "TrainStations", "Dynamodb table name")
	flag.Parse()
	fmt.Println("csvFile:", *csvFile)
	fmt.Println("dynamodbTableName:", *dynamodbTableName)

	if fileExists(*csvFile) {
		awscfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion("eu-west-2"),
		)
		if err != nil {
			fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
			fmt.Println(err)
			return
		}
		// bedrockClient := bedrock.NewFromConfig(sdkConfig)
		dynamoDBClient := dynamodb.NewFromConfig(awscfg)
		dynamoDBTable := DynamoTable{dynamoDBClient, *dynamodbTableName}
		if tableExist, tableErr := dynamoDBTable.TableExists(); tableExist {
			log.Printf("Table %v has been found", dynamoDBTable.TableName)
		} else {
			if tableErr != nil {
				log.Printf("Couldn't find the Table %v", dynamoDBTable.TableName)
				return
			}
			tableErr = dynamoDBTable.CreateTable()
			if tableErr != nil {
				log.Printf("Couldn't create Table %v", dynamoDBTable.TableName)
				return
			} else {
				log.Printf("Table %v Created Successfully", dynamoDBTable.TableName)
			}

		}

		records, err := csvReader(*csvFile)
		if err != nil {
			log.Printf("failed to read file, %w", err)
		}

		err = dynamoDBTable.TableLoad(records)
		if err != nil {
			log.Printf("failed to put Record, %w", err)
		}

		// if TableExists(dynamodbTableName) != true {
		// 	fmt.Println("Table does not exist")
		// }
	} else {
		fmt.Println("File does not exist")
	}

}

// // Create a function to read csv files, identify column names and load the data into Dynamodb table with appropriate field names
