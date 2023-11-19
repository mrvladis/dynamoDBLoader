package main

import (
	"encoding/csv"
	"os"
	"fmt"
)
// create a function that checks if the file exists and returns true if it exists.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func csvReader(filepath string) ([][]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println(err.Error())
	}
	return records, err
}
