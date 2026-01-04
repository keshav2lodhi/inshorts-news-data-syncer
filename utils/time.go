package utils

import "time"

const (
	inputLayout  = "2006-01-02T15:04:05"
	outputLayout = "2006-01-02T15:04:05.000Z"
)

// NormalizeToESDate converts
// "yyyy-MM-dd'T'HH:mm:ss"
// → "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
func NormalizeToESDate(input string) (string, error) {
	t, err := time.Parse(inputLayout, input)
	if err != nil {
		return "", err
	}

	// Force UTC and format for Elasticsearch
	return t.UTC().Format(outputLayout), nil
}
