package main

import (
	"log"
	"strconv"
)

type parsedRequest struct {
	request
	parsedNumber float64
}

func parseRequest(req request) (parsedRequest, error) {
	parsedNumber, err := strconv.ParseFloat(string(req.rawData), 64)
	if err != nil {
		log.Println("error parsing request:", err)
		return parsedRequest{}, err
	}
	return parsedRequest{
		request:      req,
		parsedNumber: parsedNumber,
	}, nil
}
