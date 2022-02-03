package main

import "math"

type processedRequest struct {
	parsedRequest
	processedNumber float64
}

func sqrtRequest(req parsedRequest) processedRequest {
	return processedRequest{
		parsedRequest:   req,
		processedNumber: math.Sqrt(req.parsedNumber),
	}
}
