// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package util

import (
	"os"
	"strconv"
)

func GetEnvStr(key string, value string) string {
	aux := os.Getenv(key)
	if aux != "" {
		return aux
	}
	return value
}

func GetEnvInt(key string, value int) int {
	aux := os.Getenv(key)
	if aux == "" {
		return value
	}
	res, err := strconv.Atoi(aux)
	if err != nil {
		return value
	}
	return res
}

func GetEnvInt64(key string, value int64) int64 {
	aux := os.Getenv(key)
	if aux == "" {
		return value
	}
	res, err := strconv.ParseInt(aux, 10, 64)
	if err != nil {
		return value
	}
	return res
}

func GetEnvFloat64(key string, value float64) float64 {
	aux := os.Getenv(key)
	if aux == "" {
		return value
	}
	res, err := strconv.ParseFloat(aux, 64)
	if err != nil {
		return value
	}
	return res
}

func GetEnvBool(key string, value bool) bool {
	aux := os.Getenv(key)
	if aux != "" {
		return aux == "true"
	}
	return value
}
