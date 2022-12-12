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

package minio

import (
	"fmt"
	"testing"
	"time"
)

func Test_gPool_newTask(t *testing.T) {
	pool := newGPool(3)
	for i := 0; i < 5; i++ {
		pool.newTask(func() {
			fmt.Println(time.Now())
			time.Sleep(10 * time.Second)
		})
	}
	pool.wait()
}

func Test_gPool_newTask_noClosure(t *testing.T) {
	pool := newGPool(3)
	var noClosure = func(name string) {
		fmt.Println(name)
	}
	for i := 0; i < 5; i++ {
		pool.newTask(func() {
			// i take a mistake, because value copy
			noClosure(fmt.Sprintf("%d doing...", i))
		})
	}
	pool.wait()
}

func Test_gPool_newTask_noClosure_new(t *testing.T) {
	pool := newGPool(3)
	var noClosure = func(name string) {
		fmt.Println(name)
	}
	for i := 0; i < 5; i++ {
		var newName = fmt.Sprintf("%d doing...", i)
		pool.newTask(func() {
			// good job
			noClosure(newName)
		})
	}
	pool.wait()
}
