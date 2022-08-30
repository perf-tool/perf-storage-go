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

import "sync"

// gPool simple goroutine pool
type gPool struct {
	work     chan func()
	capacity chan struct{}
	wg       sync.WaitGroup
}

func newGPool(size int) *gPool {
	return &gPool{
		work:     make(chan func()),
		capacity: make(chan struct{}, size),
	}
}

func (p *gPool) newTask(task func()) {
	p.wg.Add(1)
	select {
	case p.work <- task:
	case p.capacity <- struct{}{}:
		go p.worker(task)
	}
}

func (p *gPool) worker(task func()) {
	defer func() { <-p.capacity }()
	for {
		task()
		p.wg.Done()
		task = <-p.work
	}
}

func (p *gPool) wait() {
	p.wg.Wait()
}
