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
	"fmt"
	"os/exec"
)

// SizeUnit dd block size unit
type SizeUnit string

const (
	SizeUnitKB SizeUnit = "K"
	SizeUnitMB SizeUnit = "M"
	SizeUnitGB SizeUnit = "G"
)

// DDFile call dd command to generate empty file
func DDFile(fp string, size int64, unit SizeUnit) error {
	dataSource := "if=/dev/zero"
	fp = fmt.Sprintf("of=%s", fp)
	blockSize := fmt.Sprintf("bs=1%s", unit)
	count := fmt.Sprintf("count=%d", size)
	cmd := exec.Command("dd", dataSource, fp, blockSize, count)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}
