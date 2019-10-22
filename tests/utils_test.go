/*
* Copyright [2019] [https://github.com/hopkings2008]

* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at

*     http://www.apache.org/licenses/LICENSE-2.0

* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package tests

import (
	"sync"

	"github.com/hopkings2008/snowflake/utils"
	. "gopkg.in/check.v1"
)

func (ys *SnowFlakeSuite) TestGlobalId(c *C) {
	gi, err := utils.NewGlobalIdGen()
	c.Assert(err, Equals, nil)
	c.Assert(gi, Not(Equals), nil)
	ids := make(map[int64]int64)
	count := 8192

	for i := 0; i < count; i++ {
		id := gi.GetId()
		ids[id] = id
	}

	c.Assert(len(ids), Equals, count)
}

func (ys *SnowFlakeSuite) TestGlobalIdConcurrent(c *C) {
	var wg sync.WaitGroup
	gi, _ := utils.NewGlobalIdGen()
	ids := make(map[int64]int64)
	count := 8192
	numThreads := 100
	idsChan := make(chan int64)
	wg.Add(numThreads)

	funcGen := func(loop int) {
		for i := 0; i < loop; i++ {
			id := gi.GetId()
			idsChan <- id
		}
		wg.Done()
	}

	for i := 0; i < numThreads; i++ {
		go funcGen(count)
	}

	go func() {
		wg.Wait()
		close(idsChan)
	}()

	for id := range idsChan {
		ids[id] = id
	}

	c.Logf("len(ids): %d, needed: %d\n", len(ids), numThreads*count)
	c.Assert(len(ids), Equals, numThreads*count)
}
