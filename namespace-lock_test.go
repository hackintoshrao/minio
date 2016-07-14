/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import "testing"

// Tests functionality provided by namespace lock.
func TestNamespaceLockTest(t *testing.T) {
	// List of test cases.
	testCases := []struct {
		lk               func(s1, s2, s3 string)
		unlk             func(s1, s2, s3 string)
		rlk              func(s1, s2, s3 string)
		runlk            func(s1, s2, s3 string)
		lkCount          int
		lockedRefCount   uint
		unlockedRefCount uint
		shouldPass       bool
	}{
		{
			lk:               nsMutex.Lock,
			unlk:             nsMutex.Unlock,
			lockedRefCount:   1,
			unlockedRefCount: 0,
			shouldPass:       true,
		},
		{
			rlk:              nsMutex.RLock,
			runlk:            nsMutex.RUnlock,
			lockedRefCount:   4,
			unlockedRefCount: 2,
			shouldPass:       true,
		},
		{
			rlk:              nsMutex.RLock,
			runlk:            nsMutex.RUnlock,
			lockedRefCount:   1,
			unlockedRefCount: 0,
			shouldPass:       true,
		},
	}

	// Run all test cases.

	// Write lock tests.
	testCase := testCases[0]
	testCase.lk("a", "b", "c") // lock once.
	nsLk, ok := nsMutex.lockMap[nsParam{"a", "b"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock in map missing.")
	}
	// Validate loced ref count.
	if testCase.lockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 1, testCase.lockedRefCount, nsLk.ref)
	}
	testCase.unlk("a", "b", "c") // unlock once.
	if testCase.unlockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 1, testCase.unlockedRefCount, nsLk.ref)
	}
	_, ok = nsMutex.lockMap[nsParam{"a", "b"}]
	if ok && !testCase.shouldPass {
		t.Errorf("Lock map found after unlock.")
	}

	// Read lock tests.
	testCase = testCases[1]
	testCase.rlk("a", "b", "c") // lock once.
	testCase.rlk("a", "b", "c") // lock second time.
	testCase.rlk("a", "b", "c") // lock third time.
	testCase.rlk("a", "b", "c") // lock fourth time.
	nsLk, ok = nsMutex.lockMap[nsParam{"a", "b"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock in map missing.")
	}
	// Validate loced ref count.
	if testCase.lockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 1, testCase.lockedRefCount, nsLk.ref)
	}
	testCase.runlk("a", "b", "c") // unlock once.
	testCase.runlk("a", "b", "c") // unlock second time.
	if testCase.unlockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 2, testCase.unlockedRefCount, nsLk.ref)
	}
	_, ok = nsMutex.lockMap[nsParam{"a", "b"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock map not found.")
	}

	// Read lock 0 ref count.
	testCase = testCases[2]
	testCase.rlk("a", "c", "d") // lock once.

	nsLk, ok = nsMutex.lockMap[nsParam{"a", "c"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock in map missing.")
	}
	// Validate loced ref count.
	if testCase.lockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 3, testCase.lockedRefCount, nsLk.ref)
	}
	testCase.runlk("a", "c", "d") // unlock once.
	if testCase.unlockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 3, testCase.unlockedRefCount, nsLk.ref)
	}
	_, ok = nsMutex.lockMap[nsParam{"a", "c"}]
	if ok && !testCase.shouldPass {
		t.Errorf("Lock map not found.")
	}
}
