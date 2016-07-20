/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

import (
	"net/http"

	router "github.com/gorilla/mux"
)

// SystemLockState - Structure to fill the lock state of entire object storage.
// That is the total locks held, total calls blocked on locks and state of al the locks for the entire system.
type SystemLockState struct {
	TotalLocksCount    int64            `json:"totalLocks"`
	TotalBlockedLocks  int64            `json:"totalBlockedLocks"`
	TotalRunningLocks  int64            `json:"totalAcquiredLocks"`
	LocksInfoPerVolume []VolumeLockInfo `json:"locksInfoPerObject"`
}

// VolumeLockInfo - Structure to contain the lock state info for volume, path pair.
type VolumeLockInfo struct {
	Volume            string         `json:"bucket"`
	Path              string         `json:"object"`
	TotalLocks        int64          `json:"locksOnObject"`
	TotalRunningOps   int64          `json:"locksAcquiredOnObject"` // count of operations which has successfully acquired the lock but hasn't unlocked yet( operation in progress).
	TotalBlockedLocks int64          `json:"locksBlockedOnObject"`  // count of operations which are blocked waiting for the lock to be released.
	OpsLockState      []OpsLockState `json:"LockDetailsOnObject"`   // state information containing state of the locks for all operations on given <volume,path> pair.
}

// OpsLockState - structure to fill in state information of the lock.
// structure to fill in status information for each operation with given operation ID.
type OpsLockState struct {
	OperationID string `json:"opsID"`      // string containing operation ID.
	LockOrigin  string `json:"lockOrigin"` // contant which mentions the operation type (Get Obejct, PutObject...)
	LockType    string `json:"lockType"`
	Status      string `json:"status"`      // status can be running/ready/blocked.
	Since       string `json:"statusSince"` // time info of the since how long the status holds true, value in seconds.
}

func registerDebugRouter(mux *router.Router) {
	debugRouter := mux.PathPrefix(reservedBucket).Subrouter()
	// return all the locking state information for all <bucket, object> pair.
	debugRouter.Methods("GET").Path("/debug/locks").HandlerFunc(debugReturnSystemLockState)
}

// Template for handler.
func debugReturnSystemLockState(w http.ResponseWriter, r *http.Request) {
	response := generateSystemLockResponse()
	encodedSuccessResponse := mustEncodeJSON(response)
	// write headers
	setCommonHeaders(w)
	// write success response.
	writeSuccessResponse(w, encodedSuccessResponse)

}
