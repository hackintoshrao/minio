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

import (
	"errors"
	"time"
)

const (
	debugRLockStr = "RLock"
	debugWLockStr = "WLock"
)

// struct containing information of status (ready/running/blocked) of an operation with given operation ID.
type debugLockInfo struct {
	lockType   string    // "Rlock" or "WLock".
	lockOrigin string    // contains the trace of the function which invoked the lock, obtained from runtime.
	status     string    // status can be running/ready/blocked.
	since      time.Time // time info of the since how long the status holds true.
}

// debugLockInfo - container for storing locking information for unique copy (volume,path) pair.
// ref variable holds the reference count for locks held for.
// `ref` values helps us understand the n locks held for given <volume, path> pair.
// `running` value helps us understand the total successful locks held (not blocked) for given <volume, path> pair and the operation is under execution.
// `blocked` value helps us understand the total number of operations blocked waiting on locks for given <volume,path> pair.
type debugLockInfoPerVolumePath struct {
	ref      int64                      // running + blocked operations.
	running  int64                      // count of successful lock acquire and running operations.
	blocked  int64                      // count of number of operations blocked waiting on lock.
	lockInfo (map[string]debugLockInfo) // map of [operationID] debugLockInfo{operation, status, since} .
}

// returns an instance of debugLockInfo.
// need to create this for every unique pair of {volume,path}.
// total locks, number of calls blocked on locks, and number of successful locks held but not unlocked yet.
func newDebugLockInfoPerVolumePath() *debugLockInfoPerVolumePath {
	return &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		ref:      0,
		blocked:  0,
		running:  0,
	}
}

// change the state of the lock from Blocked to Running.
func (n *nsLockMap) statusBlockedToRunning(param nsParam, lockOrigin, operationID string, readLock bool) {
	// This operation is not executed under the scope nsLockMap.mutex.Lock(), lock has to be explicitly held here.
	n.mutex.Lock()
	defer n.mutex.Unlock()

	newLockInfo := debugLockInfo{
		lockOrigin: lockOrigin,
		status:     "Running",
		since:      time.Now().UTC(),
	}

	if readLock {
		newLockInfo.lockType = debugRLockStr
	} else {
		newLockInfo.lockType = debugWLockStr
	}
	// changing the status of the operation from blocked to running and updating the time.
	n.debugLockMap[param].lockInfo[operationID] = newLockInfo

	// After locking unblocks decrease the blocked counter.
	n.blockedCounter--
	// Increase the running counter.
	n.runningLockCounter++
	n.debugLockMap[param].blocked--
	n.debugLockMap[param].running++
}

// change the state of the lock from Ready to Blocked.
// lock is already held in the caller on nsLockMap.
func (n *nsLockMap) statusNoneToBlocked(param nsParam, lockOrigin, operationID string, readLock bool) {

	newLockInfo := debugLockInfo{
		lockOrigin: lockOrigin,
		status:     "Blocked",
		since:      time.Now().UTC(),
	}
	if readLock {
		newLockInfo.lockType = debugRLockStr
	} else {
		newLockInfo.lockType = debugWLockStr
	}
	// Need replacement for Operation ID. It is the tracker ID for the operation.
	// The status of the operation with the given operation ID is marked blocked till its gets unblocked from the lock.
	n.debugLockMap[param].lockInfo[operationID] = newLockInfo
	// Increment the Global lock counter.
	n.globalLockCounter++
	// Increment the counter for number of blocked opertions, decrement it after the locking unblocks.
	n.blockedCounter++
	// increment the reference of the lock for the given <volume,path> pair.
	n.debugLockMap[param].ref++
	// increment the blocked counter for the given <volume, path> pair.
	n.debugLockMap[param].blocked++
}

// deleteLockInfoEntry - Deletes the lock state information for given <volume, path> pair. Called when nsLk.ref count is 0.
func (n *nsLockMap) deleteLockInfoEntryForVolumePath(param nsParam, operationID string) {
	// delete the lock info for the given operation.
	if _, found := n.debugLockMap[param]; found {
		// Remove from the map if there are no more references for the given (volume,path) pair.
		delete(n.debugLockMap, param)
	}
}

// deleteLockInfoEntry - Deletes the entry for given opsID in the lock state information of given <volume, path> pair.
// called when the nsLk ref count for the given <volume, path> pair is not 0.
func (n *nsLockMap) deleteLockInfoEntryForOps(param nsParam, operationID string) {
	// delete the lock info for the given operation.
	if infoMap, found := n.debugLockMap[param]; found {
		// the opertion finished holding the lock on the resource, remove the entry for the given operation with the operation ID.
		if _, foundInfo := infoMap.lockInfo[operationID]; foundInfo {
			// decrease the global running and lock reference counter.
			n.runningLockCounter--
			n.globalLockCounter--
			// decrease the lock referene counter for the lock info for given <volume,path> pair.
			// decrease the running operation number. Its assumed that the operation is over once an attempt to release the lock is made.
			infoMap.running--
			// decrease the total reference count of locks jeld on <volume,path> pair.
			infoMap.ref--
			delete(infoMap.lockInfo, operationID)
		} else {
			// Unlock request with invalid opertion ID not accepted.
			errorIf(errors.New("Operation ID doesn't exist"), "Invalid operation ID detected in LockInfo.")
		}
	}
}

// return randomly generated string ID if lock debug is enabled,
// else returns empty string
func getOpsID() (opsID string) {
	// check if lock debug is enabled.
	if globalDebugLock {
		// generated random ID.
		opsID = string(generateRequestID())
	}
	return opsID
}

// Read entire state of the locks in the system and return.
func generateSystemLockResponse() SystemLockState {
	nsMutex.mutex.Lock()
	defer nsMutex.mutex.Unlock()

	lockState := SystemLockState{}

	lockState.TotalBlockedLocks = nsMutex.blockedCounter
	lockState.TotalLocksCount = nsMutex.globalLockCounter
	lockState.TotalRunningLocks = nsMutex.runningLockCounter

	for param := range nsMutex.debugLockMap {
		volLockInfo := VolumeLockInfo{}
		volLockInfo.Volume = param.volume
		volLockInfo.Path = param.path
		volLockInfo.TotalBlockedLocks = nsMutex.debugLockMap[param].blocked
		volLockInfo.TotalRunningOps = nsMutex.debugLockMap[param].running
		volLockInfo.TotalLocks = nsMutex.debugLockMap[param].ref
		for opsID := range nsMutex.debugLockMap[param].lockInfo {
			opsState := OpsLockState{}
			opsState.OperationID = opsID
			opsState.LockOrigin = nsMutex.debugLockMap[param].lockInfo[opsID].lockOrigin
			opsState.LockType = nsMutex.debugLockMap[param].lockInfo[opsID].lockType
			opsState.Status = nsMutex.debugLockMap[param].lockInfo[opsID].status
			opsState.Since = time.Now().Sub(nsMutex.debugLockMap[param].lockInfo[opsID].since).String()

			volLockInfo.OpsLockState = append(volLockInfo.OpsLockState, opsState)
		}
		lockState.LocksInfoPerVolume = append(lockState.LocksInfoPerVolume, volLockInfo)
	}
	return lockState
}
