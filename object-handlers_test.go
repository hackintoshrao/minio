/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"testing"
)

// Wrapper for calling Get Bucket Policy HTTP handler tests for both XL multiple disks and single node setup.
func TestAPIGetOjectHandler(t *testing.T) {
	ExecObjectLayerTest(t, testAPIGetOjectHandler)
}
func testAPIGetOjectHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {

	// get random bucket name.
	bucketName := getRandomBucketName()
	objectName := "test-object"
	// Create bucket.
	err := obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}
	// Register the API end points with XL/FS object layer.
	// Registering only the PutBucketPolicy and GetBucketPolicy handlers.
	apiRouter := initTestAPIEndPoints(obj, []string{"GetObject"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	// set of byte data for PutObject.
	// object has to be inserted before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * 1024 * 1024)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for GetBucketPolicyHandler.
	testCases := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected output.
		expectedBucketPolicy string
		expectedRespStatus   int
	}{
		{bucketName, credentials.AccessKeyID, credentials.SecretAccessKey, bucketPolicyTemplate, http.StatusOK},
	}
	// Iterating over the cases, fetching the policy and validating the response.
	for i, testCase := range testCases {
		// expected bucket policy json string.
		expectedBucketPolicyStr := fmt.Sprintf(testCase.expectedBucketPolicy, testCase.bucketName, testCase.bucketName)
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		req, err := newTestSignedRequest("GET", getGetPolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for GetBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, GetBucketPolicyHandler handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}
		// read the response body.
		bucketPolicyReadBuf, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		// Verify whether the bucket policy fetched is same as the one inserted.
		if expectedBucketPolicyStr != string(bucketPolicyReadBuf) {
			t.Errorf("Test %d: %s: Bucket policy differs from expected value.", i+1, instanceType)
		}
	}
}
