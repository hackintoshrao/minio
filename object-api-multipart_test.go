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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

// Wrapper for calling NewMultipartUpload tests for both XL multiple disks and single node setup.
func TestObjectNewMultipartUpload(t *testing.T) {
	ExecObjectLayerTest(t, testObjectNewMultipartUpload)
}

// Tests validate creation of new multipart upload instance.
func testObjectNewMultipartUpload(obj ObjectLayer, instanceType string, t *testing.T) {

	bucket := "minio-bucket"
	object := "minio-object"

	errMsg := "Bucket not found: minio-bucket"
	// opearation expected to fail since the bucket on which NewMultipartUpload is being initiated doesn't exist.
	uploadID, err := obj.NewMultipartUpload(bucket, object, nil)
	if err == nil {
		t.Fatalf("%s: Expected to fail since the NewMultipartUpload is intialized on a non-existant bucket.", instanceType)
	}
	if errMsg != err.Error() {
		t.Errorf("%s, Expected to fail with Error \"%s\", but instead found \"%s\".", instanceType, errMsg, err.Error())
	}

	// Create bucket before intiating NewMultipartUpload.
	err = obj.MakeBucket(bucket)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	uploadID, err = obj.NewMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	err = obj.AbortMultipartUpload(bucket, object, uploadID)
	if err != nil {
		switch err.(type) {
		case InvalidUploadID:
			t.Fatalf("%s: New Multipart upload failed to create uuid file.", instanceType)
		default:
			t.Fatalf(err.Error())
		}
	}
}

// Wrapper for calling isUploadIDExists tests for both XL multiple disks and single node setup.
func TestObjectAPIIsUploadIDExists(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAPIIsUploadIDExists)
}

// Tests validates the validator for existence of uploadID.
func testObjectAPIIsUploadIDExists(obj ObjectLayer, instanceType string, t *testing.T) {
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket before intiating NewMultipartUpload.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	_, err = obj.NewMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	err = obj.AbortMultipartUpload(bucket, object, "abc")
	switch err.(type) {
	case InvalidUploadID:
	default:
		t.Fatalf("%s: Expected uploadIDPath to exist.", instanceType)
	}
}

// Wrapper for calling PutObjectPart tests for both XL multiple disks and single node setup.
func TestObjectAPIPutObjectPart(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAPIPutObjectPart)
}

// Tests validate correctness of PutObjectPart.
func testObjectAPIPutObjectPart(obj ObjectLayer, instanceType string, t *testing.T) {
	// Generating cases for which the PutObjectPart fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket before intiating NewMultipartUpload.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	uploadID, err := obj.NewMultipartUpload(bucket, object, nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Creating a dummy bucket for tests.
	err = obj.MakeBucket("unused-bucket")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	failCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		intputDataSize  int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Test case  1-4.
		// Cases with invalid bucket name.
		{".test", "obj", "", 1, "", "", 0, false, "", fmt.Errorf("%s", "Bucket name invalid: .test")},
		{"------", "obj", "", 1, "", "", 0, false, "", fmt.Errorf("%s", "Bucket name invalid: ------")},
		{"$this-is-not-valid-too", "obj", "", 1, "", "", 0, false, "",
			fmt.Errorf("%s", "Bucket name invalid: $this-is-not-valid-too")},
		{"a", "obj", "", 1, "", "", 0, false, "", fmt.Errorf("%s", "Bucket name invalid: a")},
		// Test case - 5.
		// Case with invalid object names.
		{bucket, "", "", 1, "", "", 0, false, "", fmt.Errorf("%s", "Object name invalid: minio-bucket#")},
		// Test case - 6.
		// Valid object and bucket names but non-existent bucket.
		{"abc", "def", "", 1, "", "", 0, false, "", fmt.Errorf("%s", "Bucket not found: abc")},
		// Test Case - 7.
		// Existing bucket, but using a bucket on which NewMultipartUpload is not Initiated.
		{"unused-bucket", "def", "xyz", 1, "", "", 0, false, "", fmt.Errorf("%s", "Invalid upload id xyz")},
		// Test Case - 8.
		// Existing bucket, object name different from which NewMultipartUpload is constructed from.
		// Expecting "Invalid upload id".
		{bucket, "def", "xyz", 1, "", "", 0, false, "", fmt.Errorf("%s", "Invalid upload id xyz")},
		// Test Case - 9.
		// Existing bucket, bucket and object name are the ones from which NewMultipartUpload is constructed from.
		// But the uploadID is invalid.
		// Expecting "Invalid upload id".
		{bucket, object, "xyz", 1, "", "", 0, false, "", fmt.Errorf("%s", "Invalid upload id xyz")},
		// Test Case - 10.
		// Case with valid UploadID, existing bucket name.
		// But using the bucket name from which NewMultipartUpload is not constructed from.
		{"unused-bucket", object, uploadID, 1, "", "", 0, false, "", fmt.Errorf("%s", "Invalid upload id "+uploadID)},
		// Test Case - 10.
		// Case with valid UploadID, existing bucket name.
		// But using the object name from which NewMultipartUpload is not constructed from.
		{bucket, "none-object", uploadID, 1, "", "", 0, false, "", fmt.Errorf("%s", "Invalid upload id "+uploadID)},
		// Test case - 11.
		// Input to replicate Md5 mismatch.
		{bucket, object, uploadID, 1, "", "a35", 0, false, "",
			fmt.Errorf("%s", "Bad digest: Expected a35 is not valid with what we calculated "+"d41d8cd98f00b204e9800998ecf8427e")},
		// Test case - 12.
		// Input with size more than the size of actual data inside the reader.
		{bucket, object, uploadID, 1, "abcd", "a35", int64(len("abcd") + 1), false, "", fmt.Errorf("%s", "EOF")},
		// Test case - 13.
		// Input with size less than the size of actual data inside the reader.
		{bucket, object, uploadID, 1, "abcd", "a35", int64(len("abcd") - 1), false, "",
			fmt.Errorf("%s", "Contains more data than specified size of 3 bytes.")},
		// Test case - 14-17.
		// Validating for success cases.
		{bucket, object, uploadID, 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), true, "", nil},
		{bucket, object, uploadID, 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh")), true, "", nil},
		{bucket, object, uploadID, 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd")), true, "", nil},
		{bucket, object, uploadID, 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd")), true, "", nil},
	}

	for i, testCase := range failCases {
		actualMd5Hex, actualErr := obj.PutObjectPart(testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, testCase.intputDataSize,
			bytes.NewBufferString(testCase.inputReaderData), testCase.inputMd5)
		// All are test cases above are expected to fail.

		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if testCase.expectedError.Error() != actualErr.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1,
					instanceType, testCase.expectedError.Error(), actualErr.Error())
			}
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil && testCase.shouldPass {
			// Asserting whether the md5 output is correct.
			if testCase.inputMd5 != actualMd5Hex {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, actualMd5Hex)
			}
		}
	}
}

// Wrapper for calling ListMultipartUploads tests for both XL multiple disks and single node setup.
// TestListMultipartUploads - Tests validate listing of multipart uploads.
func TestListMultipartUploads(t *testing.T) {
	// getSingleNodeObjectLayer - Instantiates single node object layer and returns it.
	initNSLock()
	getSingleNodeObjectLayer := func() (ObjectLayer, string, error) {
		// Make a temporary directory to use as the obj.
		fsDir, err := ioutil.TempDir("", "minio-")
		if err != nil {
			return nil, "", err
		}

		// Create the obj.
		objLayer, err := newFSObjects(fsDir)
		if err != nil {
			return nil, "", err
		}
		return objLayer, fsDir, nil
	}

	obj, dir, err := getSingleNodeObjectLayer()
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	bucketNames := []string{"minio-bucket", "minio-2-bucket", "minio-3-bucket"}
	objectNames := []string{"minio-object", "minio-2-object", "minio-3-object", "minio-4-object", "minio-5-object"}
	uploadIDs := []string{}
	// Create bucket before intiating NewMultipartUpload.
	err = obj.MakeBucket(bucketNames[0])
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s ", err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	uploadID, err := obj.NewMultipartUpload(bucketNames[0], objectNames[0], nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s ", err.Error())
	}

	uploadIDs = append(uploadIDs, uploadID)

	// Bucket to test for mutiple upload Id's for a given object.
	err = obj.MakeBucket(bucketNames[1])
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s ", err.Error())
	}
	for i := 0; i < 3; i++ {
		// Initiate Multipart Upload on bucketNames[1] for the same object 3 times.
		//  Used to test the listing for the case of multiple uploadID's for a given object.
		uploadID, err = obj.NewMultipartUpload(bucketNames[1], objectNames[0], nil)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("%s ", err.Error())
		}

		uploadIDs = append(uploadIDs, uploadID)
	}
	fmt.Println("Upload Ids in their order of generation.")
	fmt.Println(uploadIDs)
	// Bucket to test for mutiple objects, each with unique UUID.
	// bucketnames[2].
	// objectNames[0-2].
	// uploadIds [4-8].
	err = obj.MakeBucket(bucketNames[2])
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s ", err.Error())
	}
	// Initiate Multipart Upload on bucketNames[2.
	//  Used to test the listing for the case of multiple objects for a given bucket.
	for i := 0; i < 5; i++ {
		uploadID, err := obj.NewMultipartUpload(bucketNames[2], objectNames[i], nil)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("%s ", err.Error())
		}
		uploadIDs = append(uploadIDs, uploadID)
	}
	// Create multipart parts.
	// Need parts to be uploaded before MultipartLists can be called and tested.
	createPartCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		intputDataSize  int64
		expectedMd5     string
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		// Used to ensure that the ListMultipartResult produces one output for the four parts uploaded below for the given upload ID.
		{bucketNames[0], objectNames[0], uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh")), "1f7690ebdd9b4caf8fab49ca1757bf27"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd")), "09a0877d04abf8759f99adec02baf579"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd")), "e132e96a5ddad6da8b07bba6f6131fef"},
		// Create parts with 3 uploadID's for the same object.
		// Testing for listing of all the uploadID's for given object.
		// Insertion with 3 different uploadID's are done for same bucket and object.
		{bucketNames[1], objectNames[0], uploadIDs[1], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[1], objectNames[0], uploadIDs[2], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[1], objectNames[0], uploadIDs[3], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
	}
	// Iterating over creatPartCases to generate parts.
	for _, testCase := range createPartCases {
		_, err := obj.PutObjectPart(testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, testCase.intputDataSize,
			bytes.NewBufferString(testCase.inputReaderData), testCase.inputMd5)
		if err != nil {
			t.Fatalf("PutObjectPart Fail")
		}

	}

	// Expected Results set for asserting ListObjectMultipart test.
	listMultipartResults := []ListMultipartsInfo{
		// Test Case - 1.
		// Used to check that the result produces only one output for the 4 parts uploaded in cases 1-4 of createPartCases above.
		// ListMultipartUploads doesn't list the parts.
		{
			MaxUploads: 100,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 2.
		// Used to check that the result produces only one output for the 4 parts uploaded in cases 1-4 of createPartCases above.
		// KeyMarker is set.
		// ListMultipartUploads doesn't list the parts.
		{
			MaxUploads: 100,
			KeyMarker:  "kin",
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 3.
		// KeyMarker is set, no uploadMetadata expected.
		// ListMultipartUploads doesn't list the parts.
		// Maxupload value is asserted.
		{
			MaxUploads: 100,
			KeyMarker:  "orange",
		},
		// Test Case - 4.
		// KeyMarker is set, no uploadMetadata expected.
		// Maxupload value is asserted.
		{
			MaxUploads: 1,
			KeyMarker:  "orange",
		},
		// Test Case - 5.
		// KeyMarker is set. It contains part of the objectname as KeyPrefix.
		//  Expecting the result to contain one uploadMetadata entry and Istruncated to be false.
		{
			MaxUploads:  10,
			KeyMarker:   "min",
			IsTruncated: false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 6.
		// KeyMarker is set. It contains part of the objectname as KeyPrefix.
		// MaxUploads is set equal to the number of meta data entries in the result, the result contains only one entry.
		// Expecting the result to contain one uploadMetadata entry and IsTruncated to be false.
		{
			MaxUploads:  1,
			KeyMarker:   "min",
			IsTruncated: false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 7.
		// KeyMarker is set. It contains part of the objectname as KeyPrefix.
		// Testing for the case with MaxUploads set to 0.
		// Expecting the result to contain no uploadMetadata entry since MaxUploads is set to 0.
		// Expecting isTruncated to be true.
		{
			MaxUploads:  0,
			KeyMarker:   "min",
			IsTruncated: true,
		},
		// Test Case - 8.
		// KeyMarker is set. It contains part of the objectname as KeyPrefix.
		// KeyMarker is set equal to the object name in the result.
		// Expecting the result to skip the KeyMarker entry.
		{
			MaxUploads:  2,
			KeyMarker:   "minio-object",
			IsTruncated: false,
		},
		// Test Case - 9.
		// Prefix is set. It is set equal to the object name.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain one uploadMetadata entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			Prefix:      "minio-object",
			IsTruncated: false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 10.
		// Setting `Prefix` to contain the object name as its prefix.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain one uploadMetadata entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			Prefix:      "min",
			IsTruncated: false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 11.
		// Setting `Prefix` to contain the object name as its prefix.
		// MaxUploads is set equal to number of meta data entries in the result.
		// Expecting the result to contain one uploadMetadata entry and IsTruncated to be false.
		{
			MaxUploads:  1,
			Prefix:      "min",
			IsTruncated: false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test Case - 12.
		// `Prefix` is set. It doesn't contain object name as its preifx.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting no `Uploads` metadata.
		{
			MaxUploads:  2,
			Prefix:      "orange",
			IsTruncated: false,
		},
		// Test Case - 13.
		// `Prefix` is set. It doesn't contain object name as its preifx.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting no `Uploads` metadata.
		{
			MaxUploads:  2,
			Prefix:      "Asia",
			IsTruncated: false,
		},
		// Test Case - 14.
		// Setting `Delimiter`.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain one uploadMetadata entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			Delimiter:   "/",
			Prefix:      "",
			IsTruncated: false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// Test case - 15.
		// Testing for listing of 3 uploadID's for a given object.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads: 100,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// Test case - 16.
		// Testing for listing of 2 uploadID's for a given object with uploadID Marker set.
		// Istruncted is expected to be false.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:     100,
			UploadIDMarker: uploadIDs[1],
			IsTruncated:    false,
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// Test case - 17.
		// Testing for listing of 2 uploadID's for a given object, setting maxKeys to be 2.
		// There are 3 uploadMetadata in the result (uploadIDs[1-3]), it should be truncated to 2.
		// The last entry in the result, uploadIDs[2], that is should be set as NextUploadIDMarker.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:         2,
			IsTruncated:        true,
			NextKeyMarker:      "minio-object",
			NextUploadIDMarker: uploadIDs[2],
			Uploads: []uploadMetadata{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
			},
		},
	}

	testCases := []struct {
		// Inputs to ListObjects.
		bucket         string
		prefix         string
		keyMarker      string
		uploadIDMarker string
		delimiter      string
		maxUploads     int
		// Expected output of ListObjects.
		expectedResult ListMultipartsInfo
		expectedErr    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names ( Test number 1-4 ).
		{".test", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Valid bucket names, but they donot exist (5-7).
		{"volatile-bucket-1", "", "", "", "", 0, ListMultipartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "", "", "", "", 0, ListMultipartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "", "", "", "", 0, ListMultipartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// Valid, existing bucket, but sending invalid delimeter values (8-9).
		// Empty string < "" > and forward slash < / > are the ony two valid arguments for delimeter.
		{bucketNames[0], "", "", "", "*", 0, ListMultipartsInfo{}, fmt.Errorf("delimiter '%s' is not supported", "*"), false},
		{bucketNames[0], "", "", "", "-", 0, ListMultipartsInfo{}, fmt.Errorf("delimiter '%s' is not supported", "-"), false},
		// Testing for failure cases with both perfix and marker (10).
		// The prefix and marker combination to be valid it should satisy strings.HasPrefix(marker, prefix).
		{bucketNames[0], "asia", "europe-object", "", "", 0, ListMultipartsInfo{},
			fmt.Errorf("Invalid combination of marker '%s' and prefix '%s'", "europe-object", "asia"), false},
		// Setting an invalid combination of uploadIDMarker and Marker (11-12).
		{bucketNames[0], "asia", "asia/europe/", "abc", "", 0, ListMultipartsInfo{},
			fmt.Errorf("Invalid combination of uploadID marker '%s' and marker '%s'", "abc", "asia/europe/"), false},
		{bucketNames[0], "asia", "asia/europe", "abc", "", 0, ListMultipartsInfo{},
			fmt.Errorf("unknown UUID string %s", "abc"), false},

		// Setting up valid case of ListMultiPartUploads.
		// Test case with multiple parts for a single uploadID (13).
		{bucketNames[0], "", "", "", "", 100, listMultipartResults[0], nil, true},
		// Test with a KeyMarker (14-20).
		{bucketNames[0], "", "kin", "", "", 100, listMultipartResults[1], nil, true},
		{bucketNames[0], "", "orange", "", "", 100, listMultipartResults[2], nil, true},
		{bucketNames[0], "", "orange", "", "", 1, listMultipartResults[3], nil, true},
		{bucketNames[0], "", "min", "", "", 10, listMultipartResults[4], nil, true},
		{bucketNames[0], "", "min", "", "", 1, listMultipartResults[5], nil, true},
		{bucketNames[0], "", "min", "", "", 0, listMultipartResults[6], nil, true},
		{bucketNames[0], "", "min", "", "", -1, listMultipartResults[6], nil, true},
		// The result contains only one entry. The  KeyPrefix is set to the object name in the result.
		// Expecting the result to skip the KeyPrefix entry in the result (21).
		{bucketNames[0], "", "minio-object", "", "", 2, listMultipartResults[7], nil, true},
		// Test case containing prefix values.
		// Setting prefix to be equal to object name.(22).
		{bucketNames[0], "minio-object", "", "", "", 2, listMultipartResults[8], nil, true},
		// Setting `prefix` to contain the object name as its prefix (23).
		{bucketNames[0], "min", "", "", "", 2, listMultipartResults[9], nil, true},
		// Setting `prefix` to contain the object name as its prefix (24).
		{bucketNames[0], "min", "", "", "", 1, listMultipartResults[10], nil, true},
		// Setting `prefix` to not to contain the object name as its prefix (25-26).
		{bucketNames[0], "orange", "", "", "", 2, listMultipartResults[11], nil, true},
		{bucketNames[0], "Asia", "", "", "", 2, listMultipartResults[12], nil, true},
		// setting delimiter (27).
		{bucketNames[0], "", "", "", "/", 2, listMultipartResults[13], nil, true},
		// Test case with multiple uploadID listing for given object (28).
		{bucketNames[1], "", "", "", "", 100, listMultipartResults[14], nil, true},
		// Test case with multiple uploadID listing for given object, but uploadID marker set.
		// Testing whether the marker entry is skipped (29).
		{bucketNames[1], "", "", uploadIDs[1], "", 100, listMultipartResults[15], nil, true},
		// Test case with multiple uploadID listing for a given object, but with maxKeys set to 2.
		// IsTruncated is expected to be true (30).
		{bucketNames[1], "", "", "", "", 2, listMultipartResults[16], nil, true},
	}

	for i, testCase := range testCases {
		actualResult, actualErr := obj.ListMultipartUploads(testCase.bucket, testCase.prefix, testCase.keyMarker, testCase.uploadIDMarker, testCase.delimiter, testCase.maxUploads)
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: Expected to pass, but failed with: <ERROR> %s", i+1, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, testCase.expectedErr.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if !strings.Contains(actualErr.Error(), testCase.expectedErr.Error()) {
				t.Errorf("Test %d: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, testCase.expectedErr.Error(), actualErr.Error())
			}
		}
		// Passes as expected, but asserting the results.
		if actualErr == nil && testCase.shouldPass {
			expectedResult := testCase.expectedResult
			// Asserting the MaxUploads.
			if actualResult.MaxUploads != expectedResult.MaxUploads {
				t.Errorf("Test %d: Expected the MaxUploads to be %d, but instead found it to be %d", i+1, expectedResult.MaxUploads, actualResult.MaxUploads)
			}
			// Asserting Prefix.
			if actualResult.Prefix != expectedResult.Prefix {
				t.Errorf("Test %d: Expected Prefix to be \"%s\", but instead found it to be \"%s\"", i+1, expectedResult.Prefix, actualResult.Prefix)
			}
			// Asserting Delimiter.
			if actualResult.Delimiter != expectedResult.Delimiter {
				t.Errorf("Test %d: Expected Delimiter to be \"%s\", but instead found it to be \"%s\"", i+1, expectedResult.Delimiter, actualResult.Delimiter)
			}
			// Asserting NextUploadIDMarker.
			if actualResult.NextUploadIDMarker != expectedResult.NextUploadIDMarker {
				t.Errorf("Test %d: Expected NextUploadIDMarker to be \"%s\", but instead found it to be \"%s\"", i+1, expectedResult.NextUploadIDMarker, actualResult.NextUploadIDMarker)
			}
			// Asserting NextKeyMarker.
			if actualResult.NextKeyMarker != expectedResult.NextKeyMarker {
				t.Errorf("Test %d: Expected NextKeyMarker to be \"%s\", but instead found it to be \"%s\"", i+1, expectedResult.NextKeyMarker, actualResult.NextKeyMarker)
			}
			// Asserting the keyMarker.
			if actualResult.KeyMarker != expectedResult.KeyMarker {
				t.Errorf("Test %d: Expected keyMarker to be \"%s\", but instead found it to be \"%s\"", i+1, expectedResult.KeyMarker, actualResult.KeyMarker)
			}
			// Asserting IsTruncated.
			if actualResult.IsTruncated != testCase.expectedResult.IsTruncated {
				t.Errorf("Test %d: Expected Istruncated to be \"%v\", but found it to \"%v\"", i+1, expectedResult.IsTruncated, actualResult.IsTruncated)
			}
			// Asserting the number of upload Metadata info.
			if len(expectedResult.Uploads) != len(actualResult.Uploads) {
				t.Fatalf("Test %d: Expected the result to contain info of %d Multipart Uploads, but found %d instead", i+1, len(expectedResult.Uploads), len(actualResult.Uploads))
			}
			// Iterating over the uploads Metadata and asserting the fields.
			for j, actualMetaData := range actualResult.Uploads {
				//  Asserting the object name in the upload Metadata.
				if actualMetaData.Object != expectedResult.Uploads[j].Object {
					t.Errorf("Test %d: Metadata %d: Expected Metadata Object to be \"%s\", but instead found \"%s\"", i+1, j+1, expectedResult.Uploads[j].Object, actualMetaData.Object)
				}
				//  Asserting the uploadID in the upload Metadata.
				if actualMetaData.UploadID != expectedResult.Uploads[j].UploadID {
					t.Errorf("Test %d: Metadata %d: Expected Metadata UploadID to be \"%s\", but instead found \"%s\"", i+1, j+1, expectedResult.Uploads[j].UploadID, actualMetaData.UploadID)
				}
			}
		}
	}
}
