//go:build osstest
// +build osstest

package oss

import (
	"log"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"

	oss "github.com/IceFireDB/IceFireDB/driver/oss"
	"github.com/philippgille/gokv/encoding"

	"github.com/philippgille/gokv/test"
)

// For Minio Docker container.
// See https://docs.minio.io/docs/minio-docker-quickstart-guide.html.
const (
	customEndpoint string = "http://localhost:9000"
	accesskey      string = "AKIAIOSFODNN7EXAMPLE"
	secretkey      string = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	bucketname     string = "gokv"
)

// TestClient tests if reading from, writing to and deleting from the store works properly.
// A struct is used as value. See TestTypes() for a test that is simpler but tests all types.
//
// Note: This test is only executed if the initial connection to S3 works.
func TestClient(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to S3 could be established. Probably not running in a proper test environment.")
	}

	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		test.TestStore(client, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		test.TestStore(client, t)
	})
}

// TestTypes tests if setting and getting values works with all Go types.
//
// Note: This test is only executed if the initial connection to S3 works.
func TestTypes(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to S3 could be established. Probably not running in a proper test environment.")
	}

	// Test with JSON
	t.Run("JSON", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		test.TestTypes(client, t)
	})

	// Test with gob
	t.Run("gob", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		test.TestTypes(client, t)
	})
}

// TestClientConcurrent launches a bunch of goroutines that concurrently work with the S3 client.
//
// Note: This test is only executed if the initial connection to S3 works.
func TestClientConcurrent(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to S3 could be established. Probably not running in a proper test environment.")
	}

	client := createClient(t, encoding.JSON)

	goroutineCount := 1000

	test.TestConcurrentInteractions(t, goroutineCount, client)
}

// TestErrors tests some error cases.
//
// Note: This test is only executed if the initial connection to S3 works.
func TestErrors(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to S3 could be established. Probably not running in a proper test environment.")
	}

	// Test empty key
	client := createClient(t, encoding.JSON)
	err := client.Set("", "bar")
	if err == nil {
		t.Error("Expected an error")
	}
	_, err = client.Get("", new(string))
	if err == nil {
		t.Error("Expected an error")
	}
	err = client.Delete("")
	if err == nil {
		t.Error("Expected an error")
	}

	// Test client creation with bad options
	options := oss.Options{
		AWSaccessKeyID:     accesskey,
		AWSsecretAccessKey: secretkey,
	}
	client, err = oss.NewClient(options)
	if err.Error() != "The BucketName in the options must not be empty" {
		t.Error("An error was expected, but didn't occur.")
	}
	options = oss.Options{
		BucketName:     bucketname,
		AWSaccessKeyID: accesskey,
	}
	client, err = oss.NewClient(options)
	if err.Error() != "When passing credentials via options, you need to set BOTH AWSaccessKeyID AND AWSsecretAccessKey" {
		t.Error("An error was expected, but didn't occur.")
	}
	options = oss.Options{
		BucketName:         bucketname,
		AWSsecretAccessKey: secretkey,
	}
	client, err = oss.NewClient(options)
	if err.Error() != "When passing credentials via options, you need to set BOTH AWSaccessKeyID AND AWSsecretAccessKey" {
		t.Error("An error was expected, but didn't occur.")
	}
	// Bad credentials on actual AWS S3 endpoint (no custom endpoint for local Docker container)
	options = oss.Options{
		BucketName:         bucketname,
		AWSaccessKeyID:     accesskey,
		AWSsecretAccessKey: secretkey,
		Region:             endpoints.UsWest2RegionID,
	}
	_, err = oss.NewClient(options)
	if strings.Index(err.Error(), "InvalidAccessKeyId: The AWS Access Key Id you provided does not exist in our records.") != 0 {
		t.Errorf("An InvalidAccessKeyId error was expected, but it seems like it didn't occur. Instead, the error was: %v", err)
	}
}

// TestNil tests the behaviour when passing nil or pointers to nil values to some methods.
//
// Note: This test is only executed if the initial connection to S3 works.
func TestNil(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to S3 could be established. Probably not running in a proper test environment.")
	}

	// Test setting nil

	t.Run("set nil with JSON marshalling", func(t *testing.T) {
		client := createClient(t, encoding.JSON)
		err := client.Set("foo", nil)
		if err == nil {
			t.Error("Expected an error")
		}
	})

	t.Run("set nil with Gob marshalling", func(t *testing.T) {
		client := createClient(t, encoding.Gob)
		err := client.Set("foo", nil)
		if err == nil {
			t.Error("Expected an error")
		}
	})

	// Test passing nil or pointer to nil value for retrieval

	createTest := func(codec encoding.Codec) func(t *testing.T) {
		return func(t *testing.T) {
			client := createClient(t, codec)

			// Prep
			err := client.Set("foo", test.Foo{Bar: "baz"})
			if err != nil {
				t.Error(err)
			}

			_, err = client.Get("foo", nil) // actually nil
			if err == nil {
				t.Error("An error was expected")
			}

			var i interface{} // actually nil
			_, err = client.Get("foo", i)
			if err == nil {
				t.Error("An error was expected")
			}

			var valPtr *test.Foo // nil value
			_, err = client.Get("foo", valPtr)
			if err == nil {
				t.Error("An error was expected")
			}
		}
	}
	t.Run("get with nil / nil value parameter", createTest(encoding.JSON))
	t.Run("get with nil / nil value parameter", createTest(encoding.Gob))
}

// TestClose tests if the close method returns any errors.
//
// Note: This test is only executed if the initial connection to S3 works.
func TestClose(t *testing.T) {
	if !checkConnection() {
		t.Skip("No connection to S3 could be established. Probably not running in a proper test environment.")
	}

	client := createClient(t, encoding.JSON)
	err := client.Close()
	if err != nil {
		t.Error(err)
	}
}

// checkConnection returns true if a connection could be made, false otherwise.
func checkConnection() bool {
	os.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	// No S3ForcePathStyle required because we're not accessing a specific bucket here.
	sess, err := session.NewSession(aws.NewConfig().WithRegion(endpoints.UsEast1RegionID).WithEndpoint(customEndpoint))
	if err != nil {
		log.Printf("An error occurred during testing the connection to the server: %v\n", err)
		return false
	}
	svc := awss3.New(sess)

	_, err = svc.ListBuckets(&awss3.ListBucketsInput{})
	if err != nil {
		log.Printf("An error occurred during testing the connection to the server: %v\n", err)
		return false
	}

	return true
}

func createClient(t *testing.T, codec encoding.Codec) oss.Client {
	//	os.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	///	os.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	options := oss.Options{
		BucketName:             bucketname,
		Region:                 endpoints.UsEast1RegionID, // endpoints.EuCentral1RegionID,
		CustomEndpoint:         customEndpoint,
		UsePathStyleAddressing: true,
		Codec:                  codec,
		AWSaccessKeyID:         accesskey,
		AWSsecretAccessKey:     secretkey,
	}
	client, err := oss.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

//Thanks, from https://github.com/philippgille/gokv
