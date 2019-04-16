/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bucket

import (
	"fmt"
	_ "net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/coreos/pkg/capnslog"
	"github.com/yard-turkey/lib-bucket-provisioner/pkg/apis/objectbucket.io/v1alpha1"
	libbkt "github.com/yard-turkey/lib-bucket-provisioner/pkg/provisioner"
	apibkt "github.com/yard-turkey/lib-bucket-provisioner/pkg/provisioner/api"
	bkterr "github.com/yard-turkey/lib-bucket-provisioner/pkg/provisioner/api/errors"

	storageV1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

var logger = capnslog.NewPackageLogger("github.com/rook/rook", "op-bucket-prov")

const (
	httpPort         = 80
	httpsPort        = 443
	provisionerName  = "ceph.rook.io/bucket"
	regionInsert     = "<REGION>"
	s3Hostname       = "s3-" + regionInsert + ".amazonaws.com"
	createBucketUser = false
	obStateARN       = "ARN"
	obStateUser      = "UserName"
	maxBucketLen     = 58
	maxUserLen       = 63
	genUserLen       = 5
)

type Provisioner struct {
	bucketName string
	region     string
	//kube client
	clientset kubernetes.Interface
	// the namespace where the rook cluster should respond to events
	namespace string
	// access keys for aws acct for the bucket *owner*
	bktOwnerAccessId  string
	bktOwnerSecretKey string
	bktCreateUser     string
	bktUserName       string
	bktUserAccessId   string
	bktUserSecretKey  string
	bktUserAccountId  string
	bktUserPolicyArn  string
}

func NewProvisioner(clientset *kubernetes.Interface, namespace string) *Provisioner {
	return &Provisioner{clientset: clientset, namespace: namespace}
}

func NewBucketController(cfg *restclient.Config, p provisioner) (*libbkt.Controller, error) {
	const allNamespaces = ""
	return libbkt.NewProvisioner(cfg, provisionerName, p, allNamespaces)
}

// Return the aws default session.
func awsDefaultSession() (*session.Session, error) {

	logger.Infof("Creating AWS *default* session")
	return session.NewSession(&aws.Config{
		Region: aws.String(defaultRegion),
		//Credentials: credentials.NewStaticCredentials(os.Getenv),
	})
}

// Return the OB struct with minimal fields filled in.
func (p *provisioner) rtnObjectBkt(bktName string) *v1alpha1.ObjectBucket {

	host := strings.Replace(s3Hostname, regionInsert, p.region, 1)
	conn := &v1alpha1.Connection{
		Endpoint: &v1alpha1.Endpoint{
			BucketHost: host,
			BucketPort: httpsPort,
			BucketName: bktName,
			Region:     p.region,
			SSL:        true,
		},
		Authentication: &v1alpha1.Authentication{
			AccessKeys: &v1alpha1.AccessKeys{
				AccessKeyID:     p.bktUserAccessId,
				SecretAccessKey: p.bktUserSecretKey,
			},
		},
		AdditionalState: map[string]string{
			obStateARN:  p.bktUserPolicyArn,
			obStateUser: p.bktUserName,
		},
	}

	return &v1alpha1.ObjectBucket{
		Spec: v1alpha1.ObjectBucketSpec{
			Connection: conn,
		},
	}
}

func (p *provisioner) createBucket(bktName string) error {

	bucketinput := &s3.CreateBucketInput{
		Bucket: &bktName,
	}

	_, err := p.s3svc.CreateBucket(bucketinput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				msg := fmt.Sprintf("Bucket %q already exists", bktName)
				logger.Errorf(msg)
				return bkterr.NewBucketExistsError(msg)
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				msg := fmt.Sprintf("Bucket %q already owned by you", bktName)
				logger.Errorf(msg)
				return bkterr.NewBucketExistsError(msg)
			}
		}
		return fmt.Errorf("Bucket %q could not be created: %v", bktName, err)
	}
	logger.Infof("Bucket %s successfully created", bktName)

	//Now at this point, we have a bucket and an owner
	//we should now create the user for the bucket
	return nil
}

// Create an aws session based on the OBC's storage class's secret and region.
// Set in the receiver the session and region used to create the session.
// Note: in error cases it's possible that the set region is different from
//   the OBC's storage class's region.
func (p *provisioner) awsSessionFromStorageClass(sc *storageV1.StorageClass) error {

	// helper func var for error returns
	var errDefault = func() error {
		var err error
		p.region = defaultRegion
		p.session, err = awsDefaultSession()
		return err
	}

	region := getRegion(sc)
	if region == "" {
		logger.Infof("region is empty in storage class %q, default region %q used", sc.Name, defaultRegion)
		region = defaultRegion
	}
	secretNS, secretName := getSecretName(sc)
	if secretNS == "" || secretName == "" {
		logger.Infof("secret name or namespace are empty in storage class %q", sc.Name)
		return errDefault()
	}

	// get the sc's bucket owner secret
	err := p.credsFromSecret(p.clientset, secretNS, secretName)
	if err != nil {
		logger.Warningf("secret \"%s/%s\" in storage class %q for %q is empty.\nUsing default credentials.", secretNS, secretName, sc.Name, p.bucketName)
		return errDefault()
	}

	// use the OBC's SC to create our session, set receiver fields
	logger.Infof("Creating AWS session using credentials from storage class %s's secret", sc.Name)
	p.region = region
	p.session, err = session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(p.bktOwnerAccessId, p.bktOwnerSecretKey, ""),
	})

	return err
}

// Create the AWS session and S3 service and store them to the receiver.
func (p *provisioner) setSessionAndService(sc *storageV1.StorageClass) error {
	// set the aws session
	logger.Infof("Creating AWS session based on storageclass %q", sc.Name)
	err := p.awsSessionFromStorageClass(sc)
	if err != nil {
		return fmt.Errorf("error creating AWS session: %v", err)
	}

	logger.Infof("Creating S3 service based on storageclass %q", sc.Name)
	p.s3svc = s3.New(p.session)
	if p.s3svc == nil {
		return fmt.Errorf("error creating S3 service: %v", err)
	}

	return nil
}

// initializeCreateOrGrant sets common provisioner receiver fields and
// the services and sessions needed to provision.
func (p *provisioner) initializeCreateOrGrant(options *apibkt.BucketOptions) error {
	logger.Infof("initializing and setting CreateOrGrant services")
	// set the bucket name
	p.bucketName = options.BucketName

	// get the OBC and its storage class
	obc := options.ObjectBucketClaim
	scName := options.ObjectBucketClaim.Spec.StorageClassName
	sc, err := p.getClassByNameForBucket(scName)
	if err != nil {
		logger.Errorf("failed to get storage class for OBC \"%s/%s\": %v", obc.Namespace, obc.Name, err)
		return err
	}

	// check for bkt user access policy vs. bkt owner policy based on SC
	p.setCreateBucketUserOptions(obc, sc)

	// set the aws session and s3 service from the storage class
	err = p.setSessionAndService(sc)
	if err != nil {
		return fmt.Errorf("error using OBC \"%s/%s\": %v", obc.Namespace, obc.Name, err)
	}

	return nil
}

// initializeUserAndPolicy sets commonly used provisioner
// receiver fields, generates a unique username and calls
// handleUserandPolicy.
func (p *provisioner) initializeUserAndPolicy() error {

	//Create IAM service (maybe this should be added into our default or obc session
	//or create all services type of function?
	p.iamsvc = awsuser.New(p.session)

	// TODO: default access and key are set to bkt owner.
	//   This needs to be more restrictive or a failure...
	p.bktUserAccessId = p.bktOwnerAccessId
	p.bktUserSecretKey = p.bktOwnerSecretKey
	if p.bktCreateUser == "yes" {
		// Create a new IAM user using the name of the bucket and set
		// access and attach policy for bucket and user
		p.bktUserName = p.createUserName(p.bucketName)

		// handle all iam and policy operations
		uAccess, uKey, err := p.handleUserAndPolicy(p.bucketName)
		if err != nil || uAccess == "" || uKey == "" {
			//what to do - something failed along the way
			//do we fall back and create our connection with
			//the default bktOwnerRef?
			logger.Errorf("Something failed along the way for handling Users and Policy: %v", err)
		} else {
			p.bktUserAccessId = uAccess
			p.bktUserSecretKey = uKey
		}
	}
	return nil
}

func (p *provisioner) checkIfBucketExists(name string) bool {

	input := &s3.HeadBucketInput{
		Bucket: aws.String(name),
	}

	_, err := p.s3svc.HeadBucket(input)
	if err != nil {
		if err.(awserr.Error).Code() == s3.ErrCodeNoSuchBucket {
			return false
		}
		return false
	}

	return true
}

func (p *provisioner) checkIfUserExists(name string) bool {

	input := &awsuser.GetUserInput{
		UserName: aws.String(name),
	}

	_, err := p.iamsvc.GetUser(input)
	if err != nil {
		if err.(awserr.Error).Code() == awsuser.ErrCodeEntityAlreadyExistsException {
			return true
		}
		return false
	}

	return false
}

// Provision creates an s3 bucket and returns a connection info
// representing the bucket's endpoint and user access credentials.
// Programming Note: _all_ methods on "provisioner" called directly
//   or indirectly by `Provision` should use pointer receivers. This allows
//   all supporting methods to set receiver fields where convenient. An
//   alternative (arguably better) would be for all supporting methods
//   to take value receivers and functionally return back to `Provision`
//   receiver fields they need set. The first approach is easier for now.
func (p provisioner) Provision(options *apibkt.BucketOptions) (*v1alpha1.ObjectBucket, error) {

	// initialize and set the AWS services and commonly used variables
	err := p.initializeCreateOrGrant(options)
	if err != nil {
		return nil, err
	}

	// create the bucket
	logger.Infof("Creating bucket %q", p.bucketName)
	err = p.createBucket(p.bucketName)
	if err != nil {
		err = fmt.Errorf("error creating bucket %q. %+v", p.bucketName, err)
		logger.Errorf(err.Error())
		return nil, err
	}

	// createBucket was successful, deal with user and policy
	// Bucket does exist, attach new user and policy wrapper
	// calling initializeCreateOrGrant
	if err := p.initializeUserAndPolicy(); err != nil {
		err = fmt.Errorf("error initializing bucket user and policy. %+v", err)
		logger.Errorf(err.Error())
		return nil, err
	}

	// returned ob with connection info
	return p.rtnObjectBkt(p.bucketName), nil
}

// Grant attaches to an existing aws s3 bucket and returns a connection info
// representing the bucket's endpoint and user access credentials.
func (p provisioner) Grant(options *apibkt.BucketOptions) (*v1alpha1.ObjectBucket, error) {

	// initialize and set the AWS services and commonly used variables
	err := p.initializeCreateOrGrant(options)
	if err != nil {
		return nil, err
	}

	// check and make sure the bucket exists
	logger.Infof("Checking for existing bucket %q", p.bucketName)
	if !p.checkIfBucketExists(p.bucketName) {
		return nil, fmt.Errorf("bucket %s does not exist", p.bucketName)
	}

	// Bucket does exist, attach new user and policy wrapper
	// calling initializeUserAndPolicy
	// TODO: we currently are catching an error that is always nil
	_ = p.initializeUserAndPolicy()

	// returned ob with connection info
	// TODO: assuming this is the same Green vs Brown?
	return p.rtnObjectBkt(p.bucketName), nil
}

// Delete the bucket and all its objects.
// Note: only called when the bucket's reclaim policy is "delete".
func (p provisioner) Delete(ob *v1alpha1.ObjectBucket) error {

	// set receiver fields from OB data
	p.bucketName = ob.Spec.Endpoint.BucketName
	p.bktUserPolicyArn = ob.Spec.AdditionalState[obStateARN]
	p.bktUserName = ob.Spec.AdditionalState[obStateUser]
	scName := ob.Spec.StorageClassName
	logger.Infof("Deleting bucket %q for OB %q", p.bucketName, ob.Name)

	// get the OB and its storage class
	sc, err := p.getClassByNameForBucket(scName)
	if err != nil {
		return fmt.Errorf("failed to get storage class for OB %q: %v", ob.Name, err)
	}

	// set the aws session and s3 service from the storage class
	err = p.setSessionAndService(sc)
	if err != nil {
		return fmt.Errorf("error using OB %q: %v", ob.Name, err)
	}

	// Delete IAM Policy and User
	err = p.handleUserAndPolicyDeletion(p.bucketName)
	if err != nil {
		// We are currently only logging
		// because if failure do not want to stop
		// deletion of bucket
		logger.Errorf("Failed to delete Policy and/or User - manual clean up required")
		//return fmt.Errorf("Error deleting Policy and/or User %v", err)
	}

	// Delete Bucket
	iter := s3manager.NewDeleteListIterator(p.s3svc, &s3.ListObjectsInput{
		Bucket: aws.String(p.bucketName),
	})

	logger.Infof("Deleting all objects in bucket %q (from OB %q)", p.bucketName, ob.Name)
	err = s3manager.NewBatchDeleteWithClient(p.s3svc).Delete(aws.BackgroundContext(), iter)
	if err != nil {
		return fmt.Errorf("Error deleting objects from bucket %q: %v", p.bucketName, err)
	}

	logger.Infof("Deleting empty bucket %q from OB %q", p.bucketName, ob.Name)
	_, err = p.s3svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(p.bucketName),
	})
	if err != nil {
		return fmt.Errorf("Error deleting empty bucket %q: %v", p.bucketName, err)
	}
	logger.Infof("Deleted bucket %q from OB %q", p.bucketName, ob.Name)

	return nil
}

// Revoke removes a user, policy and access keys from an existing bucket.
func (p provisioner) Revoke(ob *v1alpha1.ObjectBucket) error {
	//TODO: need to make sure we are deleting correct user

	// set receiver fields from OB data
	p.bucketName = ob.Spec.Endpoint.BucketName
	p.bktUserPolicyArn = ob.Spec.AdditionalState[obStateARN]
	p.bktUserName = ob.Spec.AdditionalState[obStateUser]
	scName := ob.Spec.StorageClassName
	logger.Infof("Revoking access to bucket %q for OB %q", p.bucketName, ob.Name)

	// get the OB and its storage class
	sc, err := p.getClassByNameForBucket(scName)
	if err != nil {
		return fmt.Errorf("failed to get storage class for OB %q: %v", ob.Name, err)
	}

	// set the aws session and s3 service from the storage class
	err = p.setSessionAndService(sc)
	if err != nil {
		return fmt.Errorf("error using OB %q: %v", ob.Name, err)
	}

	// Delete IAM Policy and User
	err = p.handleUserAndPolicyDeletion(p.bucketName)
	if err != nil {
		// We are currently only logging
		// because if failure do not want to stop
		// deletion of bucket
		logger.Errorf("Failed to delete Policy and/or User - manual clean up required")
		//return fmt.Errorf("Error deleting Policy and/or User %v", err)
	}

	// No Deletion of Bucket or Content in Brownfield
	return nil
}
