/*
Copyright 2016 The Rook Authors. All rights reserved.

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

// Package cluster to manage a Ceph cluster.
package cluster

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/coreos/pkg/capnslog"
	opkit "github.com/rook/operator-kit"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	cephbeta "github.com/rook/rook/pkg/apis/ceph.rook.io/v1beta1"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/agent/flexvolume/attachment"
	"github.com/rook/rook/pkg/operator/ceph/cluster/mon"
	"github.com/rook/rook/pkg/operator/ceph/cluster/osd"
	"github.com/rook/rook/pkg/operator/ceph/file"
	"github.com/rook/rook/pkg/operator/ceph/nfs"
	"github.com/rook/rook/pkg/operator/ceph/object"
	objectuser "github.com/rook/rook/pkg/operator/ceph/object/user"
	"github.com/rook/rook/pkg/operator/ceph/pool"
	"github.com/rook/rook/pkg/operator/k8sutil"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/kubelet/apis"
)

const (
	crushConfigMapName    = "rook-crush-config"
	crushmapCreatedKey    = "initialCrushMapCreated"
	clusterCreateInterval = 6 * time.Second
	clusterCreateTimeout  = 60 * time.Minute
	updateClusterInterval = 30 * time.Second
	updateClusterTimeout  = 1 * time.Hour
)

const (
	// DefaultClusterName states the default name of the rook-cluster if not provided.
	DefaultClusterName         = "rook-ceph"
	clusterDeleteRetryInterval = 2 //seconds
	clusterDeleteMaxRetries    = 15
)

var (
	logger                  = capnslog.NewPackageLogger("github.com/rook/rook", "op-cluster")
	finalizerName           = fmt.Sprintf("%s.%s", ClusterResource.Name, ClusterResource.Group)
	finalizerNameRookLegacy = fmt.Sprintf("%s.%s", ClusterResourceRookLegacy.Name, ClusterResourceRookLegacy.Group)
)

var ClusterResource = opkit.CustomResource{
	Name:    "cephcluster",
	Plural:  "cephclusters",
	Group:   cephv1.CustomResourceGroup,
	Version: cephv1.Version,
	Scope:   apiextensionsv1beta1.NamespaceScoped,
	Kind:    reflect.TypeOf(cephv1.CephCluster{}).Name(),
}

var ClusterResourceRookLegacy = opkit.CustomResource{
	Name:    "cluster",
	Plural:  "clusters",
	Group:   cephbeta.CustomResourceGroup,
	Version: cephbeta.Version,
	Scope:   apiextensionsv1beta1.NamespaceScoped,
	Kind:    reflect.TypeOf(cephbeta.Cluster{}).Name(),
}

// ClusterController controls an instance of a Rook cluster
type ClusterController struct {
	context            *clusterd.Context
	volumeAttachment   attachment.Attachment
	devicesInUse       bool
	rookImage          string
	clusterMap         map[string]*cluster
	addClusterCallback func(bool) error
}

// NewClusterController create controller for watching cluster custom resources created
func NewClusterController(context *clusterd.Context, rookImage string, volumeAttachment attachment.Attachment, addClusterCallback func(bool) error) *ClusterController {
	return &ClusterController{
		context:            context,
		volumeAttachment:   volumeAttachment,
		rookImage:          rookImage,
		clusterMap:         make(map[string]*cluster),
		addClusterCallback: addClusterCallback,
	}
}

// Watch watches instances of cluster resources
func (c *ClusterController) StartWatch(namespace string, stopCh chan struct{}) error {
	resourceHandlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onAdd,
		UpdateFunc: c.onUpdate,
		DeleteFunc: c.onDelete,
	}

	logger.Infof("start watching clusters in all namespaces")
	watcher := opkit.NewWatcher(ClusterResource, namespace, resourceHandlerFuncs, c.context.RookClientset.CephV1().RESTClient())
	go watcher.Watch(&cephv1.CephCluster{}, stopCh)

	// watch for events on new/updated K8s nodes, too

	lwNodes := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.context.Clientset.CoreV1().Nodes().List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.context.Clientset.CoreV1().Nodes().Watch(options)
		},
	}

	_, nodeController := cache.NewInformer(
		lwNodes,
		&v1.Node{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.onK8sNodeAdd,
			UpdateFunc: c.onK8sNodeUpdate,
			DeleteFunc: nil,
		},
	)
	go nodeController.Run(stopCh)

	// watch for events on all legacy types too
	c.watchLegacyClusters(namespace, stopCh, resourceHandlerFuncs)

	return nil
}

func (c *ClusterController) StopWatch() {
	for _, cluster := range c.clusterMap {
		close(cluster.stopCh)
	}
	c.clusterMap = make(map[string]*cluster)
}

// ************************************************************************************************
// Add event functions
// ************************************************************************************************
func (c *ClusterController) onK8sNodeAdd(obj interface{}) {
	newNode, ok := obj.(*v1.Node)
	if !ok {
		logger.Warningf("Expected NodeList but handler received %#v", obj)
	}

	if k8sutil.GetNodeSchedulable(*newNode) == false {
		logger.Debugf("Skipping cluster update. Added node %s is unschedulable", newNode.Labels[apis.LabelHostname])
		return
	}

	for _, cluster := range c.clusterMap {
		if cluster.Spec.Storage.UseAllNodes == false {
			logger.Debugf("Skipping -> Do not use all Nodes")
			continue
		}

		if valid, _ := k8sutil.ValidNode(*newNode, cluster.Spec.Placement.All()); valid == true {
			logger.Debugf("Adding %s to cluster %s", newNode.Labels[apis.LabelHostname], cluster.Namespace)
			err := cluster.createInstance(c.rookImage, cluster.Info.CephVersion)
			if err != nil {
				logger.Errorf("Failed to update cluster in namespace %s. Was not able to add %s. %+v", cluster.Namespace, newNode.Labels[apis.LabelHostname], err)
			}
		} else {
			logger.Infof("Could not add host %s . It is not valid", newNode.Labels[apis.LabelHostname])
			continue
		}
		logger.Infof("Added %s to cluster %s", newNode.Labels[apis.LabelHostname], cluster.Namespace)
	}
}

func (c *ClusterController) onAdd(obj interface{}) {

	clusterObj, migrationNeeded, err := getClusterObject(obj)
	if err != nil {
		logger.Errorf("failed to get cluster object: %+v", err)
		return
	}

	if migrationNeeded {
		err = c.migrateClusterObject(clusterObj, obj)
		if err != nil {
			logger.Errorf("failed to migrate legacy cluster %s in namespace %s: %+v", clusterObj.Name, clusterObj.Namespace, err)
		}

		// no matter the outcome of the migration, bail out now. if it was successful, then we'll be getting
		// another event for the migrated object and we'll just handle it there.
		return
	}

	if existing, ok := c.clusterMap[clusterObj.Namespace]; ok {
		logger.Errorf("Failed to add cluster crd %s in namespace %s. Cluster crd %s already exists in this namespace. Only one cluster crd per namespace is supported.",
			clusterObj.Name, clusterObj.Namespace, existing.crdName)
		return
	}

	cluster := newCluster(clusterObj, c.context)
	c.clusterMap[cluster.Namespace] = cluster

	logger.Infof("starting cluster in namespace %s", cluster.Namespace)

	// notify the callback that a cluster crd is being added
	if c.addClusterCallback != nil {
		if err := c.addClusterCallback(cluster.Spec.ExternalCeph); err != nil {
			logger.Errorf("%+v", err)
		}
	}

	if !cluster.Spec.ExternalCeph {
		if err := c.configureLocalCephCluster(clusterObj.Namespace, clusterObj.Name, cluster); err != nil {
			logger.Errorf("failed to configure local ceph cluster. %+v", err)
			return
		}
	} else {
		if err := c.configureExternalCephCluster(clusterObj.Namespace, clusterObj.Name, cluster); err != nil {
			logger.Errorf("failed to configure external ceph cluster. %+v", err)
			return
		}
	}

	// add the finalizer to the crd
	err = c.addFinalizer(clusterObj.Namespace, clusterObj.Name)
	if err != nil {
		logger.Errorf("failed to add finalizer to cluster crd. %+v", err)
		return
	}

	// Start pool CRD watcher
	poolController := pool.NewPoolController(c.context, cluster.Spec)
	poolController.StartWatch(cluster.Namespace, cluster.stopCh)

	// Start object store CRD watcher
	objectStoreController := object.NewObjectStoreController(cluster.Info, c.context, c.rookImage, cluster.Spec, cluster.ownerRef)
	objectStoreController.StartWatch(cluster.Namespace, cluster.stopCh)

	// Start object store user CRD watcher
	objectStoreUserController := objectuser.NewObjectStoreUserController(c.context, cluster.Spec, cluster.ownerRef)
	objectStoreUserController.StartWatch(cluster.Namespace, cluster.stopCh)

	// Start file system CRD watcher
	fileController := file.NewFilesystemController(cluster.Info, c.context, c.rookImage, cluster.Spec, cluster.ownerRef)
	fileController.StartWatch(cluster.Namespace, cluster.stopCh)

	// Start nfs ganesha CRD watcher
	ganeshaController := nfs.NewCephNFSController(cluster.Info, c.context, c.rookImage, cluster.Spec, cluster.ownerRef)
	ganeshaController.StartWatch(cluster.Namespace, cluster.stopCh)

	// Start mon health checker
	healthChecker := mon.NewHealthChecker(cluster.mons, cluster.Spec)
	go healthChecker.Check(cluster.stopCh)

	if !cluster.Spec.ExternalCeph {
		// Start the osd health checker only if running OSDs in the local ceph cluster
		osdChecker := osd.NewMonitor(c.context, cluster.Namespace)
		go osdChecker.Start(cluster.stopCh)
	}
}

func (c *ClusterController) configureExternalCephCluster(namespace, name string, cluster *cluster) error {
	if err := c.updateClusterStatus(namespace, name, cephv1.ClusterStateConnecting, ""); err != nil {
		logger.Warningf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
	}

	// loop until we find the secret necessary to connect to the external cluster
	for {
		var err error
		cluster.Info, _, _, err = mon.LoadClusterInfo(c.context, namespace)
		if err != nil {
			logger.Warningf("waiting for the connection info to the external cluster. %+v", err)
			time.Sleep(10 * time.Second)
			continue
		}
		logger.Infof("Found the cluster info to connect to the external cluster. mons=%+v", cluster.Info.Monitors)
		if err := c.updateClusterStatus(namespace, name, cephv1.ClusterStateConnected, ""); err != nil {
			logger.Warningf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
		}
		return nil
	}
}

func (c *ClusterController) configureLocalCephCluster(namespace, name string, cluster *cluster) error {
	if c.devicesInUse && cluster.Spec.Storage.AnyUseAllDevices() {
		message := "using all devices in more than one namespace not supported"
		if err := c.updateClusterStatus(namespace, name, cephv1.ClusterStateError, message); err != nil {
			logger.Errorf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
		}
		return fmt.Errorf(message)
	}

	if cluster.Spec.Storage.AnyUseAllDevices() {
		c.devicesInUse = true
	}

	if cluster.Spec.Mon.Count == 0 {
		logger.Warningf("mon count should be at least 1, will use default value of %d", mon.DefaultMonCount)
		cluster.Spec.Mon.Count = mon.DefaultMonCount
	}
	if cluster.Spec.Mon.Count%2 == 0 {
		logger.Warningf("mon count is even (given: %d), should be uneven, continuing", cluster.Spec.Mon.Count)
	}

	cephVersion, err := cluster.detectCephVersion(cluster.Spec.CephVersion.Image, 15*time.Minute)
	if err != nil {
		return fmt.Errorf("unknown ceph major version. %+v", err)
	}

	if !cluster.Spec.CephVersion.AllowUnsupported {
		if !cephVersion.Supported() {
			return fmt.Errorf("unsupported ceph version detected: %s. allowUnsupported must be set to true to run with this version.", cephVersion)
		}
	}

	// Start the Rook cluster components. Retry several times in case of failure.
	err = wait.Poll(clusterCreateInterval, clusterCreateTimeout, func() (bool, error) {
		if err := c.updateClusterStatus(namespace, name, cephv1.ClusterStateCreating, ""); err != nil {
			logger.Errorf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
			return false, nil
		}

		err := cluster.createInstance(c.rookImage, *cephVersion)
		if err != nil {
			logger.Errorf("failed to create cluster in namespace %s. %+v", cluster.Namespace, err)
			return false, nil
		}

		// cluster is created, update the cluster CRD status now
		if err := c.updateClusterStatus(namespace, name, cephv1.ClusterStateCreated, ""); err != nil {
			logger.Errorf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		message := fmt.Sprintf("giving up creating cluster in namespace %s after %s", cluster.Namespace, clusterCreateTimeout)
		if err := c.updateClusterStatus(namespace, name, cephv1.ClusterStateError, message); err != nil {
			logger.Errorf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
		}
		return fmt.Errorf(message)
	}

	return nil
}

// ************************************************************************************************
// Update event functions
// ************************************************************************************************
func (c *ClusterController) onK8sNodeUpdate(oldObj, newObj interface{}) {
	// skip forced resyncs
	if reflect.DeepEqual(oldObj, newObj) {
		return
	}

	// Checking for nodes where NoSchedule-Taint got removed
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		logger.Warningf("Expected Node but handler received %#v", newObj)
		return
	}

	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		logger.Warningf("Expected Node but handler received %#v", oldObj)
		return
	}

	if k8sutil.GetNodeSchedulable(*newNode) == false {
		logger.Debugf("Skipping cluster update. Updated node %s is unschedulable", newNode.Labels[apis.LabelHostname])
		return
	}

	// Checking for nodes where NoSchedule-Taint got removed
	if k8sutil.GetNodeSchedulable(*oldNode) == true {
		logger.Debugf("Skipping cluster update. Updated node %s was and it is still schedulable", oldNode.Labels[apis.LabelHostname])
		return
	}

	for _, cluster := range c.clusterMap {
		if valid, _ := k8sutil.ValidNode(*newNode, cephv1.GetOSDPlacement(cluster.Spec.Placement)); valid == true {
			logger.Debugf("Adding %s to cluster %s", newNode.Labels[apis.LabelHostname], cluster.Namespace)
			err := cluster.createInstance(c.rookImage, cluster.Info.CephVersion)
			if err != nil {
				logger.Errorf("Failed adding the updated node %s to cluster in namespace %s. %+v", newNode.Labels[apis.LabelHostname], cluster.Namespace, err)
				continue
			}
		} else {
			logger.Infof("Updated node %s is not valid and could not get added to cluster in namespace %s.", newNode.Labels[apis.LabelHostname], cluster.Namespace)
			continue
		}
		logger.Infof("Added updated node %s to cluster %s", newNode.Labels[apis.LabelHostname], cluster.Namespace)
	}
}

func (c *ClusterController) onUpdate(oldObj, newObj interface{}) {
	oldClust, _, err := getClusterObject(oldObj)
	if err != nil {
		logger.Errorf("failed to get old cluster object: %+v", err)
		return
	}
	newClust, migrationNeeded, err := getClusterObject(newObj)
	if err != nil {
		logger.Errorf("failed to get new cluster object: %+v", err)
		return
	}

	if migrationNeeded {
		logger.Infof("update event for legacy cluster %s", newClust.Namespace)

		if isLegacyClusterObjectDeleted(newObj) {
			// the legacy cluster object has been requested to be deleted but the finalizer is preventing
			// that.  Let's remove the finalizer and allow the deletion of the legacy object to proceed.
			c.removeFinalizer(newObj)
			return
		}

		if err = c.migrateClusterObject(newClust, newObj); err != nil {
			logger.Errorf("failed to migrate legacy cluster %s in namespace %s: %+v", newClust.Name, newClust.Namespace, err)
		}

		// no matter the outcome of the migration, bail out now. if it was successful, then we'll be getting
		// another event for the migrated object and we'll just handle it there.
		return
	}

	if existing, ok := c.clusterMap[newClust.Namespace]; ok && existing.crdName != newClust.Name {
		logger.Errorf("Skipping update of cluster crd %s in namespace %s. Cluster crd %s already exists in this namespace. Only one cluster crd per namespace is supported.",
			newClust.Name, newClust.Namespace, existing.crdName)
		return
	}

	logger.Infof("update event for cluster %s", newClust.Namespace)

	// Check if the cluster is being deleted. This code path is called when a finalizer is specified in the crd.
	// When a cluster is requested for deletion, K8s will only set the deletion timestamp if there are any finalizers in the list.
	// K8s will only delete the crd and child resources when the finalizers have been removed from the crd.
	if newClust.DeletionTimestamp != nil {
		logger.Infof("cluster %s has a deletion timestamp", newClust.Namespace)
		err := c.handleDelete(newClust, time.Duration(clusterDeleteRetryInterval)*time.Second)
		if err != nil {
			logger.Errorf("failed finalizer for cluster. %+v", err)
			return
		}
		// remove the finalizer from the crd, which indicates to k8s that the resource can safely be deleted
		c.removeFinalizer(newClust)
		return
	}
	cluster, ok := c.clusterMap[newClust.Namespace]
	if !ok {
		logger.Errorf("Cannot update cluster %s that does not exist", newClust.Namespace)
		return
	}

	changed, _ := clusterChanged(oldClust.Spec, newClust.Spec, cluster)
	if !changed {
		logger.Infof("update event for cluster %s is not supported", newClust.Namespace)
		return
	}

	logger.Infof("update event for cluster %s is supported, orchestrating update now", newClust.Namespace)

	// if the image changed, we need to detect the new image version
	cephImageChanged := false
	if oldClust.Spec.CephVersion.Image != newClust.Spec.CephVersion.Image {
		logger.Infof("the ceph version changed. detecting the new image version...")
		cephImageChanged = true
		version, err := cluster.detectCephVersion(newClust.Spec.CephVersion.Image, 15*time.Minute)
		if err != nil {
			logger.Errorf("unknown ceph major version. %+v", err)
			return
		}
		cluster.Info.CephVersion = *version
	} else {
		logger.Infof("ceph version is still %s on image %s", &cluster.Info.CephVersion, cluster.Spec.CephVersion.Image)
	}

	logger.Debugf("old cluster: %+v", oldClust.Spec)
	logger.Debugf("new cluster: %+v", newClust.Spec)

	cluster.Spec = &newClust.Spec

	// attempt to update the cluster.  note this is done outside of wait.Poll because that function
	// will wait for the retry interval before trying for the first time.
	done, _ := c.handleUpdate(newClust.Name, cluster)
	if done {
		return
	}

	err = wait.Poll(updateClusterInterval, updateClusterTimeout, func() (bool, error) {
		return c.handleUpdate(newClust.Name, cluster)
	})
	if err != nil {
		message := fmt.Sprintf("giving up trying to update cluster in namespace %s after %s", cluster.Namespace, updateClusterTimeout)
		logger.Error(message)
		if err := c.updateClusterStatus(newClust.Namespace, newClust.Name, cephv1.ClusterStateError, message); err != nil {
			logger.Errorf("failed to update cluster status in namespace %s: %+v", newClust.Namespace, err)
		}
		return
	}

	if cephImageChanged {
		// TODO: Update all the crd controllers that there is a new version of the ceph image to deploy
	}
}

func (c *ClusterController) handleUpdate(crdName string, cluster *cluster) (bool, error) {
	if err := c.updateClusterStatus(cluster.Namespace, crdName, cephv1.ClusterStateUpdating, ""); err != nil {
		logger.Errorf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
		return false, nil
	}

	if err := cluster.createInstance(c.rookImage, cluster.Info.CephVersion); err != nil {
		logger.Errorf("failed to update cluster in namespace %s. %+v", cluster.Namespace, err)
		return false, nil
	}

	if err := c.updateClusterStatus(cluster.Namespace, crdName, cephv1.ClusterStateCreated, ""); err != nil {
		logger.Errorf("failed to update cluster status in namespace %s: %+v", cluster.Namespace, err)
		return false, nil
	}

	logger.Infof("succeeded updating cluster in namespace %s", cluster.Namespace)
	return true, nil
}

// ************************************************************************************************
// Delete event functions
// ************************************************************************************************
func (c *ClusterController) onDelete(obj interface{}) {
	clust, migrationNeeded, err := getClusterObject(obj)
	if err != nil {
		logger.Errorf("failed to get cluster object: %+v", err)
		return
	}

	if migrationNeeded {
		// ignore deletion of a legacy cluster as it should have been migrated to an object of the current type
		// and tracked now with that object.
		logger.Infof("ignoring deletion of legacy cluster %s in namespace %s", clust.Name, clust.Namespace)
		return
	}

	if existing, ok := c.clusterMap[clust.Namespace]; ok && existing.crdName != clust.Name {
		logger.Errorf("Skipping deletion of cluster crd %s in namespace %s. Cluster crd %s already exists in this namespace. Only one cluster crd per namespace is supported.",
			clust.Name, clust.Namespace, existing.crdName)
		return
	}

	logger.Infof("delete event for cluster %s in namespace %s", clust.Name, clust.Namespace)

	err = c.handleDelete(clust, time.Duration(clusterDeleteRetryInterval)*time.Second)
	if err != nil {
		logger.Errorf("failed to delete cluster. %+v", err)
	}
	if cluster, ok := c.clusterMap[clust.Namespace]; ok {
		close(cluster.stopCh)
		delete(c.clusterMap, clust.Namespace)
	}
	if clust.Spec.Storage.AnyUseAllDevices() {
		c.devicesInUse = false
	}
}

func (c *ClusterController) handleDelete(cluster *cephv1.CephCluster, retryInterval time.Duration) error {

	operatorNamespace := os.Getenv(k8sutil.PodNamespaceEnvVar)
	retryCount := 0
	for {
		// TODO: filter this List operation by cluster namespace on the server side
		vols, err := c.volumeAttachment.List(operatorNamespace)
		if err != nil {
			return fmt.Errorf("failed to get volume attachments for operator namespace %s: %+v", operatorNamespace, err)
		}

		// find volume attachments in the deleted cluster
		attachmentsExist := false
	AttachmentLoop:
		for _, vol := range vols.Items {
			for _, a := range vol.Attachments {
				if a.ClusterName == cluster.Namespace {
					// there is still an outstanding volume attachment in the cluster that is being deleted.
					attachmentsExist = true
					break AttachmentLoop
				}
			}
		}

		if !attachmentsExist {
			logger.Infof("no volume attachments for cluster %s to clean up.", cluster.Namespace)
			break
		}

		retryCount++
		if retryCount == clusterDeleteMaxRetries {
			logger.Warningf(
				"exceeded retry count while waiting for volume attachments for cluster %s to be cleaned up. vols: %+v",
				cluster.Namespace,
				vols.Items)
			break
		}

		logger.Infof("waiting for volume attachments in cluster %s to be cleaned up. Retrying in %s.",
			cluster.Namespace, retryInterval)
		<-time.After(retryInterval)
	}

	return nil
}

func isLegacyClusterObjectDeleted(obj interface{}) bool {
	// if the object is a legacy cluster type and the deletion timestamp on the legacy cluster object is set,
	// it has been requested to be deleted
	if clusterLegacy, ok := obj.(*cephbeta.Cluster); ok {
		return clusterLegacy.DeletionTimestamp != nil
	}

	// not a legacy type
	return false
}

// ************************************************************************************************
// Finalizer functions
// ************************************************************************************************
func (c *ClusterController) addFinalizer(namespace, name string) error {

	// get the latest cluster object since we probably updated it before we got to this point (e.g. by updating its status)
	clust, err := c.context.RookClientset.CephV1().CephClusters(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// add the finalizer (cephcluster.ceph.rook.io) if it is not yet defined on the cluster CRD
	for _, finalizer := range clust.Finalizers {
		if finalizer == finalizerName {
			logger.Infof("finalizer already set on cluster %s", clust.Namespace)
			return nil
		}
	}

	// adding finalizer to the cluster crd
	clust.Finalizers = append(clust.Finalizers, finalizerName)

	// update the crd
	_, err = c.context.RookClientset.CephV1().CephClusters(clust.Namespace).Update(clust)
	if err != nil {
		return fmt.Errorf("failed to add finalizer to cluster. %+v", err)
	}

	logger.Infof("added finalizer to cluster %s", clust.Name)
	return nil
}

func (c *ClusterController) removeFinalizer(obj interface{}) {
	var fname string
	var objectMeta *metav1.ObjectMeta

	// first determine what type/version of cluster we are dealing with
	if cl, ok := obj.(*cephv1.CephCluster); ok {
		fname = finalizerName
		objectMeta = &cl.ObjectMeta
	} else if cl, ok := obj.(*cephbeta.Cluster); ok {
		fname = finalizerNameRookLegacy
		objectMeta = &cl.ObjectMeta
	} else {
		logger.Warningf("cannot remove finalizer from object that is not a cluster: %+v", obj)
		return
	}

	// remove the finalizer from the slice if it exists
	found := false
	for i, finalizer := range objectMeta.Finalizers {
		if finalizer == fname {
			objectMeta.Finalizers = append(objectMeta.Finalizers[:i], objectMeta.Finalizers[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		logger.Infof("finalizer %s not found in the cluster crd '%s'", fname, objectMeta.Name)
		return
	}

	// update the crd to remove the finalizer for good. retry several times in case of intermittent failures.
	maxRetries := 5
	retrySeconds := 5 * time.Second
	for i := 0; i < maxRetries; i++ {
		var err error
		if cluster, ok := obj.(*cephv1.CephCluster); ok {
			_, err = c.context.RookClientset.CephV1().CephClusters(cluster.Namespace).Update(cluster)
		} else {
			clusterLegacy := obj.(*cephbeta.Cluster)
			_, err = c.context.RookClientset.CephV1beta1().Clusters(clusterLegacy.Namespace).Update(clusterLegacy)
		}

		if err != nil {
			logger.Errorf("failed to remove finalizer %s from cluster %s. %+v", fname, objectMeta.Name, err)
			time.Sleep(retrySeconds)
			continue
		}
		logger.Infof("removed finalizer %s from cluster %s", fname, objectMeta.Name)
		return
	}

	logger.Warningf("giving up from removing the %s cluster finalizer", fname)
}

func (c *ClusterController) updateClusterStatus(namespace, name string, state cephv1.ClusterState, message string) error {
	// get the most recent cluster CRD object
	cluster, err := c.context.RookClientset.CephV1().CephClusters(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get cluster from namespace %s prior to updating its status: %+v", namespace, err)
	}

	// update the status on the retrieved cluster object
	cluster.Status = cephv1.ClusterStatus{State: state, Message: message}
	if _, err := c.context.RookClientset.CephV1().CephClusters(cluster.Namespace).Update(cluster); err != nil {
		return fmt.Errorf("failed to update cluster %s status: %+v", cluster.Namespace, err)
	}

	return nil
}

func ClusterOwnerRef(namespace, clusterID string) metav1.OwnerReference {
	blockOwner := true
	return metav1.OwnerReference{
		APIVersion:         ClusterResource.Version,
		Kind:               ClusterResource.Kind,
		Name:               namespace,
		UID:                types.UID(clusterID),
		BlockOwnerDeletion: &blockOwner,
	}
}
