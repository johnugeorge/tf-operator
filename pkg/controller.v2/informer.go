package controller

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	restclientset "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	tfv1alpha2 "github.com/kubeflow/tf-operator/pkg/apis/tensorflow/v1alpha2"
	"github.com/kubeflow/tf-operator/pkg/apis/tensorflow/validation"
	tfjobinformers "github.com/kubeflow/tf-operator/pkg/client/informers/externalversions"
	tfjobinformersv1alpha2 "github.com/kubeflow/tf-operator/pkg/client/informers/externalversions/kubeflow/v1alpha2"
	"github.com/kubeflow/tf-operator/pkg/util/unstructured"
)

const (
	resyncPeriod = 30 * time.Second
)

var (
	ErrGetFromKey    = fmt.Errorf("Failed to get TFJob from key")
	ErrNotExists     = fmt.Errorf("The object is not found")
	ErrFailedMarshal = fmt.Errorf("Failed to marshal the object to TFJob")
)

func NewUnstructuredTFJobInformer(restConfig *restclientset.Config) tfjobinformersv1alpha2.TFJobInformer {
	dynClientPool := dynamic.NewDynamicClientPool(restConfig)
	dclient, err := dynClientPool.ClientForGroupVersionKind(controllerKind)
	if err != nil {
		panic(err)
	}
	resource := &metav1.APIResource{
		Name:         tfv1alpha2.Plural,
		SingularName: tfv1alpha2.Singular,
		Namespaced:   true,
		Group:        tfv1alpha2.GroupName,
		Version:      tfv1alpha2.GroupVersion,
	}
	informer := unstructured.NewTFJobInformer(
		resource,
		dclient,
		metav1.NamespaceAll,
		resyncPeriod,
		cache.Indexers{},
	)
	return informer
}

// NewTFJobInformer returns TFJobInformer from the given factory.
func (tc *TFJobController) NewTFJobInformer(tfJobInformerFactory tfjobinformers.SharedInformerFactory) tfjobinformersv1alpha2.TFJobInformer {
	return tfJobInformerFactory.Kubeflow().V1alpha2().TFJobs()
}

func (tc *TFJobController) GetTFJobFromName(namespace, name string) (*tfv1alpha2.TFJob, error) {
	key := fmt.Sprintf("%s/%s", namespace, name)
	return tc.GetTFJobFromKey(key)
}

func (tc *TFJobController) GetTFJobFromKey(key string) (*tfv1alpha2.TFJob, error) {
	// Check if the key exists.
	obj, exists, err := tc.TfJobInformer.GetIndexer().GetByKey(key)
	if err != nil {
		log.Errorf("Failed to get TFJob '%s' from informer index: %+v", key, err)
		return nil, ErrGetFromKey
	}
	if !exists {
		// This happens after a tfjob was deleted, but the work queue still had an entry for it.
		return nil, ErrNotExists
	}

	tfjob, err := tfJobFromUnstructured(obj)
	if err != nil {
		return nil, err
	}
	return tfjob, nil
}

func tfJobFromUnstructured(obj interface{}) (*tfv1alpha2.TFJob, error) {
	// Check if the spec is valid.
	un, ok := obj.(*metav1unstructured.Unstructured)
	if !ok {
		log.Warn("The object in index is not an unstructured")
		return nil, ErrGetFromKey
	}
	var tfjob tfv1alpha2.TFJob
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(un.Object, &tfjob)
	// This is a simple validation for TFJob to close
	// https://github.com/kubeflow/tf-operator/issues/641
	// TODO(gaocegege): Add more validation here.
	if err != nil || validation.ValidateAlphaTwoTFJobSpec(&tfjob.Spec) != nil {
		return &tfjob, ErrFailedMarshal
	}
	return &tfjob, nil
}

func unstructuredFromTFJob(obj interface{}, tfJob *tfv1alpha2.TFJob) error {
	un, ok := obj.(*metav1unstructured.Unstructured)
	if !ok {
		log.Warn("The objetc in index is not an unstructured")
		return ErrGetFromKey
	}

	var err error
	un.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(tfJob)
	if err != nil {
		log.Error("The TFJob connvert failed")
		return err
	}
	return nil

}
