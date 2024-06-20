/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package registry

import (
	"context"
	"github.com/apache/dubbo-kubernetes/pkg/core/resources/store"
	"strconv"
	"sync"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	dubboRegistry "dubbo.apache.org/dubbo-go/v3/registry"
	"dubbo.apache.org/dubbo-go/v3/remoting"
)

import (
	mesh_proto "github.com/apache/dubbo-kubernetes/api/mesh/v1alpha1"
	"github.com/apache/dubbo-kubernetes/pkg/core/resources/apis/mesh"
	"github.com/apache/dubbo-kubernetes/pkg/core/resources/manager"
	core_model "github.com/apache/dubbo-kubernetes/pkg/core/resources/model"
	"github.com/apache/dubbo-kubernetes/pkg/events"
)

const (
	keySeparator = "-"
)

type NotifyListener struct {
	manager.ResourceManager
	dataplaneCache *sync.Map
	discovery      dubboRegistry.ServiceDiscovery
	eventWriter    events.Emitter
}

func NewNotifyListener(
	manager manager.ResourceManager,
	cache *sync.Map,
	discovery dubboRegistry.ServiceDiscovery,
	writer events.Emitter,
) *NotifyListener {
	return &NotifyListener{
		manager,
		cache,
		discovery,
		writer,
	}
}

func (l *NotifyListener) Notify(event *dubboRegistry.ServiceEvent) {
	switch event.Action {
	case remoting.EventTypeAdd, remoting.EventTypeUpdate:
		//if event.Updated() {
		if err := l.createOrUpdateDataplane(context.Background(), event.Service); err != nil {
			return
		}
		if err := l.createOrUpdateApplication(context.Background(), event.Service); err != nil {
			return
		}
		//} else {
		//	if err := l.createOrUpdateDataplaneFromSd(context.Background(), event.Service); err != nil {
		//		return
		//	}
		//	if err := l.createOrUpdateApplicationFromSd(context.Background(), event.Service); err != nil {
		//		return
		//	}
		//}
	case remoting.EventTypeDel:
		if err := l.deleteDataplane(context.Background(), event.Service); err != nil {
			return
		}
		//if err := l.deleteApplication(context.Background(), event.Service); err != nil {
		//	return
		//}
	}
}

func (l *NotifyListener) NotifyAll(events []*dubboRegistry.ServiceEvent, f func()) {
	for _, event := range events {
		l.Notify(event)
	}
}

func (l *NotifyListener) deleteDataplane(ctx context.Context, url *common.URL) error {
	app := url.GetParam(constant.ApplicationKey, "")
	address := url.Address()
	var revision string
	instances := l.discovery.GetInstances(app)
	for _, instance := range instances {
		if instance.GetAddress() == address {
			revision = instance.GetMetadata()[constant.ExportedServicesRevisionPropertyName]
		}
	}
	key := getDataplaneKey(app, revision)

	l.dataplaneCache.Delete(key)
	if l.eventWriter != nil {
		go func() {
			l.eventWriter.Send(events.ResourceChangedEvent{
				Operation: events.Delete,
				Type:      mesh.DataplaneType,
				Key: core_model.ResourceKey{
					Name: key,
				},
			})
		}()
	}
	return nil
}

//
//
//func (l *NotifyListener) createOrUpdateDataplaneFromSd(ctx context.Context, url *common.URL) error {
//	app := url.GetParam(constant.ApplicationKey, "")
//	address := url.Address()
//	metadata, _ := url.GetAttribute("metadata")
//	rawMetadata, _ := url.GetAttribute("rawMetadata")
//
//	dataplaneResource := mesh.NewDataplaneResource()
//	dataplaneResource.SetMeta(&resourceMetaObject{
//		Name: app,
//		Mesh: core_model.DefaultMesh,
//	})
//	dataplaneResource.Spec.Networking = &mesh_proto.Dataplane_Networking{}
//	dataplaneResource.Spec.Extensions = map[string]string{}
//	dataplaneResource.Spec.Extensions[mesh_proto.ApplicationName] = app
//	dataplaneResource.Spec.Extensions[mesh_proto.Revision] = metadata.GetRevision()
//	url.GetParams()
//	dataplaneResource.SPec.Extensions
//	dataplaneResource.Spec.Networking.Address = url.Address()
//	ifaces, err := InboundInterfacesFor(ctx, url)
//	if err != nil {
//		return err
//	}
//	ofaces, err := OutboundInterfacesFor(ctx, url)
//	if err != nil {
//		return err
//	}
//	dataplaneResource.Spec.Networking.Inbound = ifaces
//	dataplaneResource.Spec.Networking.Outbound = ofaces
//	l.dataplaneCache.Store(key, dataplaneResource)
//
//	appResource := mesh.NewDubboApplicationResource()
//	appResource.SetMeta(&resourceMetaObject{
//		Name: app,
//		Mesh: core_model.DefaultMesh,
//	})
//	appResource.Spec.Name = app
//	appResource.Spec.Services =
//
//	if l.eventWriter != nil {
//		go func() {
//			l.eventWriter.Send(events.ResourceChangedEvent{
//				Operation: events.Update,
//				Type:      mesh.DataplaneType,
//				Key:       core_model.MetaToResourceKey(dataplaneResource.GetMeta()),
//			})
//		}()
//	}
//	return nil
//}

// 10.0.0.1/service1?key=value
// 10.0.0.1/service2?key=value
// 10.0.0.1/service3?key=value

// app1-10.0.0.1   dataplane, revision=1
// service1/service2/service3 metadata

// 10.0.0.2/service1?key=value
// 10.0.0.2/service2?key=value
// 10.0.0.2/service3?key=value

// app1-10.0.0.2  dataplane, revision=1
// service1/service2/service3 metadata

func (l *NotifyListener) createOrUpdateDataplane(ctx context.Context, url *common.URL) error {
	app := url.GetParam(constant.ApplicationKey, "")
	address := url.Address()

	dataplaneResource := mesh.NewDataplaneResource()
	dataplaneResource.SetMeta(&resourceMetaObject{
		Name: app + "-" + revision,
		Mesh: core_model.DefaultMesh,
	})
	dataplaneResource.Spec.Networking = &mesh_proto.Dataplane_Networking{}
	dataplaneResource.Spec.Extensions = map[string]string{}
	dataplaneResource.Spec.Extensions[mesh_proto.ApplicationName] = app
	dataplaneResource.Spec.Extensions[mesh_proto.Revision] = revision
	dataplaneResource.Spec.Networking.Address = url.Address()
	ifaces, err := InboundInterfacesFor(ctx, url)
	if err != nil {
		return err
	}
	ofaces, err := OutboundInterfacesFor(ctx, url)
	if err != nil {
		return err
	}
	dataplaneResource.Spec.Networking.Inbound = ifaces
	dataplaneResource.Spec.Networking.Outbound = ofaces
	l.dataplaneCache.Store(key, dataplaneResource)

	if l.eventWriter != nil {
		go func() {
			l.eventWriter.Send(events.ResourceChangedEvent{
				Operation: events.Update,
				Type:      mesh.DataplaneType,
				Key:       core_model.MetaToResourceKey(dataplaneResource.GetMeta()),
			})
		}()
	}
	return nil
}

func (l *NotifyListener) createOrUpdateApplication(ctx context.Context, url *common.URL) error {
	app := url.GetParam(constant.ApplicationKey, "")
	address := url.Address()
	service := url.ServiceKey()
	port := url.Port
	protocol := url.Protocol
	extProtocols := url.GetParam("ext-protocol", "")

	create := false
	appResource := mesh.NewDubboApplicationResource()
	if err := l.ResourceManager.Get(ctx, appResource, store.GetByApplication(app)); err != nil {
		if store.IsResourceNotFound(err) {
			create = true
		}
	}

	appResource.SetMeta(&resourceMetaObject{
		Name: app,
		Mesh: core_model.DefaultMesh,
	})
	appResource.Spec.Name = app
	appResource.Spec.Services[service] = service
	appResource.Spec.Instances[address] = address
	appResource.Spec.Ports[port] = port
	appResource.Spec.Protocols[protocol] = protocol
	appResource.Spec.Protocols[extProtocols] = extProtocols
	mode := url.GetParam("register-mode", "")
	if mode == "" {
		appResource.Spec.Mode[mode] = mode
	}

	if create {
		l.ResourceManager.Create()
	} else {
		l.ResourceManager.Update()
	}

	//appResource.Spec.Versions
	//appResource.Spec.Registries
	//appResource.Spec.Deployments

	//if l.eventWriter != nil {
	//	go func() {
	//		l.eventWriter.Send(events.ResourceChangedEvent{
	//			Operation: events.Update,
	//			Type:      mesh.DataplaneType,
	//			Key:       core_model.MetaToResourceKey(dataplaneResource.GetMeta()),
	//		})
	//	}()
	//}
	return nil
}

func (l *NotifyListener) createOrUpdateMetadataFromSd(ctx context.Context, url *common.URL) error {
	metadataResource := mesh.NewMetaDataResource()
	metadataResource.Spec.App = appName
	metadataResource.Spec.Revision = revision
	metadataResource.Spec.Address = address
	metadataResource.CopyFrom(metadata)
	l.ResourceManager.Create(metadataResource)
}

func (l *NotifyListener) createOrUpdateApplicationFromSd(ctx context.Context, url *common.URL) error {
	appName, address, revision;

	create := false
	appResource := mesh.NewDubboApplicationResource()
	if err := l.ResourceManager.Get(ctx, appResource, store.GetByApplication(app)); err != nil {
		if store.IsResourceNotFound(err) {
			create = true
		}
	}

	appResource.SetMeta(&resourceMetaObject{
		Name: app,
		Mesh: core_model.DefaultMesh,
	})

	range metadata.services {
		appResource.Spec.Name = app
		appResource.Spec.Services[service] = service
		appResource.Spec.Instances[address] = address
		appResource.Spec.Ports[port] = port
		appResource.Spec.Protocols[protocol] = protocol
		appResource.Spec.Protocols[extProtocols] = extProtocols
		mode := url.GetParam("register-mode", "")
		if mode == "" {
			appResource.Spec.Mode[mode] = mode
		}
	}


	if create {
		l.ResourceManager.Create()
	} else {
		l.ResourceManager.Update()
	}
}

func InboundInterfacesFor(ctx context.Context, url *common.URL) ([]*mesh_proto.Dataplane_Networking_Inbound, error) {
	var ifaces []*mesh_proto.Dataplane_Networking_Inbound
	num, err := strconv.ParseUint(url.Port, 10, 32)
	if err != nil {
		return nil, err
	}
	ifaces = append(ifaces, &mesh_proto.Dataplane_Networking_Inbound{
		Port: uint32(num),
	})
	return ifaces, nil
}

func OutboundInterfacesFor(ctx context.Context, url *common.URL) ([]*mesh_proto.Dataplane_Networking_Outbound, error) {
	var outbounds []*mesh_proto.Dataplane_Networking_Outbound

	return outbounds, nil
}

func getDataplaneKey(app string, revision string) string {
	return app + keySeparator + revision
}
