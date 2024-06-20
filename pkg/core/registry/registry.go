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
	"github.com/apache/dubbo-kubernetes/pkg/core/logger"
	"github.com/apache/dubbo-kubernetes/pkg/core/reg_client"
	"github.com/apache/dubbo-kubernetes/pkg/core/resources/apis/mesh"
	"github.com/apache/dubbo-kubernetes/pkg/core/resources/store"
	gxset "github.com/dubbogo/gost/container/set"
	"net/url"
	"sync"
	"time"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/metadata/report"
	dubboRegistry "dubbo.apache.org/dubbo-go/v3/registry"
)

import (
	"github.com/apache/dubbo-kubernetes/pkg/core/consts"
	core_manager "github.com/apache/dubbo-kubernetes/pkg/core/resources/manager"
	"github.com/apache/dubbo-kubernetes/pkg/events"
)

type Registry struct {
	delegate   dubboRegistry.Registry
	sdDelegate dubboRegistry.ServiceDiscovery
	stop       chan struct{}
}

func NewRegistry(delegate dubboRegistry.Registry, sdDelegate dubboRegistry.ServiceDiscovery) *Registry {
	return &Registry{
		delegate:   delegate,
		sdDelegate: sdDelegate,
		stop:       make(chan struct{}),
	}
}

func (r *Registry) Destroy() error {
	close(r.stop)
	return nil
}

func (r *Registry) Delegate() dubboRegistry.Registry {
	return r.delegate
}

func (r *Registry) Subscribe(
	metadataReport report.MetadataReport,
	resourceManager core_manager.ResourceManager,
	cache *sync.Map,
	discovery dubboRegistry.ServiceDiscovery,
	regClient reg_client.RegClient,
	out events.Emitter,
	systemNamespace string,
) error {
	queryParams := url.Values{
		consts.InterfaceKey:  {consts.AnyValue},
		consts.GroupKey:      {consts.AnyValue},
		consts.VersionKey:    {consts.AnyValue},
		consts.ClassifierKey: {consts.AnyValue},
		consts.CategoryKey: {consts.ProvidersCategory +
			"," + consts.ConsumersCategory +
			"," + consts.RoutersCategory +
			"," + consts.ConfiguratorsCategory},
		consts.EnabledKey: {consts.AnyValue},
		consts.CheckKey:   {"false"},
	}
	subscribeUrl, _ := common.NewURL(common.GetLocalIp()+":0",
		common.WithProtocol(consts.AdminProtocol),
		common.WithParams(queryParams))
	listener := NewNotifyListener(resourceManager, cache, discovery, out)

	go func() {
		err := r.delegate.Subscribe(subscribeUrl, listener)
		if err != nil {
			logger.Error("Failed to subscribe to registry, might not be able to show services of the cluster!")
		}
	}()

	scheduler := &Scheduler{
		NewTicker: func() *time.Ticker {
			return time.NewTicker(5 * time.Minute)
		},
		OnTick: func(ctx context.Context) error {
			r.doSubscribe(resourceManager, r.sdDelegate, listener)
			return nil
		},
		OnError: func(err error) {
			logger.Error(err, "OnTick() failed")
		},
		OnStop: func() {
			//
		},
	}

	go scheduler.Schedule(r.stop)

	return nil
}

func (r *Registry) doSubscribe(resourceManager core_manager.ResourceManager, sdDelegate dubboRegistry.ServiceDiscovery, listener *NotifyListener) {
	applicationList := &mesh.DubboApplicationResourceList{}
	_ = resourceManager.List(context.Background(), applicationList)
	apps := sdDelegate.GetServices()

	newApps := make([]string, 0)
	for _, a := range apps.Values() {
		application := mesh.NewDubboApplicationResource()
		newApp, _ := a.(string)
		err := resourceManager.Get(context.Background(), application, store.GetByApplication(newApp))
		if err != nil {
			if !store.IsResourceNotFound(err) {
				logger.Errorf("Error searching for existing app %s from resource manager.", newApp)
			}
			newApps = append(newApps, newApp)
		}
	}
	// 生成应用列表
	for _, app := range newApps {
		instances := sdDelegate.GetInstances(app)
		delSDListener := NewDubboSDNotifyListener(gxset.NewSet(app))
		logger.Infof("Synchronized instance notification on subscription, instance list size %s", len(instances))
		if len(instances) > 0 {
			err := delSDListener.OnEvent(&dubboRegistry.ServiceInstancesChangedEvent{
				ServiceName: app,
				Instances:   instances,
			})
			if err != nil {
				logger.Warnf("[ServiceDiscoveryRegistry] ServiceInstancesChangedListenerImpl handle error:%v", err)
			}
		}
		delSDListener.AddListenerAndNotify("unifiedKey", listener)
		err := sdDelegate.AddListener(delSDListener)
		if err != nil {
			logger.Warnf("Failed to Add Listener")
		}
	}

	// todo, remove listeners of apps not exist in registry
	//for _, a := range appsToRemove {
	//	sdDelegate.RemoveListener(delSDListener)
	//}
}
