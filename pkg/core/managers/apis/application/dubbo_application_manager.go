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

package application

import (
	"context"
	"github.com/apache/dubbo-kubernetes/pkg/core"
	core_model "github.com/apache/dubbo-kubernetes/pkg/core/resources/model"
	kube_ctrl "sigs.k8s.io/controller-runtime"
)

import (
	config_core "github.com/apache/dubbo-kubernetes/pkg/config/core"
	core_manager "github.com/apache/dubbo-kubernetes/pkg/core/resources/manager"
	core_store "github.com/apache/dubbo-kubernetes/pkg/core/resources/store"
)

type dubboApplicationManager struct {
	core_manager.ResourceManager
	store      core_store.ResourceStore
	manager    kube_ctrl.Manager
	deployMode config_core.DeployMode
}

func NewDubboApplicationManager(store core_store.ResourceStore, manager kube_ctrl.Manager, mode config_core.DeployMode) core_manager.ResourceManager {
	return &dubboApplicationManager{
		ResourceManager: core_manager.NewResourceManager(store),
		store:           store,
		manager:         manager,
		deployMode:      mode,
	}
}

func (m *dubboApplicationManager) Create(ctx context.Context, r core_model.Resource, fs ...core_store.CreateOptionsFunc) error {
	return m.store.Create(ctx, r, append(fs, core_store.CreatedAt(core.Now()))...)
}

func (m *dubboApplicationManager) Update(ctx context.Context, r core_model.Resource, fs ...core_store.UpdateOptionsFunc) error {
	return m.ResourceManager.Update(ctx, r, fs...)
}

func (m *dubboApplicationManager) Get(ctx context.Context, r core_model.Resource, opts ...core_store.GetOptionsFunc) error {
	return m.store.Get(ctx, r, opts...)
	//dataplane, err := m.dataplane(r)
	//if err != nil {
	//	return err
	//}
	//
	//if err := m.store.Get(ctx, dataplane, opts...); err != nil {
	//	return err
	//}
	//m.setInboundsClusterTag(dataplane)
	//m.setHealth(dataplane)
	//if m.deployMode != config_core.UniversalMode {
	//	m.setExtensions(ctx, dataplane)
	//}
	//return nil
}

func (m *dubboApplicationManager) List(ctx context.Context, r core_model.ResourceList, opts ...core_store.ListOptionsFunc) error {
	return m.store.List(ctx, r, opts...)
	//dataplanes, err := m.dataplanes(r)
	//if err != nil {
	//	return err
	//}
	//if err := m.store.List(ctx, dataplanes, opts...); err != nil {
	//	return err
	//}
	//for _, item := range dataplanes.Items {
	//	m.setHealth(item)
	//	m.setInboundsClusterTag(item)
	//	if m.deployMode != config_core.UniversalMode {
	//		m.setExtensions(ctx, item)
	//	}
	//}
	//return nil
}
