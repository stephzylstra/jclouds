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
package org.jclouds.kinetic.config;

import static org.jclouds.kinetic.util.Utils.isWindows;

import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.LocalBlobRequestSigner;
import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;
import org.jclouds.blobstore.config.LocalBlobStore;
import org.jclouds.blobstore.util.BlobUtils;
import org.jclouds.kinetic.predicates.validators.KineticBlobKeyValidator;
import org.jclouds.kinetic.predicates.validators.KineticContainerNameValidator;
import org.jclouds.kinetic.predicates.validators.internal.KineticBlobKeyValidatorImpl;
import org.jclouds.kinetic.predicates.validators.internal.KineticContainerNameValidatorImpl;
import org.jclouds.kinetic.strategy.internal.KineticStorageStrategyImpl;
import org.jclouds.kinetic.util.internal.KineticBlobUtilsImpl;

import com.google.inject.AbstractModule;

public class KineticBlobStoreContextModule extends AbstractModule {

   @Override
   protected void configure() {
      bind(BlobStore.class).to(LocalBlobStore.class);
      install(new BlobStoreObjectModule());
      if (isWindows()) {
         bind(ConsistencyModel.class).toInstance(ConsistencyModel.EVENTUAL);
      } else {
         bind(ConsistencyModel.class).toInstance(ConsistencyModel.STRICT);
      }
      bind(LocalStorageStrategy.class).to(KineticStorageStrategyImpl.class);
      bind(BlobUtils.class).to(KineticBlobUtilsImpl.class);
      bind(KineticBlobKeyValidator.class).to(KineticBlobKeyValidatorImpl.class);
      bind(KineticContainerNameValidator.class).to(KineticContainerNameValidatorImpl.class);
      bind(BlobRequestSigner.class).to(LocalBlobRequestSigner.class);
   }

}
