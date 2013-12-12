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
package org.jclouds.ec2.config;




import org.jclouds.ec2.EC2Api;
import org.jclouds.rest.ConfiguresHttpApi;


/**
 * Configures the EC2 connection.
 * 
 * @author Adrian Cole (EDIT: Nick Terry nterry@familysearch.org)
 */
@ConfiguresHttpApi
public class EC2HttpApiModule extends BaseEC2HttpApiModule<EC2Api> {

   public EC2HttpApiModule() {
      super(EC2Api.class);
   }
}
