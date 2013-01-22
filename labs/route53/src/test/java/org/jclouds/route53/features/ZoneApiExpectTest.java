/**
 * Licensed to jclouds, Inc. (jclouds) under one or more
 * contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  jclouds licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unles required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either expres or implied.  See the License for the
 * specific language governing permisions and limitations
 * under the License.
 */
package org.jclouds.route53.features;

import static org.jclouds.route53.options.ListZonesOptions.Builder.afterMarker;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.jclouds.http.HttpRequest;
import org.jclouds.http.HttpResponse;
import org.jclouds.rest.ResourceNotFoundException;
import org.jclouds.route53.Route53Api;
import org.jclouds.route53.internal.BaseRoute53ApiExpectTest;
import org.jclouds.route53.parse.GetHostedZoneResponseTest;
import org.jclouds.route53.parse.ListHostedZonesResponseTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

/**
 * @author Adrian Cole
 */
@Test(groups = "unit", testName = "ZoneApiExpectTest")
public class ZoneApiExpectTest extends BaseRoute53ApiExpectTest {

   HttpRequest get = HttpRequest.builder().method("GET")
         .endpoint("https://route53.amazonaws.com/2012-02-29//hostedzone/Z1XTHCPEFRWV1X")
         .addHeader("Host", "route53.amazonaws.com")
         .addHeader("Date", "Mon, 21 Jan 02013 19:29:03 -0800")
         .addHeader("X-Amzn-Authorization",
               "AWS3-HTTPS AWSAccessKeyId=identity,Algorithm=HmacSHA256,Signature=pylxNiLcrsjNRZOsxyT161JCwytVPHyc2rFfmNCuZKI=")
         .build();
   
   HttpResponse getResponse = HttpResponse.builder().statusCode(200)
         .payload(payloadFromResourceWithContentType("/hosted_zone.xml", "text/xml")).build();

   public void testGetWhenResponseIs2xx() {
      Route53Api apiWhenExist = requestSendsResponse(get, getResponse);
      assertEquals(apiWhenExist.getZoneApi().get("/hostedzone/Z1XTHCPEFRWV1X").toString(), new GetHostedZoneResponseTest().expected()
            .toString());
   }
   
   HttpResponse notFound = HttpResponse.builder().statusCode(404).build();

   public void testGetWhenResponseIs404() {
      Route53Api apiWhenDontExist = requestSendsResponse(get, notFound);
      assertNull(apiWhenDontExist.getZoneApi().get("/hostedzone/Z1XTHCPEFRWV1X"));
   }

   HttpRequest list = HttpRequest.builder().method("GET")
         .endpoint("https://route53.amazonaws.com/2012-02-29/hostedzone")
         .addHeader("Host", "route53.amazonaws.com")
         .addHeader("Date", "Mon, 21 Jan 02013 19:29:03 -0800")
         .addHeader("X-Amzn-Authorization",
               "AWS3-HTTPS AWSAccessKeyId=identity,Algorithm=HmacSHA256,Signature=pylxNiLcrsjNRZOsxyT161JCwytVPHyc2rFfmNCuZKI=")
         .build();

   HttpResponse listResponse = HttpResponse.builder().statusCode(200)
         .payload(payloadFromResourceWithContentType("/hosted_zones.xml", "text/xml")).build();
   
   public void testListWhenResponseIs2xx() {
      Route53Api apiWhenExist = requestSendsResponse(list, listResponse);
      assertEquals(apiWhenExist.getZoneApi().list().get(0).toString(), new ListHostedZonesResponseTest().expected()
            .toString());
   }

   // TODO: this should really be an empty set
   @Test(expectedExceptions = ResourceNotFoundException.class)
   public void testListWhenResponseIs404() {
      Route53Api apiWhenDontExist = requestSendsResponse(list, notFound);
      assertEquals(apiWhenDontExist.getZoneApi().list().get(0).toSet(), ImmutableSet.of());
   }
   
   HttpRequest listWithOptions = HttpRequest.builder().method("GET")
         .endpoint("https://route53.amazonaws.com/2012-02-29/hostedzone?marker=Z333333YYYYYYY")
         .addHeader("Host", "route53.amazonaws.com")
         .addHeader("Date", "Mon, 21 Jan 02013 19:29:03 -0800")
         .addHeader("X-Amzn-Authorization",
               "AWS3-HTTPS AWSAccessKeyId=identity,Algorithm=HmacSHA256,Signature=pylxNiLcrsjNRZOsxyT161JCwytVPHyc2rFfmNCuZKI=")
         .build();
   
   public void testListWithOptionsWhenResponseIs2xx() {
      Route53Api apiWhenWithOptionsExist = requestSendsResponse(listWithOptions, listResponse);
      assertEquals(apiWhenWithOptionsExist.getZoneApi().list(afterMarker("Z333333YYYYYYY")).toString(),
            new ListHostedZonesResponseTest().expected().toString());
   }

   
   public void testList2PagesWhenResponseIs2xx() {
      HttpResponse noMore = HttpResponse.builder().statusCode(200)
            .payload(payloadFromStringWithContentType("<ListHostedZonesResponse />", "text/xml")).build();

      Route53Api apiWhenExist = requestsSendResponses(list, listResponse, listWithOptions, noMore);
      assertEquals(apiWhenExist.getZoneApi().list().concat().toString(), new ListHostedZonesResponseTest().expected()
            .toString());
   }
}
