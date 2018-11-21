/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.access;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Guard that checks whether principal has required access to schema
 */
public class PrincipalBasedAuthorization implements Authorization {

  private final Map<String, SqlAccessType> accessMap;
  private final Set<String> owners;
  private final CalcitePrincipalFairy fairy;

  public PrincipalBasedAuthorization(
      CalcitePrincipalFairy fairy,
      Map<String, SqlAccessType> accessMap,
      Set<String> owners) {
    this.accessMap = accessMap;
    this.fairy = fairy;
    this.owners = ImmutableSet.copyOf(owners);
  }

  @Override public boolean accessGranted(AuthorizationRequest request) {
    CalcitePrincipal principal = fairy.get();
    return accessGranted(principal, request);
  }

  private boolean accessGranted(CalcitePrincipal principal, AuthorizationRequest request) {
    // If no principal - this authorization assumes no access granted
    if (principal == null) {
      return false;
    }
    // owners are always authorized to all requests
    if (owners.contains(principal.getName())) {
      return true;
    }
    // otherwise check in user to access type map
    if (accessMap.getOrDefault(principal.getName(), SqlAccessType.NONE)
        .allowsAccess(request.getRequiredAccess())) {
      return true;
    }
    // otherwise check if view and check authorization for owner of view schema
    if (request.getObjectPath() != null && !request.getObjectPath().isEmpty()) {
      List<String> path = request.getObjectPath().subList(0, request.getObjectPath().size() - 1);
      CalciteSchema schema = SqlValidatorUtil.getSchema(
          request.getCatalogReader().getRootSchema(),
          path,
          request.getCatalogReader().nameMatcher());
      PrincipalBasedAuthorization authorization
          = (PrincipalBasedAuthorization) schema.getAuthorization();
      for (String ownerName : authorization.getOwners()) {
        if (accessGranted(mockedPrincipal(ownerName), request)) {
          return true;
        }
      }
    }
    return false;
  }

  public Set<String> getOwners() {
    return owners;
  }

  private CalcitePrincipal mockedPrincipal(String ownerName) {
    return CalcitePrincipalImpl.fromName(ownerName);
  }

}

// End PrincipalBasedAuthorization.java
