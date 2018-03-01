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

import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Tests against mocked principal
 */
public class PrincipalBasedAuthorizationTest {

  private final CalcitePrincipalFairy fairyMock = mock(CalcitePrincipalFairy.class);
  private final HashMap<String, SqlAccessType> accessMapMock = new HashMap<String, SqlAccessType>();
  private final PrincipalBasedAuthorization tested =
      new PrincipalBasedAuthorization(fairyMock, accessMapMock, Collections.<String>emptySet());
  private final CalcitePrincipal principalMock = mock(CalcitePrincipal.class);

  @Test public void testShouldNotGrantAccessWhenNoPrincipal() {
    // given
    accessMapMock.clear();
    accessMapMock.put("someuser", SqlAccessType.ALL);
    given(fairyMock.get()).willReturn(null);
    // when
    boolean result = tested.accessGranted(toRequest(SqlAccessEnum.UPDATE));
    // then
    Assert.assertFalse("Access should not be granted when no principal", result);
  }

  @Test public void testShouldNotGrantAccessWhenNoPrincipalAndEmptyMap() {
    // given
    accessMapMock.clear();
    given(fairyMock.get()).willReturn(null);
    // when
    boolean result = tested.accessGranted(toRequest(SqlAccessEnum.UPDATE));
    // then
    Assert.assertFalse("Access should not be granted when no principal and empty map", result);
  }

  @Test public void testShouldNotGrantAccessWhenNoPrincipalNotMapped() {
    // given
    accessMapMock.clear();
    accessMapMock.put("someuser", SqlAccessType.ALL);
    given(fairyMock.get()).willReturn(principalMock);
    given(principalMock.getName()).willReturn("someOTHERuser");
    // when
    boolean result = tested.accessGranted(toRequest(SqlAccessEnum.UPDATE));
    // then
    Assert.assertFalse("Access should not be granted when principal not mapped", result);
  }

  @Test public void testShouldNotGrantAccessWhenNoPrincipalMappedWithOtherAccessTypes() {
    // given
    accessMapMock.clear();
    accessMapMock.put("someuser", SqlAccessType.WRITE_ONLY);
    given(fairyMock.get()).willReturn(principalMock);
    given(principalMock.getName()).willReturn("someuser");
    // when
    boolean result = tested.accessGranted(toRequest(SqlAccessEnum.UPDATE));
    // then
    Assert.assertFalse("Access should not be granted when principal mapped to other types", result);
  }

  @Test public void testShouldGrantAccess() {
    // given
    accessMapMock.clear();
    accessMapMock.put("someuser", SqlAccessType.READ_ONLY);
    given(fairyMock.get()).willReturn(principalMock);
    given(principalMock.getName()).willReturn("someuser");
    // when
    boolean result = tested.accessGranted(toRequest(SqlAccessEnum.SELECT));
    // then
    Assert.assertTrue("Access should be granted", result);
  }

  private AuthorizationRequest toRequest(SqlAccessEnum sqlAccessEnum) {
    return new AuthorizationRequest(sqlAccessEnum, null, null, null, null);
  }

}

// End PrincipalBasedAuthorizationTest.java
