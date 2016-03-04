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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.proto.Common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** Concrete implementation of {@link Meta.ConnectionProperties}. Provides additional state
 * tracking to enable {@code RemoteMeta} to lazily push changes up to a query server.
 *
 * <p>{@code Meta} instances should probably hold authority on the {@code isDirty}
 * flag because {@code AvaticaConnection} instances have no way of knowing if they're local or
 * remote.
 */
public class ConnectionPropertiesImpl implements Meta.ConnectionProperties {
  private static final FieldDescriptor CATALOG_DESCRIPTOR = Common.ConnectionProperties
      .getDescriptor().findFieldByNumber(Common.ConnectionProperties.CATALOG_FIELD_NUMBER);
  private static final FieldDescriptor SCHEMA_DESCRIPTOR = Common.ConnectionProperties
      .getDescriptor().findFieldByNumber(Common.ConnectionProperties.SCHEMA_FIELD_NUMBER);
  private static final FieldDescriptor TRANSACTION_ISOLATION_DESCRIPTOR = Common
      .ConnectionProperties.getDescriptor().findFieldByNumber(
          Common.ConnectionProperties.TRANSACTION_ISOLATION_FIELD_NUMBER);

  private boolean isDirty = false;
  private Boolean autoCommit;
  private Boolean readOnly;
  private Integer transactionIsolation;
  private String catalog;
  private String schema;

  // TODO: replace with Meta.ConnectionProperties$EMPTY instance?
  public ConnectionPropertiesImpl() {}

  public ConnectionPropertiesImpl(Connection conn) throws SQLException {
    this(conn.getAutoCommit(), conn.isReadOnly(), conn.getTransactionIsolation(),
        conn.getCatalog(), conn.getSchema());
  }

  @JsonCreator
  public ConnectionPropertiesImpl(
      @JsonProperty("autoCommit") Boolean autoCommit,
      @JsonProperty("readOnly") Boolean readOnly,
      @JsonProperty("transactionIsolation") Integer transactionIsolation,
      @JsonProperty("catalog") String catalog,
      @JsonProperty("schema") String schema) {
    this.autoCommit = autoCommit;
    this.readOnly = readOnly;
    this.transactionIsolation = transactionIsolation;
    this.catalog = catalog;
    this.schema = schema;
  }

  public ConnectionPropertiesImpl setDirty(boolean val) {
    this.isDirty = val;
    return this;
  }

  public boolean isDirty() {
    return this.isDirty;
  }

  @Override public boolean isEmpty() {
    return autoCommit == null && readOnly == null && transactionIsolation == null
        && catalog == null && schema == null;
  }

  /** Overwrites fields in {@code this} with any non-null fields in {@code that}. Sets
   * {@code isDirty} if any fields are changed.
   *
   * @return {@code this}
   */
  @Override public ConnectionPropertiesImpl merge(Meta.ConnectionProperties that) {
    if (this == that) {
      return this;
    }
    if (that.isAutoCommit() != null && this.autoCommit != that.isAutoCommit()) {
      this.autoCommit = that.isAutoCommit();
      this.isDirty = true;
    }
    if (that.isReadOnly() != null && this.readOnly != that.isReadOnly()) {
      this.readOnly = that.isReadOnly();
      this.isDirty = true;
    }
    if (that.getTransactionIsolation() != null
        && !that.getTransactionIsolation().equals(this.transactionIsolation)) {
      this.transactionIsolation = that.getTransactionIsolation();
      this.isDirty = true;
    }
    if (that.getCatalog() != null && !that.getCatalog().equalsIgnoreCase(this.catalog)) {
      this.catalog = that.getCatalog();
      this.isDirty = true;
    }
    if (that.getSchema() != null && !that.getSchema().equalsIgnoreCase(this.schema)) {
      this.schema = that.getSchema();
      this.isDirty = true;
    }
    return this;
  }

  /** Sets {@code autoCommit} status and flag as dirty.
   *
   * @return {@code this}
   */
  @Override public Meta.ConnectionProperties setAutoCommit(boolean val) {
    this.autoCommit = val;
    this.isDirty = true;
    return this;
  }

  @Override public Boolean isAutoCommit() {
    return this.autoCommit;
  }

  /** Sets {@code readOnly} status and flag as dirty.
   *
   * @return {@code this}
   */
  @Override public Meta.ConnectionProperties setReadOnly(boolean val) {
    this.readOnly = val;
    this.isDirty = true;
    return this;
  }

  @Override public Boolean isReadOnly() {
    return this.readOnly;
  }

  /** Sets {@code transactionIsolation} status and flag as dirty.
   *
   * @return {@code this}
   */
  @Override public Meta.ConnectionProperties setTransactionIsolation(int val) {
    this.transactionIsolation = val;
    this.isDirty = true;
    return this;
  }

  public Integer getTransactionIsolation() {
    return this.transactionIsolation;
  }

  /** Sets {@code catalog} and flag as dirty.
   *
   * @return {@code this}
   */
  @Override public Meta.ConnectionProperties setCatalog(String val) {
    this.catalog = val;
    this.isDirty = true;
    return this;
  }

  @Override public String getCatalog() {
    return this.catalog;
  }

  /** Sets {@code schema} and flag as dirty.
   *
   * @return {@code this}
   */
  @Override public Meta.ConnectionProperties setSchema(String val) {
    this.schema = val;
    this.isDirty = true;
    return this;
  }

  public String getSchema() {
    return this.schema;
  }

  @Override public int hashCode() {
    return Objects.hash(autoCommit, catalog, isDirty, readOnly, schema,
        transactionIsolation);
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof ConnectionPropertiesImpl
        && Objects.equals(autoCommit, ((ConnectionPropertiesImpl) o).autoCommit)
        && Objects.equals(catalog, ((ConnectionPropertiesImpl) o).catalog)
        && isDirty == ((ConnectionPropertiesImpl) o).isDirty
        && Objects.equals(readOnly, ((ConnectionPropertiesImpl) o).readOnly)
        && Objects.equals(schema, ((ConnectionPropertiesImpl) o).schema)
        && Objects.equals(transactionIsolation,
            ((ConnectionPropertiesImpl) o).transactionIsolation);
  }

  public Common.ConnectionProperties toProto() {
    Common.ConnectionProperties.Builder builder = Common.ConnectionProperties.newBuilder();

    if (null != autoCommit) {
      builder.setHasAutoCommit(true);
      builder.setAutoCommit(autoCommit.booleanValue());
    } else {
      // Be explicit to avoid default value confusion
      builder.setHasAutoCommit(false);
    }

    if (null != catalog) {
      builder.setCatalog(catalog);
    }

    builder.setIsDirty(isDirty);

    if (null != readOnly) {
      builder.setHasReadOnly(true);
      builder.setReadOnly(readOnly.booleanValue());
    } else {
      // Be explicit to avoid default value confusion
      builder.setHasReadOnly(false);
    }

    if (null != schema) {
      builder.setSchema(schema);
    }

    if (null != transactionIsolation) {
      builder.setTransactionIsolation(transactionIsolation.intValue());
    }

    return builder.build();
  }

  public static ConnectionPropertiesImpl fromProto(Common.ConnectionProperties proto) {
    String catalog = null;
    if (proto.hasField(CATALOG_DESCRIPTOR)) {
      catalog = proto.getCatalog();
    }

    String schema = null;
    if (proto.hasField(SCHEMA_DESCRIPTOR)) {
      schema = proto.getSchema();
    }

    Boolean autoCommit = null;
    if (proto.getHasAutoCommit()) {
      autoCommit = Boolean.valueOf(proto.getAutoCommit());
    }

    Boolean readOnly = null;
    if (proto.getHasReadOnly()) {
      readOnly = Boolean.valueOf(proto.getReadOnly());
    }

    Integer transactionIsolation = null;
    if (proto.hasField(TRANSACTION_ISOLATION_DESCRIPTOR)) {
      transactionIsolation = Integer.valueOf(proto.getTransactionIsolation());
    }

    ConnectionPropertiesImpl impl = new ConnectionPropertiesImpl(autoCommit, readOnly,
        transactionIsolation, catalog, schema);

    impl.setDirty(proto.getIsDirty());

    return impl;
  }
}

// End ConnectionPropertiesImpl.java
