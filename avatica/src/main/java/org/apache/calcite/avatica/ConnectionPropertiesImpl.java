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
import org.apache.calcite.avatica.remote.ProtobufService;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Descriptors.Descriptor;

import java.sql.Connection;
import java.sql.SQLException;

/** Concrete implementation of {@link Meta.ConnectionProperties}. Provides additional state
 * tracking to enable {@code RemoteMeta} to lazily push changes up to a query server.
 *
 * <p>{@code Meta} instances should probably hold authority on the {@code isDirty}
 * flag because {@code AvaticaConnection} instances have no way of knowing if they're local or
 * remote.
 */
public class ConnectionPropertiesImpl implements Meta.ConnectionProperties {
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
    final int prime = 31;
    int result = 1;
    result = prime * result + ((autoCommit == null) ? 0 : autoCommit.hashCode());
    result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
    result = prime * result + (isDirty ? 1231 : 1237);
    result = prime * result + ((readOnly == null) ? 0 : readOnly.hashCode());
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    result = prime * result
        + ((transactionIsolation == null) ? 0 : transactionIsolation.hashCode());
    return result;
  }

  @Override public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ConnectionPropertiesImpl) {
      ConnectionPropertiesImpl other = (ConnectionPropertiesImpl) o;

      if (null == autoCommit) {
        if (null != other.autoCommit) {
          return false;
        }
      } else if (!autoCommit.equals(other.autoCommit)) {
        return false;
      }

      if (null == catalog) {
        if (null != other.catalog) {
          return false;
        }
      } else if (!catalog.equals(other.catalog)) {
        return false;
      }

      if (isDirty != other.isDirty) {
        return false;
      }

      if (null == readOnly) {
        if (null != other.readOnly) {
          return false;
        }
      } else if (!readOnly.equals(other.readOnly)) {
        return false;
      }

      if (null == schema) {
        if (null != other.schema) {
          return false;
        }
      } else if (!schema.equals(other.schema)) {
        return false;
      }

      if (null == transactionIsolation) {
        if (null != other.transactionIsolation) {
          return false;
        }
      } else if (!transactionIsolation.equals(other.transactionIsolation)) {
        return false;
      }

      return true;
    }

    return false;
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
    final Descriptor desc = proto.getDescriptorForType();

    String catalog = null;
    if (ProtobufService.hasField(proto, desc, Common.ConnectionProperties.CATALOG_FIELD_NUMBER)) {
      catalog = proto.getCatalog();
    }

    String schema = null;
    if (ProtobufService.hasField(proto, desc, Common.ConnectionProperties.SCHEMA_FIELD_NUMBER)) {
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
    if (ProtobufService.hasField(proto, desc,
        Common.ConnectionProperties.TRANSACTION_ISOLATION_FIELD_NUMBER)) {
      transactionIsolation = Integer.valueOf(proto.getTransactionIsolation());
    }

    ConnectionPropertiesImpl impl = new ConnectionPropertiesImpl(autoCommit, readOnly,
        transactionIsolation, catalog, schema);

    impl.setDirty(proto.getIsDirty());

    return impl;
  }
}

// End ConnectionPropertiesImpl.java
