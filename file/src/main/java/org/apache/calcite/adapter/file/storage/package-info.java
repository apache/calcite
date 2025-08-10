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

/**
 * Storage provider abstraction for accessing files from various sources.
 *
 * <p>This package implements a unified storage abstraction layer that allows
 * the file adapter to seamlessly access files from different storage systems
 * including local filesystem, cloud storage, and remote servers.</p>
 *
 * <h2>Core Abstraction</h2>
 * <p>{@link org.apache.calcite.adapter.file.storage.StorageProvider} defines the interface for:</p>
 * <ul>
 *   <li>Listing files and directories</li>
 *   <li>Opening input streams and readers</li>
 *   <li>Checking file existence and metadata</li>
 *   <li>Recursive directory traversal</li>
 * </ul>
 *
 * <h2>Storage Implementations</h2>
 *
 * <h3>Local File System</h3>
 * <p>{@link org.apache.calcite.adapter.file.storage.LocalFileStorageProvider}:</p>
 * <ul>
 *   <li>Access to local files and directories</li>
 *   <li>File pattern matching with glob support</li>
 *   <li>Efficient local I/O operations</li>
 * </ul>
 *
 * <h3>Amazon S3</h3>
 * <p>{@link org.apache.calcite.adapter.file.storage.S3StorageProvider}:</p>
 * <ul>
 *   <li>Access S3 buckets and objects</li>
 *   <li>Support for S3 URIs (s3://bucket/path)</li>
 *   <li>Configurable AWS regions and credentials</li>
 *   <li>Prefix-based listing and filtering</li>
 * </ul>
 *
 * <h3>HTTP/HTTPS</h3>
 * <p>{@link org.apache.calcite.adapter.file.storage.HttpStorageProvider}:</p>
 * <ul>
 *   <li>Download files from web servers</li>
 *   <li>Support for authentication headers</li>
 *   <li>Follow redirects automatically</li>
 * </ul>
 *
 * <h3>FTP/SFTP</h3>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.storage.FtpStorageProvider} - FTP protocol support</li>
 *   <li>{@link org.apache.calcite.adapter.file.storage.SftpStorageProvider} - Secure FTP over SSH</li>
 * </ul>
 *
 * <h3>Microsoft SharePoint</h3>
 * <p>{@link org.apache.calcite.adapter.file.storage.MicrosoftGraphStorageProvider}:</p>
 * <ul>
 *   <li>Access SharePoint document libraries</li>
 *   <li>OAuth 2.0 authentication</li>
 *   <li>Microsoft Graph API integration</li>
 *   <li>Token management with {@link org.apache.calcite.adapter.file.storage.SharePointTokenManager}</li>
 * </ul>
 *
 * <h2>Storage Abstraction Classes</h2>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.storage.StorageProviderFile} - File wrapper for remote files</li>
 *   <li>{@link org.apache.calcite.adapter.file.storage.StorageProviderSource} - Source implementation for storage providers</li>
 *   <li>{@link org.apache.calcite.adapter.file.storage.StorageProviderFactory} - Factory for creating storage providers</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <p>Storage providers are configured through schema properties:</p>
 * <pre>{@code
 * {
 *   "storageType": "s3",
 *   "storageConfig": {
 *     "region": "us-west-1",
 *     "accessKeyId": "...",
 *     "secretAccessKey": "...",
 *     "bucket": "my-bucket"
 *   }
 * }
 * }</pre>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create S3 storage provider
 * Map<String, Object> config = new HashMap<>();
 * config.put("region", "us-west-1");
 * config.put("bucket", "data-bucket");
 * 
 * StorageProvider provider = StorageProviderFactory.create("s3", config);
 * 
 * // List files
 * List<FileEntry> files = provider.listFiles("/data/csv/", "*.csv", true);
 * 
 * // Open file for reading
 * try (Reader reader = provider.openReader("/data/csv/sales.csv")) {
 *     // Process file content
 * }
 * }</pre>
 *
 * <h2>Performance Considerations</h2>
 * <ul>
 *   <li>Local storage is fastest with direct file system access</li>
 *   <li>Cloud storage providers cache metadata to reduce API calls</li>
 *   <li>HTTP provider supports connection pooling and keep-alive</li>
 *   <li>Large files are streamed rather than loaded into memory</li>
 * </ul>
 *
 * <h2>Security</h2>
 * <ul>
 *   <li>Credentials should be provided via environment variables or secure configuration</li>
 *   <li>SharePoint uses OAuth 2.0 with refresh token support</li>
 *   <li>SFTP provides encrypted file transfer</li>
 *   <li>S3 supports IAM roles and temporary credentials</li>
 * </ul>
 */
@PackageMarker
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.avatica.util.PackageMarker;
