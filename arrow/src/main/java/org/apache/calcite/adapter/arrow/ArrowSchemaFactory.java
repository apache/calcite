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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates an {@link ArrowSchema}.
 */
public class ArrowSchemaFactory implements SchemaFactory {
     private static final Logger LOGGER = LoggerFactory.getLogger(ArrowSchemaFactory.class);

     @Override public Schema create(SchemaPlus parentSchema, String name,
         Map<String, Object> operand) {
       LOGGER.info("ArrowSchemaFactory.create called with name: {}", name);
       final File baseDirectory =
           (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
       final String directory = (String) operand.get("directory");
       File directoryFile = null;
       if (directory != null) {
         directoryFile = new File(directory);
       }
       if (baseDirectory != null) {
         if (directoryFile == null) {
           directoryFile = baseDirectory;
         } else if (!directoryFile.isAbsolute()) {
           directoryFile = new File(baseDirectory, directoryFile.getPath());
         }
       }
       if (directoryFile == null) {
         LOGGER.error("No directory specified");
         throw new RuntimeException("no directory");
       }
       LOGGER.info("Creating ArrowSchema with directory: {}", directoryFile.getAbsolutePath());
       try {
         // Log the class loading
         LOGGER.info("About to load ArrowSchema class");
         Class.forName("org.apache.calcite.adapter.arrow.ArrowSchema");
         LOGGER.info("ArrowSchema class loaded successfully");

         // Create the ArrowSchema instance
         LOGGER.info("About to create ArrowSchema instance");
         ArrowSchema schema = new ArrowSchema(directoryFile);
         LOGGER.info("ArrowSchema created successfully");
         return schema;
       } catch(ClassNotFoundException e) {
         LOGGER.error("Error loading ArrowSchema class", e);
         return null;
       } catch(NoClassDefFoundError e) {
         LOGGER.error("Error finding a class definition required by ArrowSchema", e);
         return null;
       } catch(ExceptionInInitializerError e) {
         LOGGER.error("Error in static initializer of ArrowSchema or a dependency", e);
         return null;
       } catch(LinkageError e) {
         LOGGER.error("Linkage error when loading ArrowSchema or a dependency", e);
         return null;
       } catch(Throwable e) {
         LOGGER.error("Unexpected error creating ArrowSchema", e);
         return null;
       }
     }
   }
