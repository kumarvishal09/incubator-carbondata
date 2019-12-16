/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto;

import java.lang.reflect.*;
import java.util.Map;
import java.util.Optional;

import io.prestosql.plugin.hive.HiveConnectorFactory;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbondataConnectorFactory extends HiveConnectorFactory {


  public CarbondataConnectorFactory(String connectorName) {
    super(connectorName);
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config,
      ConnectorContext context) {
    ClassLoader classLoader = context.duplicatePluginClassLoader();
    try {
      return (Connector) classLoader.loadClass(InternalHiveConnectorFactory.class.getName())
          .getMethod("createConnector", String.class, Map.class, ConnectorContext.class, Optional.class)
          .invoke(null, catalogName, config, context, Optional.empty());
    }
    catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      throwIfUnchecked(targetException);
      throw new RuntimeException(targetException);
    }
    catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}