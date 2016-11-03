/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.atlas.reporting;

import java.util.List;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for interacting with Referenceable instances.
 */
public class ReferenceableUtil {

    static final Logger LOGGER = LoggerFactory.getLogger(ReferenceableUtil.class);

    /**
     * Utility to retrieve a Reference by query.
     *
     * @param atlasClient
     * @param typeName
     * @param dslQuery
     * @return
     * @throws Exception
     */
    public static Referenceable getEntityReferenceFromDSL(final AtlasClient atlasClient, final String typeName, final String dslQuery)
            throws Exception {

        final JSONArray results = atlasClient.searchByDSL(dslQuery);
        if (results.length() == 0) {
            return null;
        } else {
            String guid;
            JSONObject row = results.getJSONObject(0);
            if (row.has("$id$")) {
                guid = row.getJSONObject("$id$").getString("id");
            } else {
                guid = row.getJSONObject("_col_0").getString("id");
            }
            return new Referenceable(guid, typeName, null);
        }
    }

    /**
     * Utility to create an entity and return the Referenceable instance with a populated id.
     *
     * @param atlasClient
     * @param referenceable
     * @return
     * @throws Exception
     */
    public static Referenceable register(final AtlasClient atlasClient, final Referenceable referenceable) throws Exception {
        if (referenceable == null) {
            return null;
        }

        final String typeName = referenceable.getTypeName();
        LOGGER.debug("creating instance of type " + typeName);

        final String entityJSON = InstanceSerialization.toJson(referenceable, true);
        LOGGER.debug("Submitting new entity {} = {}", new Object[] { referenceable.getTypeName(), entityJSON });

        final List<String> guids = atlasClient.createEntity(entityJSON);
        LOGGER.debug("created instance for type " + typeName + ", guid: " + guids);

        return new Referenceable(guids.get(0), referenceable.getTypeName(), null);
    }

}
