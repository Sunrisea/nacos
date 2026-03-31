/*
 * Copyright 1999-2026 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.ai.config;

import com.alibaba.nacos.api.ai.model.prompt.PromptDescriptor;
import com.alibaba.nacos.api.ai.model.prompt.PromptLabelVersionMapping;

import java.util.List;

/**
 * SPI interface for reading legacy prompt data during migration.
 *
 * <p>Different environments (open-source Nacos vs commercial) may store prompt data
 * in different legacy formats. Implementations of this interface provide the ability
 * to scan and read legacy prompt data for migration to the new DB + typed storage
 * architecture.</p>
 *
 * <p>The default implementation ({@code nacos}) reads from Nacos Config
 * ({@code nacos-ai-prompt} group). Commercial implementations can provide their own
 * {@code @Component} bean to override.</p>
 *
 * <p>The active reader is selected by configuration property
 * {@code nacos.ai.prompt.migration.provider} (default: {@code nacos}).</p>
 *
 * @author nacos
 * @since 3.2.0
 */
public interface PromptLegacyDataReader {
    
    /**
     * Provider type identifier.
     *
     * @return type string, e.g. "nacos"
     */
    String type();
    
    /**
     * Scan legacy storage and return all prompt keys that have legacy data.
     *
     * @return list of prompt keys found in legacy storage
     */
    List<String> scanLegacyPromptKeys();
    
    /**
     * Read the legacy descriptor for a prompt.
     *
     * @param promptKey prompt key
     * @return descriptor, or null if not found
     */
    PromptDescriptor readDescriptor(String promptKey);
    
    /**
     * Read the legacy label/version mapping for a prompt.
     *
     * @param promptKey prompt key
     * @return mapping, or null if not found
     */
    PromptLabelVersionMapping readLabelVersionMapping(String promptKey);
    
    /**
     * Read the legacy version content for a specific prompt version.
     *
     * @param promptKey prompt key
     * @param version   version string
     * @param mapping   the label/version mapping (for fallback resolution)
     * @return version content as JSON string, or null if not found
     */
    String readVersionContent(String promptKey, String version, PromptLabelVersionMapping mapping);
}
