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

import com.alibaba.nacos.ai.constant.Constants;
import com.alibaba.nacos.ai.utils.PromptDataIdUtils;
import com.alibaba.nacos.api.ai.model.prompt.PromptDescriptor;
import com.alibaba.nacos.api.ai.model.prompt.PromptLabelVersionMapping;
import com.alibaba.nacos.api.model.Page;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.service.query.ConfigQueryChainService;
import com.alibaba.nacos.config.server.service.query.model.ConfigQueryChainRequest;
import com.alibaba.nacos.config.server.service.query.model.ConfigQueryChainResponse;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Default {@link PromptLegacyDataReader} for open-source Nacos.
 * Reads legacy prompt data from Nacos Config ({@code nacos-ai-prompt} group).
 *
 * @author nacos
 * @since 3.2.0
 */
@Component
public class NacosPromptLegacyDataReader implements PromptLegacyDataReader {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NacosPromptLegacyDataReader.class);
    
    public static final String TYPE = "nacos";
    
    private static final int SCAN_PAGE_SIZE = 100;
    
    private static final String PROMPT_GROUP = Constants.Prompt.PROMPT_GROUP;
    
    private static final String DEFAULT_NAMESPACE = com.alibaba.nacos.api.common.Constants.DEFAULT_NAMESPACE_ID;
    
    private final ConfigInfoPersistService configInfoPersistService;
    
    private final ConfigQueryChainService configQueryChainService;
    
    public NacosPromptLegacyDataReader(ConfigInfoPersistService configInfoPersistService,
            ConfigQueryChainService configQueryChainService) {
        this.configInfoPersistService = configInfoPersistService;
        this.configQueryChainService = configQueryChainService;
    }
    
    @Override
    public String type() {
        return TYPE;
    }
    
    @Override
    public List<String> scanLegacyPromptKeys() {
        List<String> promptKeys = new ArrayList<>();
        int pageNo = 1;
        while (true) {
            Page<ConfigInfo> page = configInfoPersistService.findConfigInfo4Page(pageNo, SCAN_PAGE_SIZE, null,
                    PROMPT_GROUP, DEFAULT_NAMESPACE, null);
            if (page == null || page.getPageItems() == null || page.getPageItems().isEmpty()) {
                break;
            }
            for (ConfigInfo info : page.getPageItems()) {
                if (PromptDataIdUtils.isDescriptorDataId(info.getDataId())) {
                    String key = PromptDataIdUtils.extractPromptKeyFromDescriptorDataId(info.getDataId());
                    if (StringUtils.isNotBlank(key)) {
                        promptKeys.add(key);
                    }
                }
            }
            if (page.getPageItems().size() < SCAN_PAGE_SIZE) {
                break;
            }
            pageNo++;
        }
        return promptKeys;
    }
    
    @Override
    public PromptDescriptor readDescriptor(String promptKey) {
        return readConfigJson(PromptDataIdUtils.buildDescriptorDataId(promptKey), PromptDescriptor.class);
    }
    
    @Override
    public PromptLabelVersionMapping readLabelVersionMapping(String promptKey) {
        return readConfigJson(PromptDataIdUtils.buildLabelVersionMappingDataId(promptKey),
                PromptLabelVersionMapping.class);
    }
    
    @Override
    public String readVersionContent(String promptKey, String version, PromptLabelVersionMapping mapping) {
        String versionDataId = PromptDataIdUtils.buildVersionDataId(promptKey, version);
        String content = readConfigContent(versionDataId);
        if (StringUtils.isBlank(content) && mapping != null && version.equals(mapping.getLatestVersion())) {
            content = readConfigContent(PromptDataIdUtils.buildLatestDataId(promptKey));
        }
        return content;
    }
    
    private <T> T readConfigJson(String dataId, Class<T> clazz) {
        String content = readConfigContent(dataId);
        if (StringUtils.isBlank(content)) {
            return null;
        }
        try {
            return JacksonUtils.toObj(content, clazz);
        } catch (Exception e) {
            LOGGER.warn("Failed to parse config '{}' as {}: {}", dataId, clazz.getSimpleName(), e.getMessage());
            return null;
        }
    }
    
    private String readConfigContent(String dataId) {
        try {
            ConfigQueryChainRequest request = ConfigQueryChainRequest.buildConfigQueryChainRequest(dataId,
                    PROMPT_GROUP, DEFAULT_NAMESPACE);
            ConfigQueryChainResponse response = configQueryChainService.handle(request);
            if (response.getStatus() == ConfigQueryChainResponse.ConfigQueryStatus.CONFIG_NOT_FOUND) {
                return null;
            }
            return response.getContent();
        } catch (Exception e) {
            LOGGER.warn("Failed to read config '{}': {}", dataId, e.getMessage());
            return null;
        }
    }
}
