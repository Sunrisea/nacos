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

import com.alibaba.nacos.ai.model.AiResource;
import com.alibaba.nacos.ai.model.AiResourceVersion;
import com.alibaba.nacos.ai.service.prompt.PromptOperationService;
import com.alibaba.nacos.ai.service.repository.AiResourcePersistService;
import com.alibaba.nacos.ai.service.repository.AiResourceVersionPersistService;
import com.alibaba.nacos.ai.storage.NacosConfigAiResourceStorage;
import com.alibaba.nacos.api.ai.model.prompt.PromptDescriptor;
import com.alibaba.nacos.api.ai.model.prompt.PromptLabelVersionMapping;
import com.alibaba.nacos.api.ai.model.prompt.PromptUtils;
import com.alibaba.nacos.api.ai.model.prompt.PromptVersionInfo;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadFactoryBuilder;
import com.alibaba.nacos.config.server.exception.ConfigAlreadyExistsException;
import com.alibaba.nacos.config.server.model.ConfigRequestInfo;
import com.alibaba.nacos.config.server.model.form.ConfigForm;
import com.alibaba.nacos.config.server.service.ConfigOperationService;
import com.alibaba.nacos.config.server.service.query.ConfigQueryChainService;
import com.alibaba.nacos.config.server.service.query.model.ConfigQueryChainRequest;
import com.alibaba.nacos.config.server.service.query.model.ConfigQueryChainResponse;
import com.alibaba.nacos.plugin.ai.storage.AiResourceStorageRouter;
import com.alibaba.nacos.plugin.ai.storage.model.StorageKey;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Migrates prompt data from legacy storage to the new DB + SPI typed storage architecture.
 *
 * <p>Uses {@link PromptLegacyDataReader} SPI to read legacy data. The default implementation
 * ({@code nacos}) reads from Nacos Config ({@code nacos-ai-prompt} group). Commercial
 * environments can provide their own {@code @Component} implementing {@link PromptLegacyDataReader}.</p>
 *
 * <p>The active reader is selected by configuration property
 * {@code nacos.ai.prompt.migration.provider} (default: {@code nacos}).</p>
 *
 * @author nacos
 */
@Component
public class PromptDataMigrationTask implements ApplicationListener<ApplicationReadyEvent> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PromptDataMigrationTask.class);
    
    private static final String MIGRATION_MARKER_DATA_ID = "nacos.ai.prompt.migration";
    
    private static final String MIGRATION_MARKER_GROUP = "nacos_internal";
    
    private static final long MIGRATION_MARKER_STALE_MILLIS = 10 * 60 * 1000L;
    
    private static final String RESOURCE_TYPE_PROMPT = "prompt";
    
    private static final String VERSION_STATUS_ONLINE = "online";
    
    private static final String META_STATUS_ENABLE = "enable";
    
    private static final String STORAGE_PROVIDER_NACOS_CONFIG = "nacos_config";
    
    private static final String PROMPT_STORAGE_PROVIDER_CONFIG_KEY = "nacos.ai.prompt.storage.provider";
    
    private static final String MIGRATION_ENABLED_KEY = "nacos.ai.prompt.migration.enabled";
    
    private static final String MIGRATION_PROVIDER_KEY = "nacos.ai.prompt.migration.provider";
    
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    
    private final ExecutorService migrationExecutor = ExecutorFactory.Managed.newSingleExecutorService(
            PromptDataMigrationTask.class.getCanonicalName(),
            new ThreadFactoryBuilder().daemon(true).nameFormat("nacos-ai-prompt-migration-%d").build());
    
    private final AiResourcePersistService aiResourcePersistService;
    
    private final AiResourceVersionPersistService aiResourceVersionPersistService;
    
    private final PromptOperationService promptOperationService;
    
    private final ConfigQueryChainService configQueryChainService;
    
    private final ConfigOperationService configOperationService;
    
    private final List<PromptLegacyDataReader> legacyDataReaders;
    
    public PromptDataMigrationTask(AiResourcePersistService aiResourcePersistService,
            AiResourceVersionPersistService aiResourceVersionPersistService,
            PromptOperationService promptOperationService,
            ConfigQueryChainService configQueryChainService, ConfigOperationService configOperationService,
            List<PromptLegacyDataReader> legacyDataReaders) {
        this.aiResourcePersistService = aiResourcePersistService;
        this.aiResourceVersionPersistService = aiResourceVersionPersistService;
        this.promptOperationService = promptOperationService;
        this.configQueryChainService = configQueryChainService;
        this.configOperationService = configOperationService;
        this.legacyDataReaders = legacyDataReaders != null ? legacyDataReaders : new ArrayList<>();
    }
    
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (event.getApplicationContext().getParent() != null) {
            return;
        }
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        boolean enabled = Boolean.parseBoolean(EnvUtil.getProperty(MIGRATION_ENABLED_KEY, "true"));
        if (!enabled) {
            LOGGER.info("Prompt data migration is disabled via {}", MIGRATION_ENABLED_KEY);
            return;
        }
        migrationExecutor.execute(this::executeMigration);
    }
    
    private PromptLegacyDataReader resolveLegacyDataReader() {
        String providerType = EnvUtil.getProperty(MIGRATION_PROVIDER_KEY, NacosPromptLegacyDataReader.TYPE);
        for (PromptLegacyDataReader reader : legacyDataReaders) {
            if (providerType.equals(reader.type())) {
                LOGGER.info("Using PromptLegacyDataReader: {}", reader.type());
                return reader;
            }
        }
        LOGGER.warn("No PromptLegacyDataReader found for type '{}', skip migration", providerType);
        return null;
    }
    
    private void executeMigration() {
        boolean markerCreated = false;
        try {
            PromptLegacyDataReader reader = resolveLegacyDataReader();
            if (reader == null) {
                return;
            }
            
            List<String> promptKeys = reader.scanLegacyPromptKeys();
            if (promptKeys.isEmpty()) {
                LOGGER.info("No legacy prompt data found by reader '{}', skip migration", reader.type());
                return;
            }
            
            List<String> needsMigration = filterNeedsMigration(promptKeys, reader);
            if (needsMigration.isEmpty()) {
                LOGGER.info("All {} legacy prompts already migrated, skip", promptKeys.size());
                return;
            }
            
            markerCreated = tryAcquireMigrationMarker();
            if (!markerCreated) {
                LOGGER.info("Skip prompt migration because another node is migrating");
                return;
            }
            
            needsMigration = filterNeedsMigration(promptKeys, reader);
            if (needsMigration.isEmpty()) {
                LOGGER.info("All legacy prompts already migrated after acquiring marker");
                return;
            }
            
            LOGGER.info("Start prompt data migration: {} prompts to migrate out of {} total", needsMigration.size(),
                    promptKeys.size());
            
            int migrated = 0;
            int failed = 0;
            for (String promptKey : needsMigration) {
                try {
                    migrateOnePrompt(promptKey, reader);
                    migrated++;
                } catch (Exception e) {
                    failed++;
                    LOGGER.error("Failed to migrate prompt '{}': {}", promptKey, e.getMessage(), e);
                }
            }
            
            LOGGER.info("Prompt data migration completed: migrated={}, failed={}, skipped={}", migrated, failed,
                    promptKeys.size() - needsMigration.size());
        } catch (Exception e) {
            LOGGER.error("Prompt data migration failed unexpectedly", e);
        } finally {
            if (markerCreated) {
                releaseMigrationMarker();
            }
        }
    }
    
    private List<String> filterNeedsMigration(List<String> promptKeys, PromptLegacyDataReader reader) {
        String namespace = com.alibaba.nacos.api.common.Constants.DEFAULT_NAMESPACE_ID;
        List<String> result = new ArrayList<>();
        for (String promptKey : promptKeys) {
            AiResource existing = aiResourcePersistService.find(namespace, promptKey, RESOURCE_TYPE_PROMPT);
            if (existing == null) {
                result.add(promptKey);
                continue;
            }
            PromptLabelVersionMapping mapping = reader.readLabelVersionMapping(promptKey);
            if (mapping == null || mapping.getVersions() == null) {
                continue;
            }
            for (String version : mapping.getVersions()) {
                AiResourceVersion versionRow = aiResourceVersionPersistService.find(namespace, promptKey,
                        RESOURCE_TYPE_PROMPT, version);
                if (versionRow == null) {
                    result.add(promptKey);
                    break;
                }
            }
        }
        return result;
    }
    
    private void migrateOnePrompt(String promptKey, PromptLegacyDataReader reader) throws Exception {
        String namespace = com.alibaba.nacos.api.common.Constants.DEFAULT_NAMESPACE_ID;
        
        PromptDescriptor descriptor = reader.readDescriptor(promptKey);
        PromptLabelVersionMapping mapping = reader.readLabelVersionMapping(promptKey);
        
        if (mapping == null || mapping.getVersions() == null || mapping.getVersions().isEmpty()) {
            LOGGER.warn("Prompt '{}' has no versions in mapping, skip migration", promptKey);
            return;
        }
        
        String description = descriptor != null ? descriptor.getDescription() : null;
        List<String> bizTags = descriptor != null ? descriptor.getBizTags() : null;
        
        Map<String, Object> versionInfoMap = new HashMap<>(8);
        versionInfoMap.put("labels", mapping.getLabels() != null ? mapping.getLabels() : new HashMap<>());
        versionInfoMap.put("editingVersion", null);
        versionInfoMap.put("reviewingVersion", null);
        versionInfoMap.put("onlineCnt", mapping.getVersions().size());
        
        AiResource resource = new AiResource();
        resource.setNamespaceId(namespace);
        resource.setName(promptKey);
        resource.setType(RESOURCE_TYPE_PROMPT);
        resource.setDesc(description);
        resource.setStatus(META_STATUS_ENABLE);
        resource.setMetaVersion(1L);
        resource.setVersionInfo(JacksonUtils.toJson(versionInfoMap));
        resource.setBizTags(bizTags != null ? JacksonUtils.toJson(bizTags) : "[]");
        
        try {
            aiResourcePersistService.insert(resource);
            LOGGER.info("Migrated prompt '{}' resource record to DB", promptKey);
        } catch (Exception e) {
            AiResource existing = aiResourcePersistService.find(namespace, promptKey, RESOURCE_TYPE_PROMPT);
            if (existing != null) {
                LOGGER.info("Prompt '{}' resource record already exists, continue with version migration", promptKey);
            } else {
                throw e;
            }
        }
        
        int versionsMigrated = 0;
        for (String version : mapping.getVersions()) {
            try {
                migrateOneVersion(namespace, promptKey, version, mapping, reader);
                versionsMigrated++;
            } catch (Exception e) {
                LOGGER.error("Failed to migrate prompt '{}' version '{}': {}", promptKey, version, e.getMessage(), e);
            }
        }
        
        try {
            promptOperationService.refreshLatestMirror(namespace, promptKey);
        } catch (Exception e) {
            LOGGER.warn("Failed to refresh legacy mirror for prompt '{}': {}", promptKey, e.getMessage());
        }
        
        LOGGER.info("Migrated prompt '{}': {}/{} versions", promptKey, versionsMigrated, mapping.getVersions().size());
    }
    
    private void migrateOneVersion(String namespace, String promptKey, String version,
            PromptLabelVersionMapping mapping, PromptLegacyDataReader reader) throws Exception {
        String content = reader.readVersionContent(promptKey, version, mapping);
        if (StringUtils.isBlank(content)) {
            LOGGER.warn("No content found for prompt '{}' version '{}', skip", promptKey, version);
            return;
        }
        
        PromptVersionInfo versionInfo;
        try {
            versionInfo = JacksonUtils.toObj(content, PromptVersionInfo.class);
        } catch (Exception e) {
            versionInfo = new PromptVersionInfo();
            versionInfo.setPromptKey(promptKey);
            versionInfo.setVersion(version);
            versionInfo.setTemplate(content);
        }
        versionInfo.setPromptKey(promptKey);
        versionInfo.setVersion(version);
        
        writeToTypedStorage(namespace, promptKey, version, versionInfo);
        
        AiResourceVersion existing = aiResourceVersionPersistService.find(namespace, promptKey, RESOURCE_TYPE_PROMPT,
                version);
        if (existing != null) {
            LOGGER.debug("Prompt '{}' version '{}' already in DB, skip insert", promptKey, version);
            return;
        }
        
        AiResourceVersion versionRecord = new AiResourceVersion();
        versionRecord.setNamespaceId(namespace);
        versionRecord.setName(promptKey);
        versionRecord.setType(RESOURCE_TYPE_PROMPT);
        versionRecord.setVersion(version);
        versionRecord.setStatus(VERSION_STATUS_ONLINE);
        versionRecord.setAuthor("-");
        versionRecord.setDesc("migrated from legacy config");
        versionRecord.setStorage(buildStorageJson(namespace, promptKey, version));
        
        try {
            aiResourceVersionPersistService.insert(versionRecord);
        } catch (Exception e) {
            AiResourceVersion check = aiResourceVersionPersistService.find(namespace, promptKey, RESOURCE_TYPE_PROMPT,
                    version);
            if (check != null) {
                LOGGER.debug("Prompt '{}' version '{}' inserted by another node, skip", promptKey, version);
            } else {
                throw e;
            }
        }
        
        LOGGER.debug("Migrated prompt '{}' version '{}'", promptKey, version);
    }
    
    private boolean tryAcquireMigrationMarker() {
        for (int i = 0; i < 2; i++) {
            try {
                ConfigForm form = new ConfigForm();
                form.setNamespaceId(com.alibaba.nacos.api.common.Constants.DEFAULT_NAMESPACE_ID);
                form.setGroup(MIGRATION_MARKER_GROUP);
                form.setDataId(MIGRATION_MARKER_DATA_ID);
                form.setContent(String.valueOf(System.currentTimeMillis()));
                form.setSrcUser("nacos");
                ConfigRequestInfo requestInfo = new ConfigRequestInfo();
                requestInfo.setUpdateForExist(false);
                configOperationService.publishConfig(form, requestInfo, null);
                return true;
            } catch (ConfigAlreadyExistsException e) {
                if (isMigrationMarkerStale()) {
                    LOGGER.warn("Found stale prompt migration marker, removing and retrying");
                    releaseMigrationMarker();
                    continue;
                }
                return false;
            } catch (Exception e) {
                LOGGER.error("Failed to create prompt migration marker", e);
                return false;
            }
        }
        return false;
    }
    
    private boolean isMigrationMarkerStale() {
        try {
            ConfigQueryChainRequest request = ConfigQueryChainRequest.buildConfigQueryChainRequest(
                    MIGRATION_MARKER_DATA_ID, MIGRATION_MARKER_GROUP,
                    com.alibaba.nacos.api.common.Constants.DEFAULT_NAMESPACE_ID);
            ConfigQueryChainResponse response = configQueryChainService.handle(request);
            if (response.getStatus() == ConfigQueryChainResponse.ConfigQueryStatus.CONFIG_NOT_FOUND
                    || StringUtils.isBlank(response.getContent())) {
                return false;
            }
            long markerTime = Long.parseLong(response.getContent().trim());
            return System.currentTimeMillis() - markerTime > MIGRATION_MARKER_STALE_MILLIS;
        } catch (Exception e) {
            LOGGER.warn("Failed to inspect prompt migration marker", e);
            return false;
        }
    }
    
    private void releaseMigrationMarker() {
        try {
            configOperationService.deleteConfig(MIGRATION_MARKER_DATA_ID, MIGRATION_MARKER_GROUP,
                    com.alibaba.nacos.api.common.Constants.DEFAULT_NAMESPACE_ID, null, null, "nacos", null);
        } catch (Exception e) {
            LOGGER.warn("Failed to delete prompt migration marker", e);
        }
    }
    
    private void writeToTypedStorage(String namespace, String promptKey, String version, PromptVersionInfo versionInfo)
            throws NacosException {
        String provider = resolveStorageProvider();
        byte[] contentBytes = JacksonUtils.toJson(versionInfo).getBytes(StandardCharsets.UTF_8);
        StorageKey storageKey = NacosConfigAiResourceStorage.buildStorageKey(provider, namespace,
                NacosConfigAiResourceStorage.RESOURCE_TYPE_PROMPT, promptKey, version,
                PromptUtils.PROMPT_MAIN_DATA_ID);
        AiResourceStorageRouter.getInstance().route(storageKey).save(storageKey, contentBytes);
    }
    
    private static String buildStorageJson(String namespace, String promptKey, String version) {
        Map<String, Object> json = new HashMap<>(4);
        json.put("provider", resolveStorageProvider());
        json.put("scope", namespace + ":" + promptKey + ":" + version);
        json.put("files", Collections.singletonList(PromptUtils.PROMPT_MAIN_DATA_ID));
        return JacksonUtils.toJson(json);
    }
    
    private static String resolveStorageProvider() {
        String provider = EnvUtil.getProperty(PROMPT_STORAGE_PROVIDER_CONFIG_KEY, STORAGE_PROVIDER_NACOS_CONFIG);
        return StringUtils.isBlank(provider) ? STORAGE_PROVIDER_NACOS_CONFIG : provider.trim();
    }
}
