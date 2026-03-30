/*
 * Copyright 1999-2025 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.ai.service.agentspecs;

import com.alibaba.nacos.ai.model.AiResource;
import com.alibaba.nacos.ai.model.AiResourceVersion;
import com.alibaba.nacos.ai.pipeline.PublishPipelineExecutor;
import com.alibaba.nacos.ai.pipeline.repository.PipelineExecutionRepository;
import com.alibaba.nacos.ai.service.repository.AiResourcePersistService;
import com.alibaba.nacos.ai.service.repository.AiResourceVersionPersistService;
import com.alibaba.nacos.plugin.ai.pipeline.model.PublishPipelineContext;
import com.alibaba.nacos.plugin.ai.pipeline.model.PublishPipelineResourceType;
import com.alibaba.nacos.sys.env.EnvUtil;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.mockito.ArgumentCaptor;
import org.springframework.core.env.StandardEnvironment;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Property 5: Submit behavior consistency with Pipeline availability.
 *
 * <p>When Pipeline is not enabled or has no matching nodes, submit directly publishes
 * and version status becomes online; when Pipeline is enabled and has matching nodes,
 * version status becomes reviewing, editingVersion is cleared, reviewingVersion is set.</p>
 *
 * <p><b>Validates: Requirements 4.3, 4.4</b></p>
 *
 * @author kiro
 * @since 3.2.0
 */
class AgentSpecSubmitPipelineAvailabilityPropertyTest {

    private static final String RESOURCE_TYPE_AGENTSPEC = "agentspec";

    /**
     * Property 5a: When Pipeline is not enabled or has no matching nodes (execute returns null),
     * submit directly publishes and version status becomes online.
     *
     * <p><b>Validates: Requirements 4.3</b></p>
     */
    @Property(tries = 50)
    void submitDirectlyPublishesWhenPipelineDisabled(
            @ForAll("submitInputs") SubmitInput input) throws Exception {
        EnvUtil.setEnvironment(new StandardEnvironment());

        // Mock dependencies
        final AiResourcePersistService aiResourcePersistService = mock(AiResourcePersistService.class);
        final AiResourceVersionPersistService aiResourceVersionPersistService =
                mock(AiResourceVersionPersistService.class);
        final PublishPipelineExecutor publishPipelineExecutor = mock(PublishPipelineExecutor.class);
        final PipelineExecutionRepository pipelineExecutionRepository = mock(PipelineExecutionRepository.class);

        // Build meta with editingVersion
        AiResource meta = buildMeta(input.namespaceId, input.name, input.version);
        when(aiResourcePersistService.find(eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC)))
                .thenReturn(meta);

        // Build version row as draft
        AiResourceVersion draftVersion = buildVersionRow(input.namespaceId, input.name, input.version, "draft");
        when(aiResourceVersionPersistService.find(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC), eq(input.version)))
                .thenReturn(draftVersion);

        // Pipeline not available
        when(publishPipelineExecutor.isPipelineAvailable(any(PublishPipelineResourceType.class)))
                .thenReturn(false);

        // Mock updateMetaCas to return true
        when(aiResourcePersistService.updateMetaCas(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC), any(Long.class), any()))
                .thenReturn(true);

        // Construct service and call submit
        AgentSpecOperationServiceImpl service = new AgentSpecOperationServiceImpl(
                aiResourcePersistService, aiResourceVersionPersistService,
                publishPipelineExecutor, pipelineExecutionRepository);
        String result = service.submit(input.namespaceId, input.name, input.version);

        assertEquals(input.version, result, "submit should return the version");

        // Verify: updateStatus was called twice — first "reviewing" (before pipeline check), then "online" (direct publish)
        ArgumentCaptor<String> statusCaptor = ArgumentCaptor.forClass(String.class);
        verify(aiResourceVersionPersistService, times(2)).updateStatus(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC),
                eq(input.version), statusCaptor.capture());
        List<String> allStatuses = statusCaptor.getAllValues();
        assertEquals("reviewing", allStatuses.get(0),
                "First status update should be 'reviewing' (before pipeline check)");
        assertEquals("online", allStatuses.get(1),
                "Second status update should be 'online' (direct publish when pipeline is disabled)");

        // Verify: execute was never called since isPipelineAvailable returned false
        verify(publishPipelineExecutor, never()).execute(
                any(PublishPipelineContext.class), any(), any(String.class));

        // Verify: updatePublishPipelineInfo was NOT called (no pipeline execution)
        verify(aiResourceVersionPersistService, never()).updatePublishPipelineInfo(
                any(), any(), any(), any(), any());
    }

    /**
     * Property 5b: When Pipeline is enabled and has matching nodes (isPipelineAvailable returns true),
     * version status becomes reviewing, editingVersion is cleared, reviewingVersion is set,
     * pipelineInfo is written with IN_PROGRESS BEFORE execute is called.
     *
     * <p><b>Validates: Requirements 4.4</b></p>
     */
    @Property(tries = 50)
    void submitSetsReviewingWhenPipelineEnabled(
            @ForAll("submitInputs") SubmitInput input) throws Exception {
        EnvUtil.setEnvironment(new StandardEnvironment());

        // Mock dependencies
        final AiResourcePersistService aiResourcePersistService = mock(AiResourcePersistService.class);
        final AiResourceVersionPersistService aiResourceVersionPersistService =
                mock(AiResourceVersionPersistService.class);
        final PublishPipelineExecutor publishPipelineExecutor = mock(PublishPipelineExecutor.class);
        final PipelineExecutionRepository pipelineExecutionRepository = mock(PipelineExecutionRepository.class);

        // Build meta with editingVersion
        AiResource meta = buildMeta(input.namespaceId, input.name, input.version);
        when(aiResourcePersistService.find(eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC)))
                .thenReturn(meta);

        // Build version row as draft
        AiResourceVersion draftVersion = buildVersionRow(input.namespaceId, input.name, input.version, "draft");
        when(aiResourceVersionPersistService.find(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC), eq(input.version)))
                .thenReturn(draftVersion);

        // Pipeline is available
        when(publishPipelineExecutor.isPipelineAvailable(any(PublishPipelineResourceType.class)))
                .thenReturn(true);

        // Pipeline executor returns the caller-provided executionId (3-arg overload)
        when(publishPipelineExecutor.execute(any(PublishPipelineContext.class), any(), any(String.class)))
                .thenAnswer(invocation -> invocation.getArgument(2));

        // Mock updateMetaCas to return true
        when(aiResourcePersistService.updateMetaCas(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC), any(Long.class), any()))
                .thenReturn(true);

        // Construct service and call submit
        AgentSpecOperationServiceImpl service = new AgentSpecOperationServiceImpl(
                aiResourcePersistService, aiResourceVersionPersistService,
                publishPipelineExecutor, pipelineExecutionRepository);
        String result = service.submit(input.namespaceId, input.name, input.version);

        assertEquals(input.version, result, "submit should return the version");

        // Verify: updateStatus was called with "reviewing"
        ArgumentCaptor<String> statusCaptor = ArgumentCaptor.forClass(String.class);
        verify(aiResourceVersionPersistService).updateStatus(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC),
                eq(input.version), statusCaptor.capture());
        assertEquals("reviewing", statusCaptor.getValue(),
                "Version status should become 'reviewing' when pipeline is enabled");

        // Verify: updateMetaCas was called (editingVersion cleared, reviewingVersion set)
        ArgumentCaptor<AiResource> metaCaptor = ArgumentCaptor.forClass(AiResource.class);
        verify(aiResourcePersistService).updateMetaCas(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC),
                any(Long.class), metaCaptor.capture());
        String updatedVersionInfo = metaCaptor.getValue().getVersionInfo();
        assertTrue(updatedVersionInfo.contains("\"reviewingVersion\":\"" + input.version + "\""),
                "Meta should have reviewingVersion set to " + input.version
                        + ", but versionInfo was: " + updatedVersionInfo);
        assertTrue(!updatedVersionInfo.contains("\"editingVersion\":\"" + input.version + "\""),
                "Meta should have editingVersion cleared, but versionInfo was: " + updatedVersionInfo);

        // Capture executionId from the 3-arg execute call
        ArgumentCaptor<String> execIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(publishPipelineExecutor).execute(
                any(PublishPipelineContext.class), any(), execIdCaptor.capture());
        String callerExecutionId = execIdCaptor.getValue();

        // Verify: updatePublishPipelineInfo was called with IN_PROGRESS and the same executionId
        ArgumentCaptor<String> pipelineInfoCaptor = ArgumentCaptor.forClass(String.class);
        verify(aiResourceVersionPersistService).updatePublishPipelineInfo(
                eq(input.namespaceId), eq(input.name), eq(RESOURCE_TYPE_AGENTSPEC),
                eq(input.version), pipelineInfoCaptor.capture());
        String pipelineInfoJson = pipelineInfoCaptor.getValue();
        assertTrue(pipelineInfoJson.contains("IN_PROGRESS"),
                "publishPipelineInfo should contain IN_PROGRESS status, but was: " + pipelineInfoJson);
        assertTrue(pipelineInfoJson.contains(callerExecutionId),
                "publishPipelineInfo should contain the pre-generated executionId '"
                        + callerExecutionId + "', but was: " + pipelineInfoJson);
    }

    // ---- Helper methods ----

    private static AiResource buildMeta(String namespaceId, String name, String version) {
        AiResource meta = new AiResource();
        meta.setNamespaceId(namespaceId);
        meta.setName(name);
        meta.setType(RESOURCE_TYPE_AGENTSPEC);
        meta.setStatus("enable");
        meta.setMetaVersion(1L);
        meta.setVersionInfo("{\"editingVersion\":\"" + version
                + "\",\"onlineCnt\":0,\"labels\":{}}");
        return meta;
    }

    private static AiResourceVersion buildVersionRow(String namespaceId, String name,
            String version, String status) {
        AiResourceVersion versionRow = new AiResourceVersion();
        versionRow.setNamespaceId(namespaceId);
        versionRow.setName(name);
        versionRow.setType(RESOURCE_TYPE_AGENTSPEC);
        versionRow.setVersion(version);
        versionRow.setStatus(status);
        return versionRow;
    }

    // ---- Test input models ----

    static class SubmitInput {
        final String namespaceId;
        final String name;
        final String version;

        SubmitInput(String namespaceId, String name, String version) {
            this.namespaceId = namespaceId;
            this.name = name;
            this.version = version;
        }

        @Override
        public String toString() {
            return "SubmitInput{ns='" + namespaceId + "', name='" + name
                    + "', version='" + version + "'}";
        }
    }

    // ---- Arbitraries ----

    @Provide
    Arbitrary<SubmitInput> submitInputs() {
        Arbitrary<String> namespaceIds = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
        Arbitrary<String> names = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
        Arbitrary<String> versions = Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);

        return Combinators.combine(namespaceIds, names, versions).as(SubmitInput::new);
    }
}
