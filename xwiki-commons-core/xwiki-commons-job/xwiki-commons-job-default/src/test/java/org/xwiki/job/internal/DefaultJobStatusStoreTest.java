/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.xwiki.job.internal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.xwiki.cache.Cache;
import org.xwiki.cache.CacheException;
import org.xwiki.cache.CacheManager;
import org.xwiki.cache.config.CacheConfiguration;
import org.xwiki.cache.config.LRUCacheConfiguration;
import org.xwiki.component.util.ReflectionUtils;
import org.xwiki.job.DefaultJobStatus;
import org.xwiki.job.DefaultRequest;
import org.xwiki.job.JobManagerConfiguration;
import org.xwiki.job.event.status.JobStatus;
import org.xwiki.logging.tail.LoggerTail;
import org.xwiki.test.LogLevel;
import org.xwiki.test.annotation.BeforeComponent;
import org.xwiki.test.junit5.LogCaptureExtension;
import org.xwiki.test.junit5.mockito.ComponentTest;
import org.xwiki.test.junit5.mockito.InjectMockComponents;
import org.xwiki.test.junit5.mockito.MockComponent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultJobStatusStore}.
 *
 * @version $Id$
 */
@ComponentTest
class DefaultJobStatusStoreTest
{
    private static final List<String> ID = List.of("id1", "id2");

    private static final String ID_STRING = "id1/id2";

    private static final String ERROR = "error";

    private static final String TYPE = "type";

    private static final String REMOTE_CACHE_ID = "job.status.remote";

    private static final int REMOTE_LIFESPAN = 10;

    @RegisterExtension
    private final LogCaptureExtension logCapture = new LogCaptureExtension(LogLevel.WARN);

    @InjectMockComponents
    private DefaultJobStatusStore store;

    @MockComponent
    private PersistentJobStatusStore persistentJobStatusStore;

    @MockComponent
    private CacheManager cacheManager;

    @MockComponent
    private JobManagerConfiguration configuration;

    private Cache<JobStatus> cache;

    private ExpiringMapCache<JobStatus> remoteCache;

    @BeforeComponent
    void before() throws Exception
    {
        when(this.configuration.getJobStatusCacheSize()).thenReturn(100);
        when(this.configuration.getJobStatusRemoteCacheLifespan()).thenReturn(REMOTE_LIFESPAN);
        this.cache = spy(new ExpiringMapCache<>(0));
        // The remote cache expires its entries after the configured lifespan, on a clock moved by the tests.
        doAnswer(invocation -> {
            CacheConfiguration configuration = invocation.getArgument(0);
            if (configuration != null && REMOTE_CACHE_ID.equals(configuration.getConfigurationId())) {
                this.remoteCache = new ExpiringMapCache<>(
                    ((LRUCacheConfiguration) configuration).getLRUEvictionConfiguration().getLifespan());
                return this.remoteCache;
            }
            return this.cache;
        }).when(this.cacheManager).createNewCache(any());
    }

    @Test
    void initializeWhenCacheCreationFails() throws CacheException
    {
        when(this.configuration.getJobStatusCacheSize()).thenReturn(100);
        when(this.cacheManager.createNewCache(any())).thenThrow(new CacheException(ERROR));

        assertThrows(Exception.class, () -> this.store.initialize());
    }

    @Test
    void getJobStatusWhenCacheHit()
    {
        JobStatus status = mock();

        when(this.cache.get(ID_STRING)).thenReturn(status);

        assertSame(status, this.store.getJobStatus(ID));

        verify(this.cache).get(ID_STRING);
        verifyNoInteractions(this.persistentJobStatusStore);
    }

    @Test
    void getJobStatusWhenCacheMissThenLoadedAndCached() throws Exception
    {
        JobStatus status = mock();

        when(this.cache.get(ID_STRING)).thenReturn(null);
        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(status);

        assertSame(status, this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore).loadJobStatus(ID);
        verify(this.cache).set(ID_STRING, status);
    }

    @Test
    void getJobStatusWhenCacheMissThenSecondCheckAvoidsRead()
    {
        JobStatus status = mock();

        when(this.cache.get(ID_STRING)).thenReturn(null, status);

        assertSame(status, this.store.getJobStatus(ID));

        verify(this.cache, times(2)).get(ID_STRING);
        verifyNoInteractions(this.persistentJobStatusStore);
    }

    @Test
    void getJobStatusWhenLoadFailsRemovesCacheAndLogs() throws Exception
    {
        when(this.cache.get(ID_STRING)).thenReturn(null);
        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenThrow(new IOException(ERROR));

        assertNull(this.store.getJobStatus(ID));

        verify(this.cache).remove(ID_STRING);
        verify(this.persistentJobStatusStore, never()).loadRemoteJobStatus(any());
        assertEquals("Failed to load job status for id [[id1, id2]]: [IOException: error]", logCapture.getMessage(0));
    }

    @Test
    void initializeCreatesShortLivedRemoteCache() throws Exception
    {
        ArgumentCaptor<CacheConfiguration> captor = ArgumentCaptor.captor();
        verify(this.cacheManager, times(2)).createNewCache(captor.capture());

        LRUCacheConfiguration remoteConfiguration = (LRUCacheConfiguration) captor.getAllValues().get(1);
        assertEquals(REMOTE_CACHE_ID, remoteConfiguration.getConfigurationId());
        assertEquals(100, remoteConfiguration.getLRUEvictionConfiguration().getMaxEntries());
        assertEquals(REMOTE_LIFESPAN, remoteConfiguration.getLRUEvictionConfiguration().getLifespan());
    }

    @Test
    void getJobStatusWhenOnlyStoredByAnotherMember() throws Exception
    {
        JobStatus remoteStatus = mock();

        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(remoteStatus);

        assertSame(remoteStatus, this.store.getJobStatus(ID));
        assertSame(remoteStatus, this.store.getJobStatus(ID));

        // The main cache only holds the local absence, the status of the other member is in the remote cache.
        assertNotNull(this.cache.get(ID_STRING));
        assertNotSame(remoteStatus, this.cache.get(ID_STRING));
        assertSame(remoteStatus, this.remoteCache.get(ID_STRING));
        verify(this.persistentJobStatusStore).loadJobStatus(ID);
        verify(this.persistentJobStatusStore).loadRemoteJobStatus(ID);
    }

    @Test
    void getJobStatusWhenNoMemberHasAStatusIsNotQueriedAgainDuringTheLifespan() throws Exception
    {
        JobStatus remoteStatus = mock();

        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(null, remoteStatus);

        for (int i = 0; i < 5; ++i) {
            assertNull(this.store.getJobStatus(ID));
        }
        this.remoteCache.advance(REMOTE_LIFESPAN - 1);
        assertNull(this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore).loadRemoteJobStatus(ID);

        // Once the lifespan is over, the status stored meanwhile by another member is found.
        this.remoteCache.advance(1);
        assertSame(remoteStatus, this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore, times(2)).loadRemoteJobStatus(ID);
        verify(this.persistentJobStatusStore).loadJobStatus(ID);
    }

    @Test
    void getJobStatusSeesANewerRemoteStatusAfterTheLifespan() throws Exception
    {
        JobStatus firstRemoteStatus = mock();
        JobStatus secondRemoteStatus = mock();

        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(firstRemoteStatus,
            secondRemoteStatus);

        assertSame(firstRemoteStatus, this.store.getJobStatus(ID));
        this.remoteCache.advance(REMOTE_LIFESPAN - 1);
        assertSame(firstRemoteStatus, this.store.getJobStatus(ID));
        this.remoteCache.advance(1);
        assertSame(secondRemoteStatus, this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore, times(2)).loadRemoteJobStatus(ID);
    }

    @Test
    void getJobStatusWhenRemoteCacheIsDisabled() throws Exception
    {
        when(this.configuration.getJobStatusRemoteCacheLifespan()).thenReturn(0);
        this.store.initialize();

        JobStatus remoteStatus = mock();
        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(null, remoteStatus);

        assertNull(this.store.getJobStatus(ID));
        assertSame(remoteStatus, this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore, times(2)).loadRemoteJobStatus(ID);
    }

    @Test
    void getJobStatusWhenRemoteLoadFails() throws Exception
    {
        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenThrow(new IOException(ERROR));

        assertNull(this.store.getJobStatus(ID));
        // The failure is cached as an absence: no second lookup and no second warning during the lifespan.
        assertNull(this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore).loadRemoteJobStatus(ID);
        assertEquals(1, this.logCapture.size());
        assertEquals("Failed to load the job status for id [[id1, id2]] stored by another cluster member: "
            + "[IOException: error]", this.logCapture.getMessage(0));
    }

    @Test
    void getJobStatusWhenStoredLocallyAfterRemoteRead() throws Exception
    {
        DefaultRequest request = new DefaultRequest();
        request.setId(ID);
        JobStatus localStatus = new DefaultJobStatus<>(TYPE, request, null, null, null);
        JobStatus remoteStatus = mock();

        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(remoteStatus);

        assertSame(remoteStatus, this.store.getJobStatus(ID));

        // The job is then executed by the current member: its own status replaces the cached absence and wins over
        // the cached status of the other member.
        this.store.store(localStatus);

        assertSame(localStatus, this.store.getJobStatus(ID));
        verify(this.persistentJobStatusStore).loadRemoteJobStatus(ID);
    }

    @Test
    void getJobStatusWhenLocalAbsenceIsEvicted() throws Exception
    {
        JobStatus remoteStatus = mock();

        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(remoteStatus);

        assertSame(remoteStatus, this.store.getJobStatus(ID));

        // The local absence leaves the main cache (LRU eviction): the local storage is read again, then the remote
        // cache answers.
        this.cache.remove(ID_STRING);
        assertSame(remoteStatus, this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore, times(2)).loadJobStatus(ID);
        verify(this.persistentJobStatusStore).loadRemoteJobStatus(ID);
    }

    @Test
    void getJobStatusAfterRemoveReturnsTheStatusOfAnotherMember() throws Exception
    {
        JobStatus localStatus = mock();
        JobStatus remoteStatus = mock();

        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(localStatus, (JobStatus) null);
        when(this.persistentJobStatusStore.loadRemoteJobStatus(ID)).thenReturn(remoteStatus);

        assertSame(localStatus, this.store.getJobStatus(ID));
        verify(this.persistentJobStatusStore, never()).loadRemoteJobStatus(any());

        // Only the status of the current member is removed.
        this.store.remove(ID);

        assertSame(remoteStatus, this.store.getJobStatus(ID));
    }

    @Test
    void getJobStatusWhenLoadReturnsNullCachesSentinel() throws Exception
    {
        when(this.cache.get(ID_STRING)).thenReturn(null);
        when(this.persistentJobStatusStore.loadJobStatus(ID)).thenReturn(null);

        assertNull(this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore).loadJobStatus(ID);

        // Verify a non-null sentinel was cached to avoid repeated disk reads for missing statuses.
        ArgumentCaptor<JobStatus> captor = ArgumentCaptor.captor();
        verify(this.cache).set(eq(ID_STRING), captor.capture());
        assertNotNull(captor.getValue());
    }

    @Test
    void getJobStatusWhenPendingWriteExists() throws Exception
    {
        ExecutorService executor = mock();
        ReflectionUtils.setFieldValue(this.store, "executorService", executor);

        DefaultRequest request = new DefaultRequest();
        request.setId(ID);
        request.setStatusSerialized(true);

        JobStatus status = new DefaultJobStatus<>(TYPE, request, null, null, null);

        // Store async to create a pending write (executor mock won't actually run the task).
        this.store.storeAsync(status);

        // Simulate cache eviction by making get() return null.
        when(this.cache.get(ID_STRING)).thenReturn(null);

        // getJobStatus should find the pending write instead of loading from disk.
        assertSame(status, this.store.getJobStatus(ID));

        verify(this.persistentJobStatusStore, never()).loadJobStatus(any());
        verify(this.persistentJobStatusStore, never()).loadRemoteJobStatus(any());
    }

    @Test
    void removeDelegatesAndEvictsCache() throws Exception
    {
        this.store.remove(ID);

        verify(this.persistentJobStatusStore).removeJobStatus(ID);
        verify(this.cache).remove(ID_STRING);
    }

    @Test
    void removeWhenPersistenceFailsKeepsCacheAndLogs() throws Exception
    {
        doThrow(new IOException(ERROR)).when(this.persistentJobStatusStore).removeJobStatus(ID);

        this.store.remove(ID);

        verify(this.cache, never()).remove(ID_STRING);
        assertEquals("Failed to remove job status for id [[id1, id2]]: [IOException: error]", logCapture.getMessage(0));
    }

    @Test
    void storeWithNullStatusDoesNothing()
    {
        this.store.store(null);

        verifyNoInteractions(this.cache);
        verifyNoInteractions(this.persistentJobStatusStore);
    }

    @Test
    void storeWithNullRequestDoesNothing()
    {
        JobStatus status = new DefaultJobStatus<>(TYPE, null, null, null, null);

        this.store.store(status);

        verifyNoInteractions(this.cache);
        verifyNoInteractions(this.persistentJobStatusStore);
    }

    @Test
    void storeWithNullIdDoesNothing()
    {
        JobStatus status = new DefaultJobStatus<>(TYPE, new DefaultRequest(), null, null, null);

        this.store.store(status);

        verifyNoInteractions(this.cache);
        verifyNoInteractions(this.persistentJobStatusStore);
    }

    @Test
    void storeSerializableSavesSynchronously() throws Exception
    {
        DefaultRequest request = new DefaultRequest();
        request.setId(ID);
        request.setStatusSerialized(true);

        JobStatus status = new DefaultJobStatus<>(TYPE, request, null, null, null);

        this.store.store(status);

        verify(this.cache).set(ID_STRING, status);
        verify(this.persistentJobStatusStore).saveJobStatus(status);
    }

    @Test
    void storeAsyncSerializableSchedulesSave() throws Exception
    {
        ExecutorService executor = mock();
        ReflectionUtils.setFieldValue(this.store, "executorService", executor);

        DefaultRequest request = new DefaultRequest();
        request.setId(ID);
        request.setStatusSerialized(true);

        JobStatus status = new DefaultJobStatus<>(TYPE, request, null, null, null);

        this.store.storeAsync(status);

        verify(this.cache).set(ID_STRING, status);

        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.captor();
        verify(executor).execute(runnableCaptor.capture());

        runnableCaptor.getValue().run();

        verify(this.persistentJobStatusStore).saveJobStatus(status);
    }

    @Test
    void storeSerializableWhenSaveFailsLogs() throws Exception
    {
        DefaultRequest request = new DefaultRequest();
        request.setId(ID);
        request.setStatusSerialized(true);

        JobStatus status = new DefaultJobStatus<>(TYPE, request, null, null, null);

        doThrow(new IOException(ERROR)).when(this.persistentJobStatusStore).saveJobStatus(status);

        this.store.store(status);

        assertEquals("Failed to save job status for id [[id1, id2]]: [IOException: error]", logCapture.getMessage(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void createLoggerTailDelegates(boolean readonly) throws Exception
    {
        LoggerTail loggerTail = mock();
        when(this.persistentJobStatusStore.createLoggerTail(ID, readonly)).thenReturn(loggerTail);

        try (LoggerTail actual = this.store.createLoggerTail(ID, readonly)) {
            assertSame(loggerTail, actual);
        }

        verify(this.persistentJobStatusStore).createLoggerTail(ID, readonly);
        verifyNoMoreInteractions(this.persistentJobStatusStore);
    }
}
