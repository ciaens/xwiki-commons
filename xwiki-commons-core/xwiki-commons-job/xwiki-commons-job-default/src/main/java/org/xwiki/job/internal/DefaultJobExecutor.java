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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.classloader.ClassLoaderManager;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.DisposePriority;
import org.xwiki.component.manager.ComponentLifecycleException;
import org.xwiki.component.manager.ComponentLookupException;
import org.xwiki.component.manager.ComponentManager;
import org.xwiki.component.phase.Disposable;
import org.xwiki.component.phase.Initializable;
import org.xwiki.component.phase.InitializationException;
import org.xwiki.job.GroupedJob;
import org.xwiki.job.GroupedJobInitializer;
import org.xwiki.job.GroupedJobInitializerManager;
import org.xwiki.job.Job;
import org.xwiki.job.JobException;
import org.xwiki.job.JobExecutor;
import org.xwiki.job.JobGroupPath;
import org.xwiki.job.JobManagerConfiguration;
import org.xwiki.job.Request;
import org.xwiki.job.event.status.CancelableJobStatus;
import org.xwiki.job.event.status.JobStatus;

/**
 * Default implementation of {@link JobExecutor}.
 * <p>
 * The executor is disposed before the other components (see {@link #DISPOSE_PRIORITY}): the jobs it waits for
 * during the shutdown need the stores, the caches, the event listeners and the logging to be still alive, otherwise a
 * job keeps running in a half-disposed system and finishes with silently missing side effects.
 *
 * @version $Id$
 * @since 6.1M2
 */
@Component
@Singleton
@DisposePriority(DefaultJobExecutor.DISPOSE_PRIORITY)
public class DefaultJobExecutor implements JobExecutor, Initializable, Disposable
{
    /**
     * The dispose priority of the executor, lower than any other component's so that the running jobs are waited for
     * while the rest of the system is intact.
     *
     * @since 18.9.0RC1
     */
    public static final int DISPOSE_PRIORITY = 100;

    /**
     * Logger dedicated to shutdown information (same logger name as in other components).
     */
    private static final Logger SHUTDOWN_LOGGER = LoggerFactory.getLogger("org.xwiki.shutdown");

    /**
     * The time in milliseconds given to interrupted jobs to finish (and store their status) after they have been
     * interrupted because the shutdown timeout expired.
     */
    private static final long INTERRUPT_GRACE_TIME = 5000L;

    private static final String DISPOSED_MESSAGE = "The job executor is disposed";

    private class JobGroupExecutor extends JobThreadExecutor implements ThreadFactory
    {
        private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

        private final JobGroupPath path;

        private final String groupThreadName;

        private final GroupedJobInitializer initializer;

        JobGroupExecutor(JobGroupPath path, GroupedJobInitializer initializer)
        {
            super(initializer.getPoolSize(), initializer.getPoolSize(),
                DefaultJobExecutor.this.jobManagerConfiguration.getGroupedJobThreadKeepAliveTime(),
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

            // We want to be sure to not keep idle threads.
            this.allowCoreThreadTimeOut(true);
            this.initializer = initializer;
            setThreadFactory(this);

            this.path = path;
            this.groupThreadName = this.path + " job group daemon thread";
        }

        @Override
        protected String getThreadName(Runnable r)
        {
            return this.groupThreadName + " - " + r;
        }

        @Override
        protected String getExecutorThreadName(Runnable r)
        {
            return this.groupThreadName;
        }

        @Override
        protected void beforeExecute(Thread t, Runnable r)
        {
            // Register the job before waiting for the group lock (which cannot be interrupted) so that it's taken into
            // account by #dispose()
            registerJob(r);

            DefaultJobExecutor.this.lockTree.lock(this.path);

            super.beforeExecute(t, r);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t)
        {
            DefaultJobExecutor.this.lockTree.unlock(this.path);

            Job job = (Job) r;

            super.afterExecute(r, t);

            List<String> jobId = job.getRequest().getId();
            if (jobId != null) {
                // Delete the job from the job group's queue. Remove the queue when it is empty.
                // Use computeIfPresent for synchronization.
                DefaultJobExecutor.this.groupedJobs.computeIfPresent(jobId, (k, v) -> {
                    // Remove the job, but only when it is the first job in the queue.
                    if (v.peek() == job) {
                        v.poll();
                    }

                    // If the queue is empty, remove it from the map.
                    if (v.isEmpty()) {
                        return null;
                    }

                    // Otherwise, keep the (non-empty) queue.
                    return v;
                });
            }
        }

        @Override
        public Thread newThread(Runnable r)
        {
            Thread thread = this.threadFactory.newThread(r);

            thread.setDaemon(true);
            thread.setName(this.groupThreadName);
            thread.setPriority(this.initializer.getDefaultPriority());

            return thread;
        }
    }

    private class JobThreadExecutor extends ThreadPoolExecutor
    {
        /**
         * The jobs currently executed by this executor, including those waiting for their group lock.
         */
        private final Set<Job> currentJobs =
            Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

        JobThreadExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue)
        {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        /**
         * @param r the job given to a thread of this executor
         */
        void registerJob(Runnable r)
        {
            this.currentJobs.add((Job) r);
        }

        /**
         * @return a copy of the set of jobs currently executed by this executor
         */
        List<Job> getCurrentJobs()
        {
            // As this is a synchronized set, we need to explicitly synchronize on it for the iteration.
            synchronized (this.currentJobs) {
                return List.copyOf(this.currentJobs);
            }
        }

        protected String getThreadName(Runnable r)
        {
            return r.toString();
        }

        protected String getExecutorThreadName(Runnable r)
        {
            return "Unused job pool thread";
        }

        @Override
        protected void beforeExecute(Thread t, Runnable r)
        {
            registerJob(r);

            // Set a custom thread name corresponding to the job to make debugging easier
            Thread.currentThread().setName(getThreadName(r));

            // Make sure to set a clean classloader
            Thread.currentThread().setContextClassLoader(classloaderManager.getURLClassLoader(null, false));
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t)
        {
            Job job = (Job) r;

            this.currentJobs.remove(job);

            List<String> jobId = job.getRequest().getId();
            if (jobId != null) {
                // Remove the job from the jobs map if its job ID actually maps to the job instance.
                // Use computeIfPresent for synchronization.
                DefaultJobExecutor.this.jobs.computeIfPresent(jobId, (k, v) -> {
                    if (v == job) {
                        // If the job ID maps to the job instance, remove it from the map.
                        return null;
                    }

                    // If the job ID does not map to the job instance, keep it in the map.
                    return v;
                });
            }

            // Reset thread name since it's not used anymore
            Thread.currentThread().setName(getExecutorThreadName(r));
        }
    }

    /**
     * Used to lookup {@link Job} implementations.
     */
    @Inject
    @Named("context")
    private Provider<ComponentManager> componentManager;

    @Inject
    private JobManagerConfiguration jobManagerConfiguration;

    @Inject
    private GroupedJobInitializerManager groupedJobInitializerManager;

    @Inject
    private ClassLoaderManager classloaderManager;

    private final Map<List<String>, Queue<Job>> groupedJobs = new ConcurrentHashMap<>();

    private final Map<List<String>, Job> jobs = new ConcurrentHashMap<>();

    /**
     * Handle care of hierarchical locking for grouped jobs.
     */
    @Inject
    private JobGroupPathLockTree lockTree;

    /**
     * Map&lt;groupname, group executor&gt;.
     */
    private final Map<JobGroupPath, JobGroupExecutor> groupExecutors = new ConcurrentHashMap<>();

    /**
     * Execute non grouped jobs.
     */
    private JobThreadExecutor jobExecutor;

    private volatile boolean disposed;

    @Override
    public void initialize() throws InitializationException
    {
        this.jobExecutor =
            new JobThreadExecutor(0, Integer.MAX_VALUE, this.jobManagerConfiguration.getSingleJobThreadKeepAliveTime(),
                TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    }

    @Override
    public void dispose() throws ComponentLifecycleException
    {
        List<JobThreadExecutor> executors;
        // Same monitor as #executeGroupedJob() so that no group executor is created after the snapshot
        synchronized (this.groupExecutors) {
            this.disposed = true;

            executors = new ArrayList<>(this.groupExecutors.size() + 1);
            executors.add(this.jobExecutor);
            executors.addAll(this.groupExecutors.values());
        }

        // Stop accepting new jobs and take the jobs which have not started yet out of the queues: it's too late for
        // them
        List<Runnable> queuedJobs = new ArrayList<>();
        for (JobThreadExecutor executor : executors) {
            executor.shutdown();
            executor.getQueue().drainTo(queuedJobs);
        }
        dropQueuedJobs(queuedJobs);

        // Give the running jobs a chance to finish properly
        long timeout = this.jobManagerConfiguration.getShutdownTimeout();
        List<Job> runningJobs = getRunningJobs(executors);
        if (timeout > 0 && !runningJobs.isEmpty()) {
            SHUTDOWN_LOGGER.info("Waiting up to [{}]ms for the following [{}] job(s) to finish: {}", timeout,
                runningJobs.size(), toJobIds(runningJobs));

            awaitTermination(executors, System.currentTimeMillis() + timeout);

            runningJobs = getRunningJobs(executors);
        }

        // Interrupt the jobs which are still running
        if (!runningJobs.isEmpty()) {
            SHUTDOWN_LOGGER.warn("Interrupting the following [{}] job(s) still running after [{}]ms: {}",
                runningJobs.size(), timeout, toJobIds(runningJobs));

            interruptJobs(executors, runningJobs);

            // Give the interrupted jobs a bit of time to store their status (unless waiting is disabled)
            if (timeout > 0) {
                awaitTermination(executors, System.currentTimeMillis() + INTERRUPT_GRACE_TIME);

                if (!executors.stream().allMatch(JobThreadExecutor::isTerminated)) {
                    SHUTDOWN_LOGGER.warn("Some jobs are still running [{}]ms after being interrupted: {}",
                        INTERRUPT_GRACE_TIME, toJobIds(getRunningJobs(executors)));
                }
            }
        }
    }

    /**
     * Cancel the cancelable jobs and interrupt all the threads of the passed executors.
     *
     * @param executors the executors to interrupt
     * @param runningJobs the jobs currently executed by the executors
     */
    private void interruptJobs(List<JobThreadExecutor> executors, List<Job> runningJobs)
    {
        // Let cancelable jobs know they are expected to stop
        for (Job job : runningJobs) {
            JobStatus status = job.getStatus();
            if (status instanceof CancelableJobStatus cancelableStatus && cancelableStatus.isCancelable()) {
                cancelableStatus.cancel();
            }
        }

        for (JobThreadExecutor executor : executors) {
            executor.shutdownNow();
        }
    }

    /**
     * Release the callers of jobs which are never going to be executed.
     *
     * @param queuedJobs the jobs which were waiting for a thread
     */
    private void dropQueuedJobs(List<Runnable> queuedJobs)
    {
        if (!queuedJobs.isEmpty()) {
            List<Job> droppedJobs = queuedJobs.stream().map(Job.class::cast).toList();

            SHUTDOWN_LOGGER.warn("The following [{}] job(s) were waiting to be executed and are dropped: {}",
                droppedJobs.size(), toJobIds(droppedJobs));

            for (Job job : droppedJobs) {
                if (job instanceof org.xwiki.job.AbstractJob<?, ?> abstractJob) {
                    // Mark the job finished (with an error) and store its status so that the callers waiting for it
                    // are released and a trace of it is kept. The error is caused by an interruption, like for the
                    // running jobs interrupted by the shutdown, so that both are reported the same way.
                    abstractJob.abort(new JobException("The job has not been executed because XWiki is stopping",
                        new InterruptedException("XWiki is stopping")));
                }
            }
        }
    }

    private List<Job> getRunningJobs(List<JobThreadExecutor> executors)
    {
        List<Job> runningJobs = new ArrayList<>();

        for (JobThreadExecutor executor : executors) {
            runningJobs.addAll(executor.getCurrentJobs());
        }

        return runningJobs;
    }

    /**
     * @param jobs the jobs
     * @return the identifiers of the passed jobs, or their type for the jobs without identifier
     */
    private List<Object> toJobIds(List<Job> jobs)
    {
        return jobs.stream().map(job -> {
            List<String> id = job.getRequest() != null ? job.getRequest().getId() : null;

            return id != null ? id : "<" + job.getType() + '>';
        }).toList();
    }

    /**
     * Wait for the passed executors to terminate, until the passed deadline. The deadline is shared by all the
     * executors: the executors are waited for one after the other, but the total time spent is bounded by the deadline.
     *
     * @param executors the executors to wait for
     * @param deadline the time in milliseconds (see {@link System#currentTimeMillis()}) at which to stop waiting
     */
    private void awaitTermination(List<JobThreadExecutor> executors, long deadline)
    {
        try {
            for (JobThreadExecutor executor : executors) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0 || !executor.awaitTermination(remaining, TimeUnit.MILLISECONDS)) {
                    // Too late, don't wait for the other executors (the running jobs are reported by the caller)
                    return;
                }
            }
        } catch (InterruptedException e) {
            SHUTDOWN_LOGGER.warn("Interrupted while waiting for the running jobs to finish");

            Thread.currentThread().interrupt();
        }
    }

    // JobManager

    @Override
    public Job getCurrentJob(JobGroupPath groupPath)
    {
        return getCurrentJobs(groupPath).stream().findFirst().orElse(null);
    }

    @Override
    public List<Job> getCurrentJobs(JobGroupPath path)
    {
        JobGroupExecutor executor = this.groupExecutors.get(path);

        if (executor != null) {
            // Return an unmodifiable copy of the set of currently running jobs.
            return executor.getCurrentJobs();
        }

        return Collections.emptyList();
    }

    @Override
    public Job getJob(List<String> id)
    {
        // Is it a standalone job
        Job job = this.jobs.get(id);
        if (job != null) {
            return job;
        }

        // Is it in a group
        Queue<Job> jobQueue = this.groupedJobs.get(id);
        if (jobQueue != null) {
            return jobQueue.peek();
        }

        return null;
    }

    /**
     * @param jobType the job id
     * @param request the request
     * @return a new job
     * @throws JobException failed to create a job for the provided type
     */
    private Job createJob(String jobType, Request request) throws JobException
    {
        Job job;
        try {
            job = this.componentManager.get().getInstance(Job.class, jobType);
        } catch (ComponentLookupException e) {
            throw new JobException("Failed to lookup any Job for role hint [" + jobType + "]", e);
        }

        job.initialize(request);

        return job;
    }

    @Override
    public Job execute(String jobType, Request request) throws JobException
    {
        Job job = createJob(jobType, request);

        execute(job);

        return job;
    }

    @Override
    public void execute(Job job)
    {
        if (!this.disposed) {
            if (job instanceof GroupedJob groupedJob) {
                executeGroupedJob(groupedJob);
            } else {
                executeSingleJob(job);
            }
        } else {
            throw new RejectedExecutionException(DISPOSED_MESSAGE);
        }
    }

    private void executeSingleJob(Job job)
    {
        List<String> jobId = job.getRequest().getId();
        if (jobId != null) {
            this.jobs.put(jobId, job);
        }

        try {
            this.jobExecutor.execute(job);
        } catch (Exception e) {
            // In case the job is rejected, remove the entry in the current jobs again.
            if (jobId != null) {
                this.jobs.computeIfPresent(jobId, (k, v) -> v == job ? null : v);
            }

            // Let the caller handle the exception.
            throw e;
        }
    }

    private void executeGroupedJob(GroupedJob job)
    {
        // While synchronization isn't necessary for the insertion in the group executors, this ensures that jobs in
        // the "groupedJobs" queues have the same order as in the executor's queue.
        synchronized (this.groupExecutors) {
            // Check again under the monitor so that no group executor is created after #dispose() took its snapshot
            if (this.disposed) {
                throw new RejectedExecutionException(DISPOSED_MESSAGE);
            }

            JobGroupPath path = job.getGroupPath();

            // If path is null execute as non grouped job
            if (path == null) {
                executeSingleJob(job);

                return;
            }

            JobGroupExecutor groupExecutor = this.groupExecutors.computeIfAbsent(path,
                p -> new JobGroupExecutor(p, this.groupedJobInitializerManager.getGroupedJobInitializer(p)));

            List<String> jobId = job.getRequest().getId();
            if (jobId != null) {
                // Insert a new queue into the map and add the job to it.
                // Use compute() to synchronize on the job ID to prevent that another thread would remove the empty
                // queue again.
                this.groupedJobs.compute(jobId, (k, v) -> {
                    // If there is no queue for that ID, create a new one.
                    Queue<Job> queue = Objects.requireNonNullElseGet(v, ConcurrentLinkedQueue::new);
                    // Directly add the job to the queue before releasing the lock to ensure that all queues in the
                    // map are non-empty.
                    queue.offer(job);
                    return queue;
                });
            }

            // Execute the job only once it has been inserted in the groupedJobs to ensure that there is no race
            // condition when the job completes before it has been inserted into groupedJobs.
            try {
                groupExecutor.execute(job);
            } catch (Exception e) {
                // Remove the queued job again.
                if (jobId != null) {
                    this.groupedJobs.computeIfPresent(jobId, (k, v) -> {
                        // Remove the job object from the queue to ensure that queue and executor queue are in sync.
                        if (v.removeIf(j -> j == job) && v.isEmpty()) {
                            return null;
                        }

                        return v;
                    });
                }

                // Let the caller handle the exception.
                throw e;
            }
        }
    }
}
