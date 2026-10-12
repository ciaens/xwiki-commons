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

import org.xwiki.component.annotation.Role;
import org.xwiki.job.event.status.JobStatus;
import org.xwiki.logging.tail.LoggerTail;

/**
 * Persistent storage for job statuses. Internal role that is used by {@link DefaultPersistentJobStatusStore}.
 *
 * @version $Id$
 * @since 18.3.0RC1
 */
@Role
public interface PersistentJobStatusStore
{
    /**
     * Save the job status to the persistent storage.
     *
     * @param status the job status to save
     * @throws IOException when failing to save the job status
     */
    void saveJobStatus(JobStatus status) throws IOException;

    /**
     * Load the job status from the persistent storage.
     *
     * @param id the id of the job
     * @return the job status
     * @throws IOException when failing to load the job status
     */
    JobStatus loadJobStatus(List<String> id) throws IOException;

    /**
     * Load the most recent status of the job stored by another member of the cluster, when the storage is shared
     * between the cluster members. It is called by {@link DefaultJobStatusStore} only when {@link #loadJobStatus(List)}
     * found no status for the current member.
     * <p>
     * The other member can store a new version of the status at any time without notifying the current member, so the
     * caller only keeps the result (or its absence) in a short-lived cache, never with the statuses of the current
     * member. The returned status is the last one the other member stored, usually when the job ended: while that
     * member runs the same job again, it is the status of the previous run. It must be considered read-only.
     *
     * @param id the id of the job
     * @return the most recent status stored by another cluster member, or {@code null} if there is none or the
     * storage is not shared between the cluster members
     * @throws IOException when failing to load the job status
     * @since 18.9.0RC1
     */
    default JobStatus loadRemoteJobStatus(List<String> id) throws IOException
    {
        return null;
    }

    /**
     * Remove the job status from the persistent storage.
     *
     * @param id the id of the job
     * @throws IOException when failing to remove the job status
     */
    void removeJobStatus(List<String> id) throws IOException;

    /**
     * Create a {@link LoggerTail} instance for the given job. If {@code readonly} is true, this allows accessing the
     * existing job log, otherwise, a fresh log is created.
     *
     * @param jobId the identifier of the job
     * @param readonly true of the log is readonly
     * @return the {@link LoggerTail} instance
     */
    LoggerTail createLoggerTail(List<String> jobId, boolean readonly);
}
