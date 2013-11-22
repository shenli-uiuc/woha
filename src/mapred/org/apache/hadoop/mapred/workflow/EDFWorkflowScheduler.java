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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.WorkflowStatus;
import org.apache.hadoop.mapred.workflow.WJobStatus;

/**
 * A {@link TaskScheduler} that keeps jobs in a queue in priority order (EDF
 * by default).
 */
class EDFWorkflowScheduler extends TaskScheduler {

  public static final int ASSIGNED = 0;
  public static final int ASSIGNED_LOCAL = 1;
  public static final int ASSIGNED_NONLOCAL = 2;
  public static final int NOT_ASSIGNED = 3;
  public static final Log LOG = JobTracker.LOG;
  
  protected EDFWorkflowListener edfWorkflowListener;
  protected EagerTaskInitializationListener eagerTaskInitializationListener;
  private float padFraction;
  
  public EDFWorkflowScheduler() {
    this.edfWorkflowListener = new EDFWorkflowListener();
  }
  
  @Override
  public synchronized void start() throws IOException {
    super.start();
    taskTrackerManager.addJobInProgressListener(edfWorkflowListener);
    taskTrackerManager.addWorkflowInProgressListener(edfWorkflowListener);
    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(
        eagerTaskInitializationListener);
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (edfWorkflowListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          edfWorkflowListener);
      taskTrackerManager.removeWorkflowInProgressListener(
          edfWorkflowListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
    this.eagerTaskInitializationListener =
      new EagerTaskInitializationListener(conf);
  }

  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    // Check for JT safe-mode
    if (taskTrackerManager.isInSafeMode()) {
      LOG.info("JobTracker is in safe-mode, not scheduling any tasks.");
      return null;
    } 

    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus(); 
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();

    Collection<Object> queue = edfWorkflowListener.getQueue();
    Hashtable<JobID, JobInProgress> wjobs = 
                                      edfWorkflowListener.getWJobs();

    //
    // Get map + reduce counts for the current tracker.
    //
    final int trackerMapCapacity = taskTrackerStatus.getMaxMapSlots();
    final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceSlots();
    final int trackerRunningMaps = taskTrackerStatus.countMapTasks();
    final int trackerRunningReduces = taskTrackerStatus.countReduceTasks();

    // Assigned tasks
    List<Task> assignedTasks = new ArrayList<Task>();

    int availableMapSlots = trackerMapCapacity - trackerRunningMaps;

    int numLocalMaps = 0;
    int numNonLocalMaps = 0;
    
    // schedule maps
    int state;
    for (int i=0; i < availableMapSlots; ++i) {
      long startTime = System.currentTimeMillis();
      state = NOT_ASSIGNED;
      synchronized (queue) {
        for (Object obj : queue) {
          if (obj instanceof JobInProgress) {
            state = obtainNewMapTask((JobInProgress) obj, 
                                     assignedTasks,
                                     taskTrackerStatus,
                                     numTaskTrackers);
          } else {
            // it is a workflow
            WorkflowInProgress wip = (WorkflowInProgress) obj;
            WorkflowStatus wfStatus = wip.getStatus();
           
            //first check submitters
            for (String activeJobName : wfStatus.getActiveJobs()) {
              WJobStatus wJobStatus = 
                wfStatus.getWJobStatus(activeJobName);
              JobID submitterID = wJobStatus.getSubmitterID();

              if (null == submitterID) {
                // did not reveive the submitter of this wjob yet
                continue;
              }
              JobInProgress submitter = wjobs.get(submitterID);
              state = obtainNewMapTask(
                        submitter, assignedTasks, 
                        taskTrackerStatus, numTaskTrackers);
              if (NOT_ASSIGNED != state) {
                LOG.info("Shen Li edf log: submitter " 
                         + activeJobName + ", " +
                         assignedTasks.get(
                           assignedTasks.size() - 1).getTaskID());
                wfStatus.addScheduledWJob(activeJobName);
                break;
              }
            } 

            //check wjobs
            if (NOT_ASSIGNED == state) {

              // no available submitter map tasks
              // try to schedule wjob tasks
              HashSet<String> submittedJobs = 
                wfStatus.getSubmittedJobs();
              Hashtable<String, JobID> nameToID = 
                wfStatus.getNameToID();
              //TODO: synchronized on wfStatus?
              for (String name : submittedJobs) {
                JobID id = nameToID.get(name);

                if (null == id) {
                  LOG.info("Shen Li: Job " + name + " of workflow " +
                      wfStatus.getWorkflowID().toString()
                      + " in submitted set does not have"
                      + " a JobID");
                  continue;
                } else {
                  JobInProgress jip = wjobs.get(id);
                  state = obtainNewMapTask(jip, 
                      assignedTasks, 
                      taskTrackerStatus,
                      numTaskTrackers);
                }
                if (NOT_ASSIGNED != state) {
                  // got one task, don't iterate again, as I do not
                  // want to violate availableMapSlots
                  LOG.info("Shen Li edf log: map " + name + ", " +
                           assignedTasks.get(
                             assignedTasks.size() - 1).getTaskID());
                  break;
                }
              }
            }
          }

          if (ASSIGNED_LOCAL == state) {
            ++numLocalMaps;
          } else if (ASSIGNED_NONLOCAL == state) {
            ++numNonLocalMaps;
          }

          if (NOT_ASSIGNED != state) {
            break;
          }


        }

        if (NOT_ASSIGNED == state) {
          // no job has available map tasks for now
          break;
        }
      }
      long endTime = System.currentTimeMillis();
      long schedDelay = endTime - startTime;
      LOG.info("Shen Li edf log: schedDelay " + schedDelay);
    }
    int assignedMaps = assignedTasks.size();

    //
    // Same thing, but for reduce tasks
    //
    final int availableReduceSlots = 
                trackerReduceCapacity - trackerRunningReduces;

    for (int i = 0; i < availableReduceSlots; ++i) {
      state = NOT_ASSIGNED;
      synchronized (queue) {
        for (Object obj : queue) {
          if (obj instanceof JobInProgress) {
            state = obtainNewReduceTask(
                      (JobInProgress) obj,
                      assignedTasks,
                      taskTrackerStatus,
                      numTaskTrackers);
          } else {
            WorkflowInProgress wip = (WorkflowInProgress) obj;
            WorkflowStatus wfStatus = wip.getStatus();
            HashSet<String> submittedJobs = wfStatus.getSubmittedJobs();
            Hashtable<String, JobID> nameToID = wfStatus.getNameToID();
            for (String name : submittedJobs) {
              JobID id = nameToID.get(name);

              if (null == id) {
                // This should not happend
                LOG.info("Shen Li: job " + name + " of Workflow " +
                         wfStatus.getWorkflowID().toString() +
                         " in submittedJobs but does not have a Job ID");
                continue;
              } else {
                JobInProgress jip = wjobs.get(id);
                state = obtainNewReduceTask(
                          jip,
                          assignedTasks,
                          taskTrackerStatus,
                          numTaskTrackers);
              }
              if (ASSIGNED == state) {
                LOG.info("Shen Li edf log: reduce " + name + ", " +
                         assignedTasks.get(
                           assignedTasks.size() - 1).getTaskID());
                break;
              }
            }
          }

          if (ASSIGNED == state) {
            break;
          }
        }
        
        if (NOT_ASSIGNED == state) {
          // no job has available reduce tasks for now
          break;
        }
      }
    }
  
    if (LOG.isDebugEnabled()) {
      LOG.info("Shen Li: Task assignments for " 
               + taskTrackerStatus.getTrackerName() + " --> " +
               "[" + trackerMapCapacity + ", " +  trackerRunningMaps + "] -> [" + 
               (trackerMapCapacity - trackerRunningMaps) + ", " +
               assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
               ")] [" + trackerReduceCapacity + ", " + trackerRunningReduces + 
               "] -> [" + (trackerReduceCapacity - trackerRunningReduces) + 
              ", " + (assignedTasks.size()-assignedMaps) + "]");
    }


    return assignedTasks;
  }

  /**
   * @return assigned local map, non-local map or not assigned
   */
  private synchronized int obtainNewMapTask(
                             JobInProgress jip,
                             List<Task> assignedTasks,
                             TaskTrackerStatus taskTrackerStatus,
                             int numTaskTrackers) throws IOException {

    if (jip.getStatus().getRunState() != JobStatus.RUNNING) {
      return NOT_ASSIGNED;
    }

    // assign local task (node or rack)
    Task t = null;
    t = jip.obtainNewNodeOrRackLocalMapTask(
              taskTrackerStatus,
              numTaskTrackers,
              taskTrackerManager.getNumberOfUniqueHosts());

    if (null != t) {
      assignedTasks.add(t);
      return ASSIGNED_LOCAL;
    }

    // assign non-local task
    t = jip.obtainNewNonLocalMapTask(
              taskTrackerStatus,
              numTaskTrackers,
              taskTrackerManager.getNumberOfUniqueHosts());

    if (null != t) {
      assignedTasks.add(t);
      return ASSIGNED_NONLOCAL;
    }

    return NOT_ASSIGNED;
  }

  private synchronized int obtainNewReduceTask(
                             JobInProgress jip,
                             List<Task> assignedTasks,
                             TaskTrackerStatus taskTrackerStatus,
                             int numTaskTrackers) throws IOException {

    if (jip.getStatus().getRunState() != JobStatus.RUNNING) {
      return NOT_ASSIGNED;
    }

    Task t = null;
    t = jip.obtainNewReduceTask(
              taskTrackerStatus,
              numTaskTrackers,
              taskTrackerManager.getNumberOfUniqueHosts());

    if (null != t) {
      assignedTasks.add(t);
      return ASSIGNED;
    } else {
      return NOT_ASSIGNED;
    }
  }

  /**
   * This will be called by the JobTracker. Here I put together all jobs
   * and wjobs.
   */
  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    ArrayList<JobInProgress> jobs = new ArrayList<JobInProgress>();

    Collection<Object> queue = edfWorkflowListener.getQueue();
    Hashtable<JobID, JobInProgress> wjobs = 
                               edfWorkflowListener.getWJobs();
    for (Object obj : queue) {
      if (obj instanceof JobInProgress) {
        jobs.add((JobInProgress) obj);
      } else {
        WorkflowInProgress wip = (WorkflowInProgress)obj;
        Hashtable<JobID, String> idToName = wip.getStatus().getIDToName();
        for (JobID id : idToName.keySet()) {
          jobs.add(wjobs.get(id));
        }
      }
    }

    return jobs;
  }  
}
