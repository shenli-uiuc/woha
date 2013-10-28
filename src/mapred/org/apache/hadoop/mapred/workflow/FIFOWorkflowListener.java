package org.apache.hadoop.mapred;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Hashtable;

import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;

import org.apache.hadoop.mapred.workflow.WorkflowInProgressListener;
import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.WorkflowChangeEvent;
import org.apache.hadoop.mapred.workflow.WorkflowStatus;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WJobStatus;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link FIFOWorkflowListener} that maintains the jobs and workflows
 * being managed in a queue in FIFO order.
 */
class FIFOWorkflowListener extends JobInProgressListener 
      implements WorkflowInProgressListener{

  public static final Log LOG = JobTracker.LOG;
  /**
   * Super class for both WorkflowSchedulingInfo and 
   * JobSchedulingInfo
   */
  static class SchedulingInfo {
    private long startTime;

    public SchedulingInfo(long startTime) {
      this.startTime = startTime;
    }

    public long getStartTime() {
      return startTime;
    }

    @Override
    public boolean equals(Object obj) {
      if (null == obj || obj.getClass() != SchedulingInfo.class) {
        return false;
      } else if (this == obj) {
        return true;
      } else if (obj instanceof SchedulingInfo) {
        SchedulingInfo that = (SchedulingInfo) obj;
        return (this.startTime == that.startTime);
      }

      return false;
    }
  }

  static class WorkflowSchedulingInfo extends SchedulingInfo{
    private WorkflowID wfid;

    public WorkflowSchedulingInfo(WorkflowStatus status) {
      super(status.getSubmitTime());
      this.wfid = status.getWorkflowID();
    }
   
    public WorkflowID getWorkflowID() {
      return this.wfid;
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }

      if (null == obj || obj.getClass() != WorkflowSchedulingInfo.class) {
        return false;
      } else if (this == obj) {
        return true;
      } else if (obj instanceof WorkflowSchedulingInfo) {
        WorkflowSchedulingInfo that = (WorkflowSchedulingInfo) obj;
        return this.wfid.equals(that.wfid);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return (int)(wfid.hashCode() * getStartTime());
    }

  }

  /** A class that groups all the information from a {@link JobInProgress} that 
   * is necessary for scheduling a job.
   */ 
  static class JobSchedulingInfo extends SchedulingInfo{
    private JobPriority priority;
    private JobID id;

    public JobSchedulingInfo(JobInProgress jip) {
      this(jip.getStatus());
    }

    public JobSchedulingInfo(JobStatus status) {
      super(status.getStartTime());
      priority = status.getJobPriority();
      id = status.getJobID();
    }

    JobPriority getPriority() {return priority;}
    JobID getJobID() {return id;}
  
    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
  
      if (obj == null || obj.getClass() != JobSchedulingInfo.class) {
        return false;
      } else if (obj == this) {
        return true;
      }
      else if (obj instanceof JobSchedulingInfo) {
        JobSchedulingInfo that = (JobSchedulingInfo)obj;
        return (this.id.equals(that.id) && 
            this.priority == that.priority);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int)(id.hashCode() * priority.hashCode() + getStartTime());
    }

  }

  static final Comparator<SchedulingInfo> FIFO_QUEUE_COMPARATOR = 
  new Comparator<SchedulingInfo>() {
    public int compare(SchedulingInfo o1, SchedulingInfo o2) {
      if (o1.getStartTime() < o2.getStartTime()) {
        return -1;
      } else if(o1.getStartTime() > o2.getStartTime()) {
        return 1;
      } else {
        return 0;
      }
    }
  };
 
  //TODO: we only need a hashtable rather than a TreeMap, as:
  //1. compare workflows to pickout the one with the smallest
  //   start time.
  //2. the workflow determines which job goes first

  // this hashtable contains only the information of jobs that belong
  // to a workflow
  private Hashtable<JobID, JobInProgress> wjobs;
  private Hashtable<WorkflowID, WorkflowSchedulingInfo> wsis;
  //TODO: separate active Queue and inactive queue to accelerate 
  //the task assignment process.

  // contains both job and workflow sorted in FIFO order of startTime
  private Map<SchedulingInfo, Object> queue;

  public FIFOWorkflowListener() {
    this(new TreeMap<SchedulingInfo,
                     Object>(FIFO_QUEUE_COMPARATOR));
  }

  public Hashtable<JobID, JobInProgress> getWJobs() {
    return wjobs;
  }

  /**
   * For clients that want to provide their own job priorities.
   * @param jobQueue A collection whose iterator returns jobs in priority order.
   */
  protected FIFOWorkflowListener(Map<SchedulingInfo, 
                                     Object> queue) {
    this.queue = Collections.synchronizedMap(queue);
    this.wjobs = new Hashtable<JobID, JobInProgress> ();
    this.wsis = 
      new Hashtable<WorkflowID, WorkflowSchedulingInfo> ();
  }

  /**
   * Returns a synchronized view of the job queue.
   */
  public Collection<Object> getQueue() {
    return queue.values();
  }
  
  @Override
  public void jobAdded(JobInProgress job) {
    WorkflowID wfid = job.getStatus().getWorkflowID();
    if (null == wfid) {
      queue.put(new JobSchedulingInfo(job.getStatus()), job);
    } else {
      // it is a wjob
      String name = job.getJobName();

      WorkflowInProgress wip = 
        (WorkflowInProgress)queue.get(wsis.get(wfid));
      WorkflowStatus wfStatus = wip.getStatus();
      if (WJobStatus.isSubmitter(name)) {
        // the job is submitter
        
        // corresponding wjob name
        String wJobName = WJobStatus.getWJobName(name);
        WJobStatus wJobStatus = 
          wfStatus.getWJobStatus(wJobName);
        if (wJobStatus.setSubmitterID(job.getJobID())) {
          // do not add the job, if the submitter has been
          // added before.
          wjobs.put(job.getJobID(), job);
          LOG.info("Shen Li: receive submitter " + name +
            " for workflow " + wfid.toString() + ": " +
            job.getJobID().toString());
        } else {
          job.kill();
          LOG.info("Shen Li: Already seen submitter " + name + 
            " for workflow " + wfid.toString()
            + ", the current submission " 
            + job.getJobID().toString() + " has been killed.");
        }
      } else {
        if (wip.getStatus().addSubmittedWJob(
                              name, 
                              job.getJobID())) {
          // do not add submitted wjobs
          wjobs.put(job.getJobID(), job);
        } else {
          job.kill();
          LOG.info("Shen Li: Already seen wjob " + name +
            " for workflow " + wfid.toString() + "in " +
            wip.getStatus().getNameToID().get(name).toString()
            + ", the current submission " 
            + job.getJobID().toString() + " has been killed.");
        }
      }
    }
  }

  @Override
  public void workflowAdded(WorkflowInProgress wf) {
    WorkflowSchedulingInfo wsi = new WorkflowSchedulingInfo(wf.getStatus());

    wsis.put(wf.getStatus().getWorkflowID(), wsi);
    queue.put(wsi, wf);
    LOG.info("workflow Added : " + wf.getConf().getName()
             + ", queue size " + queue.size());
  }

  // Job will be removed once the job completes
  @Override
  public void jobRemoved(JobInProgress job) {}
 
  @Override
  public void workflowRemoved(WorkflowInProgress wf) {}

  private void jobCompleted(JobSchedulingInfo oldInfo) {
    queue.remove(oldInfo);
  }

  private void wJobCompleted(WorkflowID wfid, 
                             JobSchedulingInfo oldInfo) {
    WorkflowInProgress wip = 
      (WorkflowInProgress) queue.get(wsis.get(wfid));
    wip.getStatus().addFinishedWJob(oldInfo.getJobID());
    if (wip.getStatus().isCompleted()) {
      workflowCompleted(wsis.get(wfid));
      wsis.remove(wfid);
    }
    wjobs.remove(oldInfo.getJobID());
  }

  private void workflowCompleted(WorkflowSchedulingInfo oldInfo) {
    LOG.info("Shen Li: workflow completed " +
        oldInfo.getWorkflowID().toString());
    queue.remove(oldInfo);
  }
  
  @Override
  public synchronized void jobUpdated(JobChangeEvent event) {
    JobInProgress job = event.getJobInProgress();
    if (event instanceof JobStatusChangeEvent) {
      WorkflowID wfid = job.getStatus().getWorkflowID();
      JobStatusChangeEvent statusEvent = (JobStatusChangeEvent)event;
      JobSchedulingInfo oldInfo = 
        new JobSchedulingInfo(statusEvent.getOldStatus());
      if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
        int runState = statusEvent.getNewStatus().getRunState();
        if (JobStatus.SUCCEEDED == runState ||
            JobStatus.FAILED == runState ||
            JobStatus.KILLED == runState) {
          if (null == wfid) {
            jobCompleted(oldInfo);
          } else {
            wJobCompleted(wfid, oldInfo);
          }
        }
      }
    }
  }
 
  /**
   * For now, job tracker does not notify the task scheduler about 
   * workflow status changes. Job tracker only keeps track of job status
   * chagnes. The task scheduler needs to figure out whether a Workflow is
   * finished by itself.
   *
   * TODO: users may issue a kill-workflow signal.
   */
  @Override
  public synchronized void workflowUpdated(WorkflowChangeEvent wfe) {
  }

}
