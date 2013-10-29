package org.apache.hadoop.mapred;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;

import org.apache.hadoop.mapred.workflow.WorkflowInProgressListener;
import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.WorkflowChangeEvent;
import org.apache.hadoop.mapred.workflow.WorkflowStatus;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WJobStatus;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;
import org.apache.hadoop.mapred.workflow.SchedulingEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link PlanWorkflowListener} that maintains the jobs and workflows
 * being managed in a queue in the decreasing order of their progresses:
 * schedWork - requirement.
 */
class PlanWorkflowListener extends JobInProgressListener 
      implements WorkflowInProgressListener{

  public static final Log LOG = JobTracker.LOG;

  static final Comparator<Object> PROGRESS_COMPARATOR = 
  new Comparator<Object>() {

    /**
     * put jip before wip
     */
    public int compare(Object o1, Object o2) {
      long curTime = System.currentTimeMillis();
      if (o1 instanceof WorkflowInProgress &&
          o2 instanceof WorkflowInProgress) {
        WorkflowInProgress wip1 = (WorkflowInProgress) o1;
        WorkflowInProgress wip2 = (WorkflowInProgress) o2;
        long work1 = wip1.getStatus().getSchedWork();
        long work2 = wip2.getStatus().getSchedWork();
        long req1 = 
          wip1.getConf().getSchedulingPlan().getRequirement(curTime);
        long req2 = 
          wip2.getConf().getSchedulingPlan().getRequirement(curTime);
        long res1 = work1 - req1;
        long res2 = work2 - req2;

        if (res1 < res2) {
          return -1;
        } else if(res1 > res2) {
          return 1;
        } else {
          return 0;
        }
      } else if (o1 instanceof WorkflowInProgress &&
                 o2 instanceof JobInProgress) {
        return 1;
      } else if (o1 instanceof JobInProgress &&
                 o2 instanceof WorkflowInProgress) {
        return -1;
      } else {
        return 0;
      }
    }
  };
 
  private Hashtable<JobID, JobInProgress> wjobs;

  private Hashtable<WorkflowID, WorkflowInProgress> wfs;
  private Hashtable<JobID, JobInProgress> jobs; // that are not wjob

  public Hashtable<JobID, JobInProgress> getWJobs() {
    return wjobs;
  }

  protected PlanWorkflowListener() {
    this.wjobs = new Hashtable<JobID, JobInProgress> ();
    this.wfs = new Hashtable<WorkflowID, WorkflowInProgress> ();
    this.jobs = new Hashtable<JobID, JobInProgress> ();
  }


  public synchronized Collection<Object> getQueue() {
    TreeSet<Object> queue = new TreeSet<Object>(PROGRESS_COMPARATOR);
    queue.addAll(wfs.values());
    queue.addAll(jobs.values());
    return queue;
  }
  
  @Override
  public void jobAdded(JobInProgress job) {
    WorkflowID wfid = job.getStatus().getWorkflowID();
    if (null == wfid) {
      jobs.put(job.getJobID(), job);
    } else {
      // it is a wjob
      String name = job.getJobName();

      WorkflowInProgress wip = wfs.get(wfid);
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
    wfs.put(wf.getStatus().getWorkflowID(), wf);
    LOG.info("workflow Added : " + wf.getConf().getName());
  }

  // Job will be removed once the job completes
  @Override
  public void jobRemoved(JobInProgress job) {}
 
  @Override
  public void workflowRemoved(WorkflowInProgress wf) {}

  private void jobCompleted(JobID id) {
    if (jobs.keySet().contains(id)){
      jobs.remove(id);
    } else if (wjobs.keySet().contains(id)){
      JobInProgress jip = wjobs.get(id);
      WorkflowID wfid = jip.getStatus().getWorkflowID();
      WorkflowInProgress wip = wfs.get(wfid);
      wip.getStatus().addFinishedWJob(id);
      if (wip.getStatus().isCompleted()) {
        workflowCompleted(wfid);
        wfs.remove(wfid);
      }
      wjobs.remove(id);
    }
  }

  private void workflowCompleted(WorkflowID wfid) {
    LOG.info("Shen Li: workflow completed " + wfid.toString());
    wfs.remove(wfid);
  }
  
  @Override
  public synchronized void jobUpdated(JobChangeEvent event) {
    JobInProgress job = event.getJobInProgress();
    if (event instanceof JobStatusChangeEvent) {
      JobID id = job.getJobID();
      JobStatusChangeEvent statusEvent = (JobStatusChangeEvent)event;
      if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
        int runState = statusEvent.getNewStatus().getRunState();
        if (JobStatus.SUCCEEDED == runState ||
            JobStatus.FAILED == runState ||
            JobStatus.KILLED == runState) {
          jobCompleted(id);
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
