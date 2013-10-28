package org.apache.hadoop.mapred.workflow;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.JobInProgressListener;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobInProgress;


/**
 * The number of slots assigned to each workflow is proportional
 * to its MinPara. 
 *
 * The JobQueue is ordered based on slots/MinPara
 */
public class ParaWJInProgressListener 
    extends JobInProgressListener,
            WorkflowInProgressListener {

  static class JobSchedulingInfo {
    private WorkflowID wfid;
    private long startTime;
    private JobID id;

    public JobSchedulingInfo(JobInProgress jip) {
      this(jip.getStatus());
    }

    public JobSchedulingInfo(JobStatus status) {
      wfid = status.getWorkflowID();
      id = status.getJobID();
      startTime = status.getStartTime();
    }

    long getStartTime() {return startTime;}
    JobID getJobID() {return id};
    WorkflowID getWorkflowID() {return wfid;}

    @Override
    public boolean equals(Object obj) {
      if (null == obj || 
          obj.getClass() != JobSchedulingInfo.class) {
        return false;
      } else if (this == obj) {
        return true;
      } else if (obj instanceof JobSchedulingInfo) {
        JobSchedulingInfo that = (JobSchedulingInfo)obj;
        return (this.id.equals(that.id) &&
                this.startTime == that.startTime &&
                this.wfid == that.wfid);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int)(wfid.hashCode() * id.hashCode() + startTime);
    }
  }

  static final Comparator<JobSchedulingInfo> PARA_JOB_QUEUE_COMPARATOR
    = new Comparator<JobSchedulingInfo>() {
    public int compare(JobSchedulingInfo o1, JobSchedulingInfo o2) {
      return 0;
    }
  };

  private Map<JobSchedulingInfo, JobInProgress> jobQueue;

  public ParaWJInProgressListener() {
    this(new TreeMap<JobSchedulingInfo, 
                     JobInProgress>(PARA_JOB_QUEUE_COMPARATOR));
  }

  public Collection<JobInProgress> jobJobQueue() {
    return jobQueue.values();
  }
}
