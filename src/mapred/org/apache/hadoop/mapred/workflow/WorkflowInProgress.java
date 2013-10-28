package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobTracker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Hadoop internal data structure that tracks workflow
 * progress
 */
public class WorkflowInProgress {

  public static final Log LOG = 
    LogFactory.getLog(JobTracker.class);

  private WorkflowStatus status;
  private WorkflowProfile profile;
  private WorkflowConf conf;
  private WorkflowID wfid;
  private long submitTime;
  private long slotNum;

  public WorkflowInProgress(JobTracker jt, 
                            WorkflowConf conf,
                            WorkflowInfo info) 
      throws IOException, InterruptedException {
    this.wfid = info.getWorkflowID();
    this.submitTime = jt.getClock().getTime();
    this.status = new WorkflowStatus(wfid, this.submitTime,
                                     conf,
                                     WorkflowStatus.PREP);
    // TODO: implement workflowdetails.jsp
    String url = "http://" + jt.getJobTrackerMachine() + ":"
                 + jt.getInfoPort() + 
                 "/workflowdetails.jsp?wfid=" + this.wfid;
    this.conf = conf;
    this.profile = new WorkflowProfile(info.getUser().toString(),
                                       info.getWorkflowID(),
                                       url, 
                                       conf.getName());
  }

  public synchronized void setSlotNum(long slotNum) {
    this.slotNum = slotNum;
  }

  public long getSlotNum() {
    return this.slotNum;
  }

  public WorkflowStatus getStatus() {
    return status;
  }
  
  public WorkflowConf getConf() {
    return conf;
  }

  public WorkflowProfile getProfile() {
    return profile;
  }
}
