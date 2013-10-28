package org.apache.hadoop.mapred.workflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobID;

/**
 * Keeps runtime information (i.e., wfid, dependents job ID, 
 * and depends job ID) of the job.
 *
 * WJobStatus does not need to implements Writable, as it is
 * only a intermediate class used to carry information from
 * WorkflowSubmitter to JobClient locally.
 */
public class WJobStatus implements Writable {
  public static final String SUBMITTER_PREFIX = "submitter_";

  private WorkflowID wfid;
  private JobID submitterID;
  private int remainingMap;
  private int remainingRed;
    
  public WJobStatus() {
  }

  public WJobStatus(WorkflowID wfid, 
                    int remainingMap,
                    int remainingRed) {
    this(wfid, null, remainingMap, remainingRed);
  }

  public WJobStatus(WorkflowID wfid, 
                    JobID submitterID,
                    int remainingMap,
                    int remainingRed) {
    this.wfid = wfid;
    this.submitterID = submitterID;
    this.remainingMap = remainingMap;
    this.remainingRed = remainingRed;
  }

  public int getRemainingMap() {
    return this.remainingMap;
  }

  public void setRemainingMap(int remainingMap) {
    this.remainingMap = remainingMap;
  }

  public int getRemainingRed() {
    return this.remainingRed;
  }

  public void setRemainingRed(int remainingRed) {
    this.remainingRed = remainingRed;
  }

  public WorkflowID getWorkflowID() {
    return this.wfid;
  }

  public JobID getSubmitterID() {
    return submitterID;
  }

  public boolean setSubmitterID(JobID submitterID) {
    if (null == this.submitterID) {
      this.submitterID = submitterID;
      return true;
    } else {
      return false;
    }
  }

  public static boolean isSubmitter(String name) {
    if (name.length() < SUBMITTER_PREFIX.length()) {
      return false;
    }
    String prefix = name.substring(0, SUBMITTER_PREFIX.length());
    return prefix.equals(SUBMITTER_PREFIX);
  }

  public static String getWJobName(String name) {
    return name.substring(SUBMITTER_PREFIX.length());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    wfid.write(out);
    
    out.writeBoolean(null != submitterID);
    if (null != submitterID) {
      submitterID.write(out);
    }

    out.writeInt(remainingMap);
    out.writeInt(remainingRed);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    wfid = new WorkflowID();
    wfid.readFields(in);

    boolean hasSubmitterID = in.readBoolean();
    if (hasSubmitterID) {
      submitterID = new JobID();
      submitterID.readFields(in);
    } else {
      submitterID = null;
    }

    remainingMap = in.readInt();
    remainingRed = in.readInt();
  }

}
