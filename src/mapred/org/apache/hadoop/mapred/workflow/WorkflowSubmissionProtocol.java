package org.apache.hadoop.mapred.workflow;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapred.ClusterStatus;

public interface WorkflowSubmissionProtocol extends VersionedProtocol {
  public static final long versionID = 1L;

  public WorkflowID getNewWorkflowId() throws IOException;

  /**
   * Submit a workflow.
   *
   * This only put the workflow;s configuration in JobTracker's
   * Workflow pool. It is up tp JobTracker when to schedule its
   * jobs.
   */
  public WorkflowStatus submitWorkflow(
      WorkflowID wfID, 
      String wfSubmitDir,
      String name) throws IOException;

  /**
   * pull workflow status from JobTracker
   */
  public WorkflowStatus getWorkflowStatus(
      WorkflowID wfid) throws IOException;

  /**
   * pull workflow status from JobTracker
   */
  public WorkflowProfile getWorkflowProfile(
      WorkflowID wfid) throws IOException;

  /**
   * get staging area dir from the jobtracker's perspective to
   * put workflow related files
   */
  public String getWorkflowStagingAreaDir() throws IOException;

  public ClusterStatus getClusterStatus();
}
