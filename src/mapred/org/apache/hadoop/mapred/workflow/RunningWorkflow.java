package org.apache.hadoop.mapred.workflow;

import java.io.IOException;

public interface RunningWorkflow {

  /**
   * Returns a snapshot of current workflow status.
   */
  public WorkflowStatus getWorkflowStatus() throws IOException;
}
