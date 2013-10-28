package org.apache.hadoop.mapred.workflow;

import java.io.IOException;

/**
 * A listener for changes in a {@link WorkflowInProgress}'s
 * lifecycle. The implementation must also implement 
 * {@link JobInProgressListener}.
 */
public interface WorkflowInProgressListener{
  public void workflowAdded(WorkflowInProgress wf)
    throws IOException;

  public void workflowRemoved(WorkflowInProgress wf);

  public void workflowUpdated(WorkflowChangeEvent wfe);
}
