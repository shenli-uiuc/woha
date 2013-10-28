package org.apache.hadoop.mapred.workflow;

public abstract class WorkflowChangeEvent {
  private WorkflowInProgress wip;

  WorkflowChangeEvent(WorkflowInProgress wip) {
    this.wip = wip;
  }

  WorkflowInProgress getWorkflowInProgress() {
    return wip;
  }
}
