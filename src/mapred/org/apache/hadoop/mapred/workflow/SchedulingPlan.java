package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Hashtable;

import org.apache.hadoop.io.Writable;

public abstract class SchedulingPlan implements Writable {

  /**
   * get the required amount of work that needs to be scheduled
   * given the Time-To-Deadline (ttd).
   */
  public abstract long getRequirement (long ttd);

  public abstract int getSlotNum();

  /**
   * generate the scheduling plan given the maximum slots
   *
   * @param maxSlots the maximum number of slots the current workflow may occupy
   * @param wjobs all wjobs of the current workflow.
   *
   * @return true if the workflow is schedulable, false otherwise.
   */
  public abstract boolean generatePlan(int maxSlots, 
                                       WorkflowConf wfConf);

  public abstract void write(DataOutput out) throws IOException;

  public abstract void readFields(DataInput in) throws IOException;
}
