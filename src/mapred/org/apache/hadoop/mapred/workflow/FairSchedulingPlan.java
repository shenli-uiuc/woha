package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Hashtable;

import org.apache.hadoop.io.Writable;

public class FairSchedulingPlan extends SchedulingPlan {

  public long getRequirement (long ttd) {
    return 0;
  }

  public boolean generatePlan(int maxSlots, 
                              WorkflowConf wfConf) {
    return true;
  }

  public void write(DataOutput out) throws IOException {
  }

  public void readFields(DataInput in) throws IOException {
  }
}
