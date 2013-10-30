package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Hashtable;

import org.apache.hadoop.io.Writable;

public class FairSchedulingPlan extends SchedulingPlan {

  @Override
  public long getRequirement (long ttd) {
    return 0;
  }

  @Override
  public boolean generatePlan(int maxSlots, 
                              WorkflowConf wfConf) {
    return true;
  }

  @Override
  public int getSlotNum() {
    return 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    System.out.println("FFFFFFFFFFFFFFFFFFFFFFFFF in fairplan");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }
}
