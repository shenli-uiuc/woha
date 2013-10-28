package org.apache.hadoop.mapred.workflow;

import java.util.ArrayList;

public class SchedulingInstruction {
  
  public int slotNum;
  public ArrayList<SchedulingEvent> schedEvents;

  public SchedulingInstruction(int slotNum, 
                               ArrayList<SchedulingEvent> schedEvents) {
    this.slotNum = slotNum;
    this.schedEvents = schedEvents;
  }
}
