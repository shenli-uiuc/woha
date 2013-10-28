package org.apache.hadoop.mapred.workflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SchedulingEvent implements Writable{
  public long ttd;
  public long schedRequirement;

  /**
   * used before calling readFeilds
   */
  public SchedulingEvent() {
  }

  public SchedulingEvent(long ttd, long schedRequirement) {
    this.ttd = ttd;
    this.schedRequirement = schedRequirement;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(ttd);
    out.writeLong(schedRequirement);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ttd = in.readLong();
    schedRequirement = in.readLong();
  }
}
