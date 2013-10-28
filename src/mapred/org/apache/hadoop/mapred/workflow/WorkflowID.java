package org.apache.hadoop.mapred.workflow;

import java.text.NumberFormat;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ID;

/**
 * The only different between WorkflowID and JobID is replacing
 * the leading "JOB" prefix to "WF".
 *
 * However, as jtIdentifier is private in JobID, this class
 * reimplement everything instead of extends JobID.
 */
public class WorkflowID extends org.apache.hadoop.mapred.ID 
       implements Comparable<org.apache.hadoop.mapreduce.ID>,
                  Cloneable {
  protected static final String WF = "wf";
  public static final int ID_PARTS_NUM = 3;
  public static final int JT_PART_INDEX = 1;
  public static final int ID_PART_INDEX = 2;

  // workflow ID regex
  public static final String WF_REGEX = 
    WF + SEPARATOR + "[0-9]+" + SEPARATOR + "[0-9]+";

  private final Text jtIdentifier;

  protected static final NumberFormat idFormat = 
      NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(4);
  }

  public WorkflowID(String jtIdentifier, int id) {
    super(id);
    this.jtIdentifier = new Text(jtIdentifier);
  }

  public WorkflowID() {
    jtIdentifier = new Text();
  }

  public String getJtIdentifier() {
    return jtIdentifier.toString();
  }


  //////////////////////////////////////////////
  // Cloneable
  //////////////////////////////////////////////

  @Override
  public WorkflowID clone() {
    return new WorkflowID(this.jtIdentifier.toString(), this.id);
  }

  //////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////

  @Override
  public boolean equals(Object o) {
    // this guarantees the id part to be equal
    if (!super.equals(o))
      return false;

    WorkflowID that = (WorkflowID)o;
    return (this.jtIdentifier.equals(that.jtIdentifier) 
            && this.id == that.id);
  }

  public int compareTo(ID o) {
    WorkflowID that = (WorkflowID) o;
    int jtComp = this.jtIdentifier.compareTo(that.jtIdentifier);
    if (0 == jtComp) {
      return this.id - that.id;
    } else {
      return jtComp;
    }
  }

  @Override
  public int hashCode() {
    return jtIdentifier.hashCode() + id;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(WF);
    sb.append(SEPARATOR);
    sb.append(jtIdentifier);
    sb.append(SEPARATOR);
    sb.append(idFormat.format(id));
    return sb.toString();
  }

  public static WorkflowID fromString(String strID) 
      throws IOException {
    if (null == strID) {
      return null;
    }
    String [] parts = strID.split("" + SEPARATOR);
    if (ID_PARTS_NUM != parts.length) {
      throw new IOException("The workflow ID must have "
          + ID_PARTS_NUM + " parts separated by " +
          SEPARATOR);
    }

    return new WorkflowID(parts[JT_PART_INDEX], 
                          Integer.parseInt(parts[ID_PART_INDEX]));
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    jtIdentifier.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jtIdentifier.write(out);
  }

  public static WorkflowID forName(String str) 
    throws IllegalArgumentException {
    if (null == str) {
      return null;
    }

    try {
      String [] parts = str.split("_");
      if (parts.length == 3) {
        if (parts[0].equals(WF)) {
          return new WorkflowID(parts[1], 
                                Integer.parseInt(parts[2]));
        }
      }
    } catch (Exception ex) {
    }
    throw new IllegalArgumentException("WorkflowId string : "
        + str + " is not properly formed");
  }

}
