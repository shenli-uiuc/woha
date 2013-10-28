package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class WorkflowInfo implements Writable{
  private WorkflowID wfid;
  private Text user;
  private Path wfSubmitDir;
  private WorkflowInfo() {}

  public WorkflowInfo(WorkflowID wfid,
                      Text user,
                      Path wfSubmitDir) {
    this.wfid = wfid;
    this.user = user;
    this.wfSubmitDir = wfSubmitDir;
  }

  public WorkflowID getWorkflowID() {
    return wfid;
  }

  public Text getUser() {
    return user;
  }

  public Path getWfSubmitDir() {
    return wfSubmitDir;
  }

  public void readFields(DataInput in) throws IOException {
    wfid = new WorkflowID();
    wfid.readFields(in);
    user = new Text();
    user.readFields(in);
    wfSubmitDir = new Path(WritableUtils.readString(in));
  }

  public void write(DataOutput out) throws IOException {
    wfid.write(out);
    user.write(out);
    // Text.writeString should be able to do the same
    WritableUtils.writeString(out, wfSubmitDir.toString());
  }

}
