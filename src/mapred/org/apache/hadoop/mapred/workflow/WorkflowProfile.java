package org.apache.hadoop.mapred.workflow;

import java.net.URL;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * WorkflowProfile keeps some of a workflow's information
 * which does not change over time. (E.g., Workflow ID, URL)
 */
public class WorkflowProfile implements Writable {

  final WorkflowID wfid;
  String url;
  String name;
  String user;

  /**
   * For reflection
   */
  public WorkflowProfile() {
    wfid = new WorkflowID();
  }

  public WorkflowProfile(String user, WorkflowID wfid,
                         String url, String name) {
    this.user = user;
    this.wfid = wfid.clone();
    this.url  = url;
    this.name = name;
  }

  public WorkflowID getWorkflowID() {
    return wfid;
  }

  public URL getURL() {
    try {
      return new URL(url);
    } catch (IOException ex) {
      return null;
    }
  }

  public String getUser() {
    return user;
  }

  public String getJobName() {
    return name;
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    wfid.write(out);
    Text.writeString(out, user);
    Text.writeString(out, url);
    Text.writeString(out, name);
  }

  public void readFields(DataInput in) throws IOException {
    wfid.readFields(in);
    this.user = Text.readString(in);
    this.url  = Text.readString(in);
    this.name = Text.readString(in);

  }
}
