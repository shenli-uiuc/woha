package org.apache.hadoop.mapred.workflow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class stores a job's configuration, it does not change during
 * the after a workflow is submitted. 
 */
public class WJobConf implements Writable{

  private String name;
  private String jarPath;
  private String jarClass;
  private String inputs[];
  private String outputs[];
  private String deps[];
  private String pres[];
  private String param;
  // the number of mapper and reducer
  private int mapNum;
  private int redNum;
  // The estimated execution time of mapper and reducer
  private long mapEstTime;
  private long redEstTime;
  private long priority;

  // default constructor is only used before readFields
  public WJobConf() {}

  public WJobConf(String name,
                  String jarPath,
                  String jarClass,
                  String inputs[],
                  String outputs[],
                  String param,
                  int mapNum,
                  int redNum,
                  long mapEstTime,
                  long redEstTime) {
    this(name, jarPath, jarClass,
         inputs, outputs, param,
         mapNum, redNum, mapEstTime, redEstTime,
         0);
  }

  public WJobConf(String name,
              String jarPath,
              String jarClass,
              String inputs[],
              String outputs[],
              String param,
              int mapNum,
              int redNum,
              long mapEstTime,
              long redEstTime,
              long priority
              ) {
    this.name = name;
    this.jarPath = jarPath;
    this.jarClass = jarClass;
    this.inputs = inputs;
    this.outputs = outputs;
    if (null == param) {
      this.param = " ";
    }
    this.mapNum = mapNum;
    this.redNum = redNum;
    this.mapEstTime = mapEstTime;
    this.redEstTime = redEstTime;
    this.priority = priority;
  }

  public String getName() {
    return name;
  }

  public String getJarPath() {
    return jarPath;
  }

  public void setJarPath(String jarPath) {
    this.jarPath = jarPath;
  }

  public String getJarClass() {
    return jarClass;
  }

  public String [] getInputs() {
    return inputs;
  }

  public String [] getOutputs() {
    return outputs;
  }

  public String getParam() {
    return param;
  }

  public int getMapNum() {
    return mapNum;
  }

  public int getRedNum() {
    return redNum;
  }

  public long getMapEstTime() {
    return mapEstTime;
  }

  public long getRedEstTime() {
    return redEstTime;
  }

  public long getPriority() {
    return this.priority;
  }

  public void setPriority(long priority) {
    this.priority = priority;
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, jarPath);
    Text.writeString(out, jarClass);
    out.writeInt(mapNum);
    out.writeInt(redNum);
    out.writeLong(mapEstTime);
    out.writeLong(redEstTime);
    out.writeLong(priority);

    //inputs
    out.writeInt(inputs.length);
    for (String input : inputs) {
      Text.writeString(out, input);
    }

    //outputs
    out.writeInt(outputs.length);
    for (String output : outputs) {
      Text.writeString(out, output);
    }
    
    if (null != param) {
      Text.writeString(out, param);
    } else {
      Text.writeString(out, " ");
    }
  }

  public void readFields(DataInput in) throws IOException {
    int size = 0;

    name = Text.readString(in);
    jarPath = Text.readString(in);
    jarClass = Text.readString(in);
    mapNum = in.readInt();
    redNum = in.readInt();
    mapEstTime = in.readLong();
    redEstTime = in.readLong();
    priority = in.readLong();

    //read inputs
    size = in.readInt();
    inputs = new String [size];
    for (int i = 0 ; i < size; ++i) {
      inputs[i] = Text.readString(in);
    }

    //read outputs
    size = in.readInt();
    outputs = new String [size];
    for (int i = 0 ; i < size; ++i) {
      outputs[i] = Text.readString(in);
    }

    param = Text.readString(in);

  }

  /**
   * For debugging purpose
   */ 
  public void printWJobConf() {
    System.out.println("name: " + name);
    System.out.println("jarpath: " + jarPath);
    System.out.println("jarclass: " + jarClass);
    System.out.println("mapNum: " + mapNum);
    System.out.println("redNum: " + redNum);
    System.out.println("mapEstTime: " + mapEstTime);
    System.out.println("redEstTime: " + redEstTime);
    System.out.print("inputs: ");
    if (null != inputs) {
      for (String input : inputs) {
        System.out.print(input + ",");
      }
    }

    System.out.println();
    System.out.print("outputs: ");
    if (null != outputs) {
      for (String output : outputs) {
        System.out.print(output + ",");
      }
    }
    System.out.println();

    System.out.println("Param: " + param);
  }
}
