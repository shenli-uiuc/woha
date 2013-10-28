package org.apache.hadoop.mapred.workflow;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import org.xml.sax.SAXException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Given the user-defined workflow configuration (UDC) file, the WorkflowConf
 * class provides following functionalities:
 *  1. Constructor(): Read the UDC XML, check XML format, and generate DAG.
 *  2. getJarPaths(): Get the paths of user jar packages in the local disk.
 *  3. getInputDatasetPaths(): get the paths of input datasets in the HDFS
 *
 * WorkflowClient needs the above information to perform the checking steps
 * and submit the workflow to JobTracker.
 *
 * Remember to delete the jar files from jar_store when the workflow
 * is done.
 */

public class WorkflowConf extends Configuration implements Writable{

    public static final String SCHEDULING_PLAN_PROPERTY_NAME = 
                               "mapred.workflow.schedulingPlan";

    public static final Log LOG = LogFactory.getLog(WorkflowConf.class);

    private String name;
    private String user;
    //relative deadline in milliseconds for now
    private long deadline;
    // the corresponding workflow will be submitted after 
    // submit the workflow at unix time in milli-seconds
    private long submitAt;
    // the minimum parallelism among all jobs (i.e., minimum maxpara)
    private long minPara;
    private Hashtable<String, WJobConf> jobs;
    private Hashtable<String, String> datasets;
    private HashSet<String> inDs; //input datasets
    // Jobs that are active from the very beginning
    private HashSet<String> activeJobs;
    // Jobs that are inactive from the very beginning
    private HashSet<String> inactiveJobs;

    private SchedulingPlan plan;

    /**
     * This constructor is only used to create a WorkflowConf
     * object using readFields()
     */
    public WorkflowConf() {
    }

    public WorkflowConf(String name,
                        String user,
                        long deadline,
                        long submitAt,
                        long minPara,
                        Hashtable<String, WJobConf> jobs,
                        Hashtable<String, String> datasets,
                        HashSet<String> inDs) {
      this.name = name;
      this.user = user;
      this.deadline = deadline;
      this.submitAt = submitAt;
      this.minPara = minPara;
      this.jobs = jobs;
      this.datasets = datasets;
      this.inDs = inDs;
      initJobs();

      // the task scheduler in-use may ignore this scheduling plan
      Configuration conf = new Configuration();
      Class<? extends SchedulingPlan> planClass = 
        conf.getClass(SCHEDULING_PLAN_PROPERTY_NAME, 
          FairSchedulingPlan.class, SchedulingPlan.class);
      this.plan = 
        (SchedulingPlan)ReflectionUtils.newInstance(planClass, conf);
    
    }

    public boolean generatePlan(int maxSlots) {
      return plan.generatePlan(maxSlots, this);
    }

    public long getSubmitAt() {
      return submitAt;
    }

    public long getDeadline() {
      return deadline;
    }

    public SchedulingPlan getSchedulingPlan() {
      return plan;
    }

    /**
     * TODO: for recurring workflows, some job may depend on 
     * ouputs from previous round.
     */
    private void initJobs() {
      activeJobs = new HashSet<String> ();
      inactiveJobs = new HashSet<String> ();
      boolean isActive;
      for (String name : jobs.keySet()) {
        isActive = true;
        WJobConf job = jobs.get(name);
        for (String input : job.getInputs()) {
          if (!inDs.contains(input)) {
            isActive = false;
            break;
          }
        }
        if (isActive) {
          activeJobs.add(name);
        } else {
          inactiveJobs.add(name);
        }
      }
    }

    /**
     * Split the input or output datasets string into an array
     * of dataset names.
     */ 
    private String [] getIODatasets(Element e, String tag){
      NodeList nl = e.getElementsByTagName(tag);
      if (null == nl || nl.getLength() <= 0) {
        return new String[0];
      }
      String str = nl.item(0).getTextContent();
      // dataset are separated by comma. Here, we remove all whitespaces.
      str = str.replaceAll("[\t\n ]", "");
      return str.split(",");
    }

    /**
     * get the paths of input datasets in the HDFS.
     */ 
    public HashSet<String> getInputDatasets() {
      return inDs;
    }

    public String getDatasetPath(String name) {
      return datasets.get(name);
    }
    
    public Hashtable<String, WJobConf> getWJobConfs() {
      return jobs;
    }

    public String getName() {
      return name;
    }

    public long getMinPara() {
      return minPara;
    }

    public HashSet<String> getActiveJobs() {
      return activeJobs;
    }

    public HashSet<String> cloneActiveJobs() {
      // shallow copy is enough here, as elements
      // are strings
      return (HashSet<String>)activeJobs.clone();
    }

    public HashSet<String> cloneInactiveJobs() {
      return (HashSet<String>)inactiveJobs.clone();
    }

    /**
     * Implement the method in Writable interface to serialize 
     * WorkflowConf object. 
     */ 

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, name);
      Text.writeString(out, user);
      out.writeLong(minPara);
      out.writeLong(deadline);

      // write jobs
      out.writeInt(jobs.size());
      for (String name : jobs.keySet()) {
        Text.writeString(out, name);
        jobs.get(name).write(out);
      }

      // write datasets
      out.writeInt(datasets.size());
      for (String name : datasets.keySet()) {
        Text.writeString(out, name);
        Text.writeString(out, datasets.get(name));
      }

      // write inDs
      out.writeInt(inDs.size());
      for (String name : inDs) {
        Text.writeString(out, name);
      }

      // write activeJobs
      out.writeInt(activeJobs.size());
      for (String name : activeJobs) {
        Text.writeString(out, name);
      }

      // write inactiveJobs
      out.writeInt(inactiveJobs.size());
      for (String name : inactiveJobs) {
        Text.writeString(out, name);
      }
    }
  
    /**
     * Implement the method in Writable interface to deserialize 
     * WorkflowConf object.
     */ 
    
    public void readFields(DataInput in) throws IOException {
      name = Text.readString(in);
      user = Text.readString(in);
      minPara = in.readLong();
      deadline = in.readLong();

      int size = 0;
      String tmpName;
      String tmpPath;

      // read jobs
      size = in.readInt();
      LOG.info("jobs size = " + size);
      jobs = new Hashtable<String, WJobConf>();
      while (size > 0) {
        tmpName = Text.readString(in);
        WJobConf job = new WJobConf();
        job.readFields(in);
        jobs.put(tmpName, job);
        --size;
      }

      // read datasets
      size = in.readInt();
      LOG.info("datasets size = " + size);
      datasets = new Hashtable<String, String>();
      while (size > 0) {
        tmpName = Text.readString(in);
        tmpPath = Text.readString(in);
        datasets.put(tmpName, tmpPath);
        --size;
      }

      // read inDs
      size = in.readInt();
      LOG.info("inDs size = " + size);
      inDs = new HashSet<String> ();
      while (size > 0) {
        tmpName = Text.readString(in);
        inDs.add(tmpName);
        --size;
      }

      // read activeJobs
      size = in.readInt();
      activeJobs = new HashSet<String> ();
      while (size > 0) {
        tmpName = Text.readString(in);
        activeJobs.add(tmpName);
        --size;
      }

      // read inactiveJobs
      size = in.readInt();
      inactiveJobs = new HashSet<String> ();
      while (size > 0) {
        tmpName = Text.readString(in);
        inactiveJobs.add(tmpName);
        --size;
      }

      // read scheduling plan
      Configuration conf = new Configuration();
      Class<? extends SchedulingPlan> planClass =
        conf.getClass(SCHEDULING_PLAN_PROPERTY_NAME,
            FairSchedulingPlan.class, SchedulingPlan.class);
      plan = 
        (SchedulingPlan)ReflectionUtils.newInstance(planClass, conf);
      plan.readFields(in);

      LOG.info("WorkflowConf.readFields: done");
    }

    /**
     * For Debug, print out the workflow XML content
     */ 
    public void printWorkflowConf() {
      System.out.println("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW");
      System.out.println("workflow: " + name);
      System.out.println("user: " + user);
      System.out.println("minPara: " + minPara);
      System.out.println("deadline: " + deadline);

      System.out.println("Datasets:===================");

      for (String d : datasets.keySet()) {
        System.out.println("name: " + d + ", path: " + 
                            datasets.get(d));
      }

      System.out.println("input datasets: =====================");
      for (String d : inDs) {
        System.out.print(d);
      }
      System.out.println();

      System.out.println("Jobs:=======================");      
      for (String j : jobs.keySet()) {
        System.out.println("---------------------");
        WJobConf wJob = jobs.get(j);
        wJob.printWJobConf();
        System.out.println();
      }

      System.out.println("active jobs: ========================");
      for (String name : activeJobs) {
        System.out.println(name);
      }

      System.out.println("inactive jobs: ============================");
      for (String name : inactiveJobs) {
        System.out.println(name);
      }

    }
}
