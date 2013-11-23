package org.apache.hadoop.mapred.workflow;

import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobTracker;

import org.apache.commons.logging.Log;

public class WorkflowStatus implements Writable, Cloneable {

  public static final Log LOG = JobTracker.LOG;

  // resubmit the submitter task if the job tracker
  // does not receive the corresponding job in 30s
  public static final long MAX_SCHED_DELAY = 30000;

  public static final int PREP = 1;
  public static final int SUBMITTED = 2;
  public static final int RUNNING = 3;
  public static final int FAILED = 4;
  public static final int KILLED = 5;

  private static final String UNKNOWN = "UNKNOWN";
  private static final String[] runStates = 
      {UNKNOWN, "PREP", "SUBMITTED", "RUNNING",
       "FAILED", "KILLED"};

  private WorkflowID wfid;
  private int runState;
  private long submitTime;
  private long schedWork;

  // dependant set of a given job
  private Hashtable<String, HashSet<String> > deps;

  // prerequisite count of a given job
  private Hashtable<String, Integer> preCounts;

  // job names, as they are not submitted yet, they do not have job id.
  private TreeSet<String> inactiveJobs;
  private TreeSet<String> activeJobs;
  private TreeSet<String> scheduledJobs;
  private TreeSet<String> submittedJobs;
  private HashSet<String> finishedJobs;

  // Job name to job ID, and reverse
  private Hashtable<String, JobID> nameToID;
  private Hashtable<JobID, String> idToName;

  private Hashtable<String, Double> priorities;

  // wjob submitter task scheduled time, 
  // used to check the schedule delay. If the delay
  // is too long, the wjob submitter task will
  // be scheduled again.
  private Hashtable<String, Long> schedTime;

  private Hashtable<String, WJobStatus> wJobStatuses;

  /**
   * The default constructor is only used before 
   * readFields()
   */
  private WorkflowStatus() {
  }

  public WorkflowStatus(WorkflowID wfid, 
                        long submitTime,
                        WorkflowConf conf,
                        int runState) {
    this.wfid = wfid;
    this.runState = runState;
    this.submitTime = submitTime;
    Hashtable<String, Double> tmpPriorities = new Hashtable<String, Double>();
    // make sure priorities are unque, so that the tree set does not throw
    // away duplicate elements
    for (String jobName : conf.getWJobConfs().keySet()) {
      tmpPriorities.put(jobName,
          new Double(conf.getWJobConfs().get(jobName).getPriority()));
    }
    PriorityQueue<String> pQueue = new PriorityQueue<String> (
        conf.getWJobConfs().size(),
        new WorkflowUtil.StaticPriorityComparator(tmpPriorities));
    for (String jobName : conf.getWJobConfs().keySet()) {
      pQueue.add(jobName);
    }

    this.priorities = new Hashtable<String, Double>();
    double priority = pQueue.size();
    while(pQueue.size() > 0) {
      String jobName = pQueue.poll();
      this.priorities.put(jobName, new Double(priority));
      priority = priority - 1;
    }

    this.inactiveJobs = new TreeSet<String>(
          new WorkflowUtil.StaticPriorityComparator(this.priorities));
    this.activeJobs = new TreeSet<String>(
          new WorkflowUtil.StaticPriorityComparator(this.priorities));
    this.scheduledJobs = new TreeSet<String>(
          new WorkflowUtil.StaticPriorityComparator(this.priorities));
    this.submittedJobs = new TreeSet<String>(
          new WorkflowUtil.StaticPriorityComparator(this.priorities));
    this.finishedJobs = new HashSet<String>();
    this.nameToID = new Hashtable<String, JobID>();
    this.idToName = new Hashtable<JobID, String>();
    this.wJobStatuses = new Hashtable<String, WJobStatus>();
    initWJobStatuses(conf);
    this.deps = new Hashtable<String, HashSet<String> >();
    this.preCounts = new Hashtable<String, Integer>();
    this.schedTime = new Hashtable<String, Long>();
    this.schedWork = 0;
    setActiveJobs(conf.cloneActiveJobs());
    LOG.info("Shen Li: activeJobs " + conf.cloneActiveJobs().size() +
              ", " + activeJobs.size());
    setInactiveJobs(conf.cloneInactiveJobs());
    initDeps(conf);
  }

  public void initWJobStatuses(WorkflowConf conf) {
    Hashtable<String, WJobConf> wJobConfs = conf.getWJobConfs();
    for (String name : wJobConfs.keySet()) {
      WJobConf wJobConf = wJobConfs.get(name);
      wJobStatuses.put(name, 
                       new WJobStatus(this.wfid,
                                      wJobConf.getMapNum(),
                                      wJobConf.getRedNum()));
    }
  }

  public WJobStatus getWJobStatus(String name) {
    System.out.println("In getWjobStatus : " + name + 
              ", " + (null == wJobStatuses));
    return wJobStatuses.get(name);
  }

  private void initDeps(WorkflowConf conf) {
    // dataset to its output job name
    // WorkflowConf.readXML(String) guarantees one dataset
    // only comes from one wjob
    Hashtable<String, String> dsToJobName = 
      new Hashtable<String, String> ();

    //TODO: will this be too costly? should I put it into 
    //WorkflowConf, and pass it by HDFS?
    Hashtable<String, WJobConf> wJobConfs = 
      conf.getWJobConfs();
    for (String name : wJobConfs.keySet()) {
      WJobConf wJobConf = wJobConfs.get(name);
      for (String output : wJobConf.getOutputs()) {
        dsToJobName.put(output, name);
      }
    }

    for (String name : wJobConfs.keySet()) {
      WJobConf wJobConf = wJobConfs.get(name);
      HashSet<String> presSet = new HashSet<String>();
      for (String input : wJobConf.getInputs()) {
        String preName = dsToJobName.get(input);
        LOG.info("Shen Li: init deps - " + name + " - " + input + " - " + preName);
        if (null != preName) {
          if (!deps.keySet().contains(preName)) {
            HashSet<String> depsSet = new HashSet<String>();
            deps.put(preName, depsSet);
          }
          deps.get(preName).add(name);
          presSet.add(preName);
        }
      }
      this.preCounts.put(name, presSet.size());
    }

    for (String name : wJobConfs.keySet()) {
      String msg = "Shen Li : init deps : " + name + " - ";
      msg += preCounts.get(name) + " - ";
      if (null != deps.get(name)) {
        for (String dep : deps.get(name)) {
          msg += dep + ", ";
        }
      }
      LOG.info(msg);
    }


  }

  /** 
   * all HashSet/Hashtable should have already been 
   * initilized before calling add/remove/get methods.
   *
   * TODO: add boolean return value to see if job already
   * exist in the set before add/remove
   */

  public long getSubmitTime() {
    return this.submitTime;
  }

  public boolean isCompleted() {
    return (this.activeJobs.size() <= 0 &&
            this.inactiveJobs.size() <= 0 &&
            this.scheduledJobs.size() <= 0 &&
            this.submittedJobs.size() <= 0);
  }

  public void setInactiveJobs(HashSet<String> inactiveJobs) {
    synchronized (inactiveJobs) {
      for (String jobName : inactiveJobs) {
        this.inactiveJobs.add(jobName);
      }
    }
  }

  public void setActiveJobs(HashSet<String> activeJobs) {
    synchronized (activeJobs) {
      for (String jobName : activeJobs) {
        this.activeJobs.add(jobName);
      }
    }
  }

  public boolean addInactiveJob(String name) {
    synchronized (inactiveJobs) {
      return inactiveJobs.add(name);
    }
  }

  public boolean removeInactiveJob(String name) {
    synchronized (inactiveJobs) {
      return inactiveJobs.remove(name);
    }
  }

  public boolean addActiveJob(String name) {
    synchronized (activeJobs) {
      return activeJobs.add(name);
    }
  }

  public void addSchedWork(long work) {
    synchronized (this) {
      this.schedWork += work;
    }
  }

  public long getSchedWork() {
    synchronized (this) {
      return this.schedWork;
    }
  }

  public boolean addScheduledWJob(String name) {
    synchronized (this) {
      if (scheduledJobs.contains(name)) {
        // should never happen
        // TODO: handle the error rather than reset
        // the clock
        schedTime.put(name, 
            new Long(System.currentTimeMillis()));
        return false;
      } else {
        activeJobs.remove(name);
        scheduledJobs.add(name);
        schedTime.put(name, 
            new Long(System.currentTimeMillis()));
        return true;
      }
    }
  }

  public boolean addSubmittedWJob(String name, JobID id) {
    synchronized (this) {
      if (idToName.contains(id)) {
        return false;
      } else {
        idToName.put(id, name);
        nameToID.put(name, id);
        scheduledJobs.remove(name);
        submittedJobs.add(name);
        return true;
      }
    }
  }

  public void addFinishedWJob(JobID id) {
    synchronized(this) {
      if (idToName.keySet().contains(id)) {
        String name = idToName.get(id);
        LOG.info("Shen Li: addFinishedWJob id = " + id.toString() +
            ", name = " + name);
        submittedJobs.remove(name);
        finishedJobs.add(name);
        activateWJobs(name);
      }
    }
  }

  public synchronized void activateWJobs (String name) {
    LOG.info("Shen Li: in activeWJobs name is " + name);
    HashSet<String> curDeps = deps.get(name);
    String msg = "";
    String checked = "";
    if (null != curDeps) {
      for (String dep : deps.get(name)) {
        int preCount = preCounts.get(dep) - 1;
        preCounts.put(dep, preCount);
        checked = checked + "; " + dep + ", " + preCount;
        if (0 == preCount) {
          inactiveJobs.remove(dep);
          activeJobs.add(dep);
          msg = msg + dep + ",";
        }
      }
    }
    LOG.info("Shen Li: " + name + " activates " + msg + ". checked: " + checked);
  }

  public long getScheduleDelay(String name) {
    LOG.info("Shen Li: WorkflowStatus Job " + name +
             " schedTime - " + schedTime.get(name).longValue()
             + " currentTime - " + System.currentTimeMillis());
    return System.currentTimeMillis() - 
             schedTime.get(name).longValue();
  }

  public WorkflowID getWorkflowID() {
    return wfid;
  }

  public TreeSet<String> getInactiveJobs() {
    return inactiveJobs;
  }

  public TreeSet<String> getActiveJobs() {
    return activeJobs;
  }

  public TreeSet<String> getScheduledJobs() {
    return scheduledJobs;
  }

  public TreeSet<String> getSubmittedJobs() {
    return submittedJobs;
  }

  public HashSet<String> getFinishedJobs() {
    return finishedJobs;
  }

  public Hashtable<String, WJobStatus> getWJobStatuses() {
    return wJobStatuses;
  }

  public Hashtable<String, JobID> getNameToID() {
    return nameToID;
  }

  public Hashtable<JobID, String> getIDToName() {
    return idToName;
  }

  public void setRunState(int runState) {
    this.runState = runState;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen
      System.err.println("Cannot clone WorkflowStatus object");
    }
    return null;
  }

  ///////////////////////////////////////
  // Writable
  ///////////////////////////////////////

  //Writable
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    wfid.write(out);
    out.writeInt(runState);
    out.writeLong(submitTime);
    out.writeLong(schedWork);

    //write priorities
    out.writeInt(priorities.size());
    for (String name : priorities.keySet()) {
      Text.writeString(out, name);
      out.writeDouble(priorities.get(name).doubleValue());
    }

    //write deps
    out.writeInt(deps.size());
    for (String name : deps.keySet()) {
      Text.writeString(out, name);
      if (!deps.contains(name)) {
        out.writeInt(0);
      } else {
        HashSet<String> oneDeps = deps.get(name);
        out.writeInt(oneDeps.size());
        for (String dep : oneDeps) {
          Text.writeString(out, dep);
        }
      }
    }

    //write preCounts
    out.writeInt(preCounts.size());
    for (String name : preCounts.keySet()) {
      Text.writeString(out, name);
      out.writeInt(preCounts.get(name).intValue());
    }

    //write inactive jobs
    out.writeInt(inactiveJobs.size());
    for (String jobName : inactiveJobs) {
      Text.writeString(out, jobName);
    }

    //write active jobs
    out.writeInt(activeJobs.size());
    for (String jobName : activeJobs) {
      Text.writeString(out, jobName);
    }

    //write scheduled jobs
    out.writeInt(scheduledJobs.size());
    for (String jobName : scheduledJobs) {
      Text.writeString(out, jobName);
    }

    //write submitted jobs
    out.writeInt(submittedJobs.size());
    for (String jobName : submittedJobs) {
      Text.writeString(out, jobName);
    }

    //write finished jobs
    out.writeInt(finishedJobs.size());
    for (String jobName : finishedJobs) {
      Text.writeString(out, jobName);
    }

    //write job name id mapping
    out.writeInt(nameToID.size());
    for (String name : nameToID.keySet()) {
      Text.writeString(out, name);
      nameToID.get(name).write(out);
    }

    //write schedTime
    out.writeInt(schedTime.size());
    for (String name : schedTime.keySet()) {
      Text.writeString(out, name);
      out.writeLong(schedTime.get(name).longValue());
    }

    //write WJobStatuses
    out.writeInt(wJobStatuses.size());
    for (String name : wJobStatuses.keySet()) {
      Text.writeString(out, name);
      wJobStatuses.get(name).write(out);
    }
  }

  @Override
  public synchronized void readFields(DataInput in)
      throws IOException {
    wfid = new WorkflowID();
    wfid.readFields(in);
    runState = in.readInt();
    submitTime = in.readLong();
    schedWork = in.readLong();

    System.out.println("RRRRRRRRRRRRRRRRRRRRRRRRRRR: in workflowStatus readFields");

    int size = 0;

    //read priorities
    size = in.readInt();
    priorities = new Hashtable<String, Double>();
    while (size > 0) {
      String name = Text.readString(in);
      double priority = in.readDouble();
      priorities.put(name, new Double(priority));
      --size;
    }

    //read deps
    size = in.readInt();
    deps = new Hashtable<String, HashSet<String> >();
    while (size > 0) {
      String name = Text.readString(in);
      int depsSize = in.readInt();
      if (depsSize > 0) {
        HashSet<String> oneDeps = new HashSet<String>();
        while (depsSize > 0) {
          String dep = Text.readString(in);
          oneDeps.add(dep);
          --depsSize;
        }
        deps.put(name, oneDeps);
      }
      --size;
    }

    //read precounts
    size = in.readInt();
    preCounts = new Hashtable<String, Integer>();
    while (size > 0) {
      String name = Text.readString(in);
      int cnt = in.readInt();
      preCounts.put(name, new Integer(cnt));
      --size;
    }

    //read inactive jobs
    size = in.readInt();
    inactiveJobs = new TreeSet<String>(
        new WorkflowUtil.StaticPriorityComparator(priorities));
    while(size > 0) {
      inactiveJobs.add(Text.readString(in));
      --size;
    }

    //read active jobs
    size = in.readInt();
    activeJobs = new TreeSet<String>(
        new WorkflowUtil.StaticPriorityComparator(priorities));
    while(size > 0) {
      activeJobs.add(Text.readString(in));
      --size;
    }

    //read scheduled jobs
    size = in.readInt();
    scheduledJobs = new TreeSet<String>(
        new WorkflowUtil.StaticPriorityComparator(priorities));
    while (size > 0) {
      scheduledJobs.add(Text.readString(in));
      --size;
    }

    //read submitted jobs
    size = in.readInt();
    submittedJobs = new TreeSet<String> (
        new WorkflowUtil.StaticPriorityComparator(priorities));
    while (size > 0) {
      submittedJobs.add(Text.readString(in));
      --size;
    }

    //read finished jobs
    size = in.readInt();
    finishedJobs = new HashSet<String>();
    while (size > 0) {
      String name = Text.readString(in);
      finishedJobs.add(name);
      --size;
    }

    //read job name id mapping
    size = in.readInt();
    nameToID = new Hashtable<String, JobID>();
    idToName = new Hashtable<JobID, String>();
    while (size > 0) {
      String name = Text.readString(in);
      JobID id = new JobID();
      id.readFields(in);
      nameToID.put(name, id);
      idToName.put(id, name);
      --size;
    }

    //read schedTime
    size = in.readInt();
    schedTime = new Hashtable<String, Long>();
    while (size > 0) {
      String name = Text.readString(in);
      long t = in.readLong();
      schedTime.put(name, new Long(t));
      --size;
    }

    // read WJobStatuses
    size = in.readInt();
    wJobStatuses = new Hashtable<String, WJobStatus>();
    while (size > 0) {
      String name = Text.readString(in);
      WJobStatus wJobStatus = new WJobStatus();
      wJobStatus.readFields(in);
      wJobStatuses.put(name, wJobStatus);
      --size;
    }

    System.out.println("RRRRRRRRRRRRRRRRRRRRRRRRRRR after read WJobStatuses");
  }

  /**
   * For testing purpose
   */
  public void print() {
    System.out.println("InactiveJobs #: " + inactiveJobs.size()
                       + "------------------");
    for (String name : inactiveJobs) {
      System.out.print(name + ", ");
    }
    System.out.println();

    System.out.println("ActiveJobs #: " + activeJobs.size()
                       + "------------------");

    for (String name : activeJobs) {
      System.out.print(name + ", ");
    }
    System.out.println();

    System.out.println("ScheduledJobs #: " + scheduledJobs.size()
                       + "------------------");
    for (String name : scheduledJobs) {
      System.out.print(name + ", ");
    }
    System.out.println();

    System.out.println("SubmittedJobs #: " + submittedJobs.size()
                       + "--------------------");
    for (String name : submittedJobs) {
      System.out.print(name + ", ");
    }
    System.out.println();

    System.out.println("WJobStatuses: WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW");

    System.out.println("is null ? " + (null == wJobStatuses) + 
        ", " + (null == nameToID));

    System.out.println("Deps");

    for (String name : deps.keySet()) {
      System.out.print(name + ": ");
      for (String dep : deps.get(name)) {
        System.out.print(dep + ", ");
      }
      System.out.println();
      System.out.println(preCounts.get(name));
    }
  }
}
