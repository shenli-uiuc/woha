package org.cyphy;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.mapred.workflow.WJobStatus;
import org.apache.hadoop.mapred.workflow.WJobConf;
import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WJob;
import org.apache.hadoop.mapred.workflow.WJobInProgress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//TODO: refer to sleepJob in examples
public class SlotHolder implements 
       Mapper<IntWritable, IntWritable, IntWritable, IntWritable>,
       Reducer<IntWritable, IntWritable, IntWritable, IntWritable>,
       Partitioner<IntWritable, IntWritable> {

  public static final String TMP_DIR_PREFIX = "/shen/tmp/SlotHolder/";
  private static final Log LOG = LogFactory.getLog(SlotHolder.class);

  private int mapNum;
  private int redNum;
  private long mapExeTime;
  private long redExeTime;

  @Override
  public void map(IntWritable key, IntWritable value,
                  OutputCollector<IntWritable, IntWritable> out,
                  Reporter reporter) throws IOException {
    
    try {
      Thread.sleep(mapExeTime);
    } catch (InterruptedException ex) {
      LOG.error("Mapper sleep InterruptedException: " + ex.getMessage());
    }
   
    System.out.println("in SlotHolder.map");

    for (int i = 0 ; i < redNum; ++i) {
      out.collect(new IntWritable(i), new IntWritable(0));
    }
  }
  

  @Override
  public void reduce(IntWritable key, 
                     Iterator<IntWritable> value,
                     OutputCollector<IntWritable, IntWritable> out,
                     Reporter reporter) throws IOException {
    try {
      Thread.sleep(redExeTime);
    } catch (InterruptedException ex) {
      LOG.error("Reducer sleep Interrupted Exception: " + ex.getMessage());
    }

  }

  @Override
  public int getPartition(IntWritable key, IntWritable value,
                          int numPartitions) {
      return key.get() % numPartitions;
  }


  @Override
  public void configure(JobConf job) {
    this.mapNum = job.getNumMapTasks();
    this.redNum = job.getNumReduceTasks();
    //default is sleep 1 min
    this.mapExeTime = job.getLong("slotholder.job.map.sleep.time", 60000);
    this.redExeTime = job.getLong("slotholder.job.reduce.sleep.time", 
                                  60000);
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Shen Li:
   * WJobStatus contains WorkflowID, dependants, and depends.
   */
  public static RunningJob submit(WJobConf wJobConf, 
                           WorkflowID wfid) 
      throws IOException {
    return submit(wJobConf, wfid, false);
  }

  public static RunningJob submit(WJobConf wJobConf,
                           WorkflowID wfid,
                           boolean monitor) 
      throws IOException {
    //WJobConf wJobConf = wjip.getConf();
    //WJobStatus wJobStatus = wjip.getStatus();
    
    File tmpLogFile = File.createTempFile("ShenTempLog", ".log");

    BufferedWriter bw = new BufferedWriter(new FileWriter(tmpLogFile));
    bw.write("Shen Li: " + System.currentTimeMillis() + "In submit()");
    bw.close();

    LOG.info("Shen Li: in SlotHolder.submit()");
    System.out.println("Shen Li print: in SlotHolder.submit()");
    JobConf jobConf = new JobConf(new Configuration(), 
                                  SlotHolder.class);
    jobConf.setJobName(wJobConf.getName());

    jobConf.setWorkflowID(wfid);
    /*
    String [] depends = wJobStatus.getDepends();
    String [] dependants = wJobStatus.getDependants();
    if (null != depends) {
      jobConf.setJobDepends(wJobStatus.getDepends());
    }
    if (null != dependants) {
      jobConf.setJobDependants(wJobStatus.getDependants());
    }
    */
    jobConf.setInputFormat(SequenceFileInputFormat.class);

    jobConf.setOutputKeyClass(IntWritable.class);
    jobConf.setOutputValueClass(IntWritable.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);

    jobConf.setMapperClass(SlotHolder.class);
    jobConf.setNumMapTasks(wJobConf.getMapNum());

    jobConf.setReducerClass(SlotHolder.class);
    jobConf.setNumReduceTasks(wJobConf.getRedNum());

    jobConf.setPartitionerClass(SlotHolder.class);

    jobConf.setLong("slotholder.job.map.sleep.time", 
                wJobConf.getMapEstTime());
    jobConf.setLong("slotholder.job.reduce.sleep.time",
                wJobConf.getRedEstTime());

    jobConf.setSpeculativeExecution(false);

    // For SlotHolder jobs, each job has only one indir
    Path tmpDir = new Path(TMP_DIR_PREFIX + "/" + wfid.toString()
                           + "/" + wJobConf.getName());
    Path inDir = new Path(tmpDir, "in");
    Path outDir = new Path(tmpDir, "out");

    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outDir);

    FileSystem fs = FileSystem.get(jobConf);

    if (fs.exists(tmpDir)) {
      throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
          + " already exists. Please remove it first.");
    }
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Cannot create input directory " + inDir);
    }

    LOG.info("Shen Li: before generating splits");
    System.out.println("Shen Li print : before generating splits");

    // generate an input file for each map task
    for (int i = 0 ; i < wJobConf.getMapNum(); ++i) {
      final Path file = new Path(inDir, "part"+i);
      final SequenceFile.Writer writer = SequenceFile.createWriter(
          fs, jobConf, file, 
          IntWritable.class, IntWritable.class, CompressionType.NONE);
      try {
        writer.append(new IntWritable(0), new IntWritable(0));
      } finally {
        writer.close();
      }
    }

    LOG.info("Shen Li: before submitting job");
    System.out.println("Shen Li print : submit job " + wJobConf.getName());

    JobClient jobClient = new JobClient(jobConf);

    RunningJob rj = jobClient.submitJob(jobConf);

    if (monitor) {
      try {
        if(!jobClient.monitorAndPrintJob(jobConf, rj)) {
          System.out.println("Job Failed: " + rj.getFailureInfo());
          throw new IOException("Job Failed");
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    return rj;
  }

  public static void main(String args[]) {
    WJobConf wJobConf = new WJobConf(
        "SlotHolderTester",
        "/test/test.jar",
        "TestSlotHolder",
        null,
        null,
        null,
        2,
        2,
        20000,
        15000);

    WJobStatus wJobStatus = new WJobStatus(
        new WorkflowID("test_tracker", 21),
        null,
        null);

    SlotHolder slotHolder = new SlotHolder();
    try {
      slotHolder.submit(
          wJobConf, wJobStatus.getWorkflowID(), true);
    } catch (IOException ex) {
      System.out.println("IOException: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

}
