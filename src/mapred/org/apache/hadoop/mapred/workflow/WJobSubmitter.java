package org.apache.hadoop.mapred.workflow;

import java.util.jar.*;
import java.lang.reflect.*;
import java.util.*;
import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class WJobSubmitter {

  public static final String STR_TMP_DIR = "/shen/sys/input/";

  public static class Submitter extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, IntWritable> {
        
    public void map(LongWritable key, Text path,
                    OutputCollector<Text, IntWritable> out,
                    Reporter reporter) {
      try {
        Path wJobPath = new Path(path.toString());
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(wJobPath)) {
          throw new IOException("WJobInProgress file does not exist: "
                                + wJobPath.toString());
        }
        if (!fs.isFile(wJobPath)) {
          throw new IOException("WJobInProgress path does not point"
                                + " to a file: " + wJobPath.toString());
        }

        FSDataInputStream in = fs.open(wJobPath);
        WJobConf wJobConf = new WJobConf();
        WorkflowID wfid = new WorkflowID();
        wfid.readFields(in);
        wJobConf.readFields(in);
        in.close();

        submitWJob(wJobConf, wfid, new Configuration());

        out.collect(path, new IntWritable(0));
      } catch (IOException ex) {
        reporter.setStatus(ex.getMessage());
      }
    }
  }

  public static class SubmitterDummyReducer extends MapReduceBase
      implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> out,
                       Reporter reporter) throws IOException {
    
    }

  }

  public static void submitSubmitterJobs(
                             WorkflowConf wfConf,
                             WorkflowStatus wfStatus) 
    throws IOException {

      Hashtable<String, WJobConf> wJobConfs = wfConf.getWJobConfs();
      WorkflowID wfid = wfStatus.getWorkflowID();

      for (String jobName : wJobConfs.keySet()) {
        JobConf jobConf = new JobConf(WJobSubmitter.class);
        jobConf.setWorkflowID(wfid);
        String submitterName = WJobStatus.SUBMITTER_PREFIX + jobName;
        jobConf.setJobName(submitterName);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(Submitter.class);
        jobConf.setNumMapTasks(1);

        jobConf.setReducerClass(SubmitterDummyReducer.class);
        jobConf.setNumReduceTasks(0);

        jobConf.setInputFormat(SequenceFileInputFormat.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);

        // Write parts, one wjip per part
        final Path inDir = new Path(STR_TMP_DIR + 
            wfid.toString() + "/inDir/" + submitterName + "/");
        // we have nothing to write into outDir
        // but we have to setup this to obey hadoop's framework
        final Path outDir = new Path(STR_TMP_DIR +
            wfid.toString() + "/outDir/" + submitterName + "/");
        final Path dataDir = new Path(STR_TMP_DIR +
            wfid.toString() + "/dataDir/" + submitterName + "/");

        FileInputFormat.setInputPaths(jobConf, inDir);
        FileOutputFormat.setOutputPath(jobConf, outDir);

        final FileSystem fs = FileSystem.get(jobConf);
        if (!fs.mkdirs(inDir)) {
          throw new IOException("Cannot create input directory " + inDir);
        }
        if (!fs.mkdirs(dataDir)) {
          throw new IOException("Cannot create data directory " + dataDir);
        }

        WJobConf wJobConf = wJobConfs.get(jobName);
        final Path inFilePath = new Path(inDir, "part" + 0);
        final Path dataFilePath = new Path(dataDir, jobName);

        FSDataOutputStream data = fs.create(dataFilePath);
        wfid.write(data);
        wJobConf.write(data);
        data.close();

        final SequenceFile.Writer writer = SequenceFile.createWriter(
            fs, jobConf, inFilePath, LongWritable.class, Text.class, 
            CompressionType.NONE);

        try {
          writer.append(new LongWritable(0), 
              new Text(dataFilePath.toString()));
        } finally {
          writer.close();
        }

        JobClient jobClient = new JobClient(jobConf);
        WJobStatus wJobStatus = wfStatus.getWJobStatus(jobName);
        boolean res = wJobStatus.setSubmitterID(jobClient.submitJob(jobConf).getID());
        System.out.println("Submitted Job : " + res + ", " 
                           + submitterName + " with id "
                           + (null == wJobStatus.getSubmitterID()));
      }

    }

  /**
   * same as unjar in RunJar.java
   */
  public static void unJar(File jarFile, File toDir) 
    throws IOException {
      JarFile jar = new JarFile(jarFile);
      try {
        Enumeration entries = jar.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = (JarEntry)entries.nextElement();
          if (!entry.isDirectory()) {
            InputStream in = jar.getInputStream(entry);
            try {
              File file = new File(toDir, entry.getName());
              if (!file.getParentFile().mkdirs()) {
                if (!file.getParentFile().isDirectory()) {
                  throw new IOException("Mkdirs failed to create "
                      + file.getParentFile().toString());
                }
              }
              OutputStream out = new FileOutputStream(file);
              try {
                byte[] buffer = new byte[8192];
                int i;
                while ((i = in.read(buffer)) != -1) {
                  out.write(buffer, 0, i);
                }
              } finally {
                out.close();
              }
            } finally {
              in.close();
            }
          }
        }
      } finally {
        jar.close();
      }
    }

  /**
   * All exceptions need to be handled locally, as this method will
   * be called in TaskTracker. We need to make sure this does not mess
   * up TaskTracker
   *
   * @param wjip The WJobInProgress object got from JobTracker. (TODO:
   *             read wjip from HDFS rather than go through network.
   * @param conf The TaskTracker's Configuration.
   */
  public static void submitWJob(WJobConf wJobConf,
      WorkflowID wfid,
      Configuration conf) 
    throws IOException {
      try {
        String strJarPath = wJobConf.getJarPath();
        String strJarClass = wJobConf.getJarClass();

        FileSystem fs = FileSystem.get(conf);
        // This is the jar path in HDFS, during submission
        // the WorkflowClient has replaced the local path in 
        // WJobConf with the HDFS path
        Path jarPath = new Path(strJarPath);

        String strWfid = wfid.toString();

        if (!fs.exists(jarPath)) {
          throw new IOException("jar file for job " + wJobConf.getName() + 
              " of workflow " + strWfid + " does not exist");
        }
        if (!fs.isFile(jarPath)) {
          throw new IOException("jar path for job " + wJobConf.getName() +
              " of workflow " + strWfid + " is not a file");
        }

        // jar file exists and is a file
        // Now copy to local and load the jar file

        // create staging dir in local file system
        String strTmpDir = new Configuration().get("hadoop.tmp.dir");
        String strLocalJarDirPath = strTmpDir +  
          "/jars/" + strWfid + "/" +wJobConf.getName() + "/";

        File localJarDir = new File(strLocalJarDirPath);
        if (!localJarDir.mkdirs()) {
          throw new IOException("Local jar dir for job " +
              wJobConf.getName() + " of workflow " + strWfid + 
              "creation failed");
        }

        // copy the jar file from dfs to local fs
        String strLocalJarPath = strLocalJarDirPath + jarPath.getName();
        Path localJarPath = new Path(strLocalJarPath);
        fs.copyToLocalFile(jarPath, localJarPath);

        // open the jar file
        File file = new File(strLocalJarPath);

        if (!file.exists() || !file.isFile()) {
          throw new IOException("Cannot find local jar file");
        }

        JarFile jarFile;
        try {
          jarFile = new JarFile(strLocalJarPath);
        } catch (IOException ex) {
          throw new IOException("Error opening local jar file: " +
              strLocalJarPath, ex);
        }

        // no need to read the main class from manifest for now,
        // as the jarClass is mandatory

        String mainClassName = strJarClass.replaceAll("/", ".");

        final File workDir = new File(strLocalJarDirPath + "unjar");
        if (!workDir.mkdirs()) {
          throw new IOException("unjar dir for job " + wJobConf.getName()
              + " of workflow " + strWfid + "creation failed");
        }

        //TODO: addShutdownHook, or do I need that?

        unJar(file, workDir);

        ArrayList<URL> classPath = new ArrayList<URL>();
        classPath.add(new File(workDir + "/").toURL());
        classPath.add(file.toURL());
        classPath.add(new File(workDir, "classes/").toURL());
        File[] libs = new File(workDir, "lib").listFiles();
        if (null != libs) {
          for (File lib : libs) {
            classPath.add(lib.toURL());
          }
        }

        ClassLoader loader = 
          new URLClassLoader(classPath.toArray(new URL[0]));

        Thread.currentThread().setContextClassLoader(loader);
        Class<?> mainClass = Class.forName(mainClassName, 
            true, loader);

        //TODO support user defined arguments 
        Class[] cArgs = new Class[2];
        cArgs[0] = WJobConf.class;
        cArgs[1] = WorkflowID.class;
        Method submit = 
          mainClass.getMethod("submit", cArgs);

        //Method main = mainClass.getMethod("main", new Class[] {
        //  Array.newInstance(String.class, 0).getClass()    
        //});
        //TODO: implement deep clone for wjip?
        submit.invoke(null, new Object[] {wJobConf, wfid});
        //String[] newArgs = {"hello", "world"};
        //main.invoke(null, new Object[] {newArgs});
      } catch (IOException ex) {
        throw ex;
      } catch (InvocationTargetException ex) {
        throw new IOException("InvocationTargetException in " +
            "SubmitWJob(...) : " + ex.getMessage());
      } catch (ClassNotFoundException ex) {
        throw new IOException("ClassNotFoundException in " +
            "SubmitWJob(...) : " + ex.getMessage());
      } catch (NoSuchMethodException ex) {
        throw new IOException("NoSuchMethodException in " +
            "SubmitWJob(...) : " + ex.getMessage());
      } catch (IllegalAccessException ex) {
        throw new IOException("IllegalAccessException in " +
            "SubmitWJob(...) : " + ex.getMessage());
      }
    }

  /**
   * For test purpose
   */
  public static void main(String [] argv) {
    WJobConf wJobConf = new WJobConf(
        "WJobSubmitterTester",
        "/shen/exp/jar/slotholder.jar",
        "org.cyphy.SlotHolder",
        null,
        null,
        null,
        2,
        2,
        20000,
        15000);

    WJobStatus wJobStatus = new WJobStatus(
        new WorkflowID("test_submitter", 16), 2, 2);

    try {
      submitWJob(wJobConf, 
          wJobStatus.getWorkflowID(), 
          new Configuration());
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println(ex.getMessage());
    }
  }
}
