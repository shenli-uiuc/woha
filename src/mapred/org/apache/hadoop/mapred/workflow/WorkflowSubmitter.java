package org.apache.hadoop.mapred.workflow;

import java.util.*;
import java.util.jar.*;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.lang.reflect.Method;
import java.lang.reflect.Array;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import org.xml.sax.SAXException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.RunJar;

/** 
 * Submit a WOHA workflow 
 *
 * The WorkflowSubmitter also submit all jobs in the workflow.
 * However, those jobs won't get initialized by the TaskScheduler
 * until they become active.
 */
public class WorkflowSubmitter {

  public static boolean checkInputs(WorkflowConf wf) {
    HashSet<String> inDs = wf.getInputDatasets();

    boolean isValid = true;
    Configuration conf = new Configuration();

    try{
      FileSystem fs = FileSystem.get(conf);

      for (String name : inDs) {
        String path = wf.getDatasetPath(name);
        System.out.println("checking input: " + path);
        Path inFile = new Path(path);
        if (!fs.exists(inFile)) {
          System.err.println("Input dataset " + path
              + " does not exist in the HDFS.");
          isValid = false;
        } else if(!fs.isFile(inFile)) {
          System.err.println("Input dataset " + path
              + " is not a file.");
          isValid = false;
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
      System.err.println(ex.getMessage());
      return false;
    }

    return isValid;
  }

  public static boolean checkJars(WorkflowConf wf) {

    Hashtable<String, WJobConf> jobs = wf.getWJobConfs();

    WJobConf job;
    String jarPath;
    String jarClass;
    JarFile jarFile;

    boolean isValid = true;

    // For debugging purpose, to access the job name from outside of
    // of the for loop.
    String curJobName = "UNINT";
    try{
      for (String name : jobs.keySet()) {
        curJobName = name;

        job = jobs.get(name);
        jarPath = job.getJarPath();
        jarClass = job.getJarClass();

        jarFile = new JarFile(jarPath);

        File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
        tmpDir.mkdirs();
        if (!tmpDir.isDirectory()) {
          System.err.println("File.mkdirs failed to create " + tmpDir);
          System.exit(-1);
        }

        final File workDir = File.createTempFile("hadoop-workflow-unjar",
                                                 "", tmpDir);

        workDir.delete();
        workDir.mkdirs();

        if (!workDir.isDirectory()) {
          System.err.println("File.mkdirs failed to create " + workDir);
          System.exit(-1);
        }

        Runtime.getRuntime().addShutdownHook( new Thread() {
            public void run() {
              try {
                FileUtil.fullyDelete(workDir);
              } catch (IOException ex) {
                ex.printStackTrace();
                System.err.println("Exception in shudownHook: " 
                                   + ex.getMessage());
              }
            }
          });

        File file = new File(jarPath);
        RunJar.unJar(file, workDir);

        ArrayList<URL> classPath = new ArrayList<URL>();
        classPath.add(new File(workDir + "/").toURL());
        classPath.add(file.toURL());
        classPath.add(new File(workDir, "classes/").toURL());
        File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
          for (File lib : libs) {
            classPath.add(lib.toURL());
          }
        }

        ClassLoader loader = 
          new URLClassLoader(classPath.toArray(new URL[0]));

        Thread.currentThread().setContextClassLoader(loader);
        System.out.println("Loading: " + jarClass);
        Class<?> mainClass = Class.forName(jarClass, true, loader);
        Method main = mainClass.getMethod("main", new Class[] {
            Array.newInstance(String.class, 0).getClass()
          });

      }
    } catch (IOException ex) {
      ex.printStackTrace();
      System.err.println(ex.getMessage());
      isValid = false;
    } catch (NoSuchMethodException ex) {
      ex.printStackTrace();
      System.err.println(ex.getMessage());
      isValid = false;
    } catch (SecurityException ex) {
      ex.printStackTrace();
      System.err.println(ex.getMessage());
      isValid = false;
    } catch (ClassNotFoundException ex) {
      ex.printStackTrace();
      System.err.println(ex.getMessage());
      isValid = false;
    }
    
    if (!isValid) {
      System.err.println("checkJars failed at job " + curJobName);
    }
    return isValid;
  }

  public static void main(String[] args) throws Throwable {
    String usage = "dag xml-file";

    if (args.length != 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    MultiWorkflowConf mwc = new MultiWorkflowConf(args[0]);
    Collection<WorkflowConf> wfs = mwc.getWorkflowConfs();
    for (WorkflowConf wf : wfs ) {
      wf.printWorkflowConf(); 
      boolean isValid = true;
      if (!checkInputs(wf)) {
        System.err.println("input checking failed");
        isValid = false;
      }

      if (!checkJars(wf)) {
        System.err.println("jar checking failed");
        isValid = false;
      }

      if (isValid) {
        //TODO: duplicate WorkflowConf parameter
        long waitLen = wf.getSubmitAt() - System.currentTimeMillis();
        if (waitLen > 0) {
          Thread.sleep(waitLen);
        }
        WorkflowClient wfClient = new WorkflowClient(wf);
        RunningWorkflow runningWorkflow = wfClient.submitWorkflow(wf);
        WorkflowStatus status = runningWorkflow.getWorkflowStatus();
        status.print();
        System.out.println("Workflow " + status.getWorkflowID() + "submitted!");
      } else {
        System.exit(-1);
      }
    }
  }

}
