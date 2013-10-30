package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.util.*;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobTrackerNotYetInitializedException;
import org.apache.hadoop.mapred.SafeModeException;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//TODO: import JobClient should be removed once we have our 
//own retry policy for Workflow
import org.apache.hadoop.mapred.JobClient;

public class WorkflowClient extends Configured{

  private static final Log LOG = LogFactory.getLog(WorkflowClient.class);

  private WorkflowConf conf;

  private WorkflowSubmissionProtocol rpcWorkflowSubmitClient;
  private WorkflowSubmissionProtocol workflowSubmitClient;

  // the location in HDFS where workflow related files are placed.
  private Path stagingAreaDir = null;

  private UserGroupInformation ugi;

  public WorkflowClient() {
  }

  public WorkflowClient(WorkflowConf conf) throws IOException {
    setConf(conf);
    init(conf);
  }

  /**
   * initialize rpc proxies used to communicate with the JobTracker
   *
   * @param conf Workflow configuration information, which is used
   *             to get the JobTracker's URL.
   */
  public void init(WorkflowConf conf) throws IOException {
    String tracker = conf.get("mapred.job.tracker", "local");

    this.ugi = UserGroupInformation.getCurrentUser();

    //TODO: handle "local" case

    this.rpcWorkflowSubmitClient = 
        createRPCProxy(JobTracker.getAddress(conf), conf);
    this.workflowSubmitClient = 
        createProxy(this.rpcWorkflowSubmitClient, conf);
        
  }

  // TODO: remove when test is not needed
  public String testGetNewWorkflowId() throws IOException{
    WorkflowID wfID = workflowSubmitClient.getNewWorkflowId();
    return wfID.toString();
  }

  /**
   * Create an RPC proxy to communicate with the JobTracker
   */
  private static WorkflowSubmissionProtocol createRPCProxy(
      InetSocketAddress addr,
      Configuration conf) throws IOException {
    
    //TODO: have workflow's own retry policy instead of using
    //JobClient's parameters.
    WorkflowSubmissionProtocol rpcWorkflowSubmitClient = 
        (WorkflowSubmissionProtocol)RPC.getProxy(
            WorkflowSubmissionProtocol.class,
            WorkflowSubmissionProtocol.versionID,
            addr,
            UserGroupInformation.getCurrentUser(),
            conf,
            NetUtils.getSocketFactory(conf, 
                                      WorkflowSubmissionProtocol.class),
            0,
            RetryUtils.getMultipleLinearRandomRetry(
                conf,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT
                ),
            false);
    return rpcWorkflowSubmitClient;
  }

  //TODO: specific 
  /**
   * wraps the RPC proxy.
   */
  private static WorkflowSubmissionProtocol createProxy(
      WorkflowSubmissionProtocol rpcWorkflowSubmitClient,
      Configuration conf) throws IOException {
    
    @SuppressWarnings("unchecked")
    RetryPolicy defaultPolicy = 
        RetryUtils.getDefaultRetryPolicy(
            conf,
            JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY,
            JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
            JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY,
            JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
            JobTrackerNotYetInitializedException.class,
            SafeModeException.class
            );

    final WorkflowSubmissionProtocol wsp = 
        (WorkflowSubmissionProtocol) RetryProxy.create(
            WorkflowSubmissionProtocol.class,
            rpcWorkflowSubmitClient,
            defaultPolicy, 
            new HashMap<String, RetryPolicy>()
            );

    RPC.checkVersion(WorkflowSubmissionProtocol.class,
        WorkflowSubmissionProtocol.versionID, wsp);
    return wsp;
  }


  public RunningWorkflow submitWorkflow(WorkflowConf wf)
      throws FileNotFoundException, IOException {
    try{
      return submitWorkflowInternal(wf);
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("class not found", cnfe);
    }
  }

  public Path getStagingAreaDir() throws IOException {
    if (null == stagingAreaDir) {
      stagingAreaDir = new Path(
          workflowSubmitClient.getWorkflowStagingAreaDir());
    }
    return stagingAreaDir;
  }

  private RunningWorkflow submitWorkflowInternal(
      final WorkflowConf wf) throws FileNotFoundException,
                                    ClassNotFoundException,
                                    InterruptedException,
                                    IOException {
    return ugi.doAs(new PrivilegedExceptionAction<RunningWorkflow>() {
      public RunningWorkflow run() throws 
          FileNotFoundException,
          ClassNotFoundException,
          InterruptedException,
          IOException {
        System.out.println("In WorkflowClient.submitWorkflowInternal");
        WorkflowConf wfCopy = wf;
        Path wfStagingArea = WorkflowSubmissionFiles.getStagingDir(
          getStagingAreaDir(), wfCopy);
        System.out.println("Before request ID");

        // get workflow id
        WorkflowID wfid = workflowSubmitClient.getNewWorkflowId();
        Path submitWfDir = new Path(wfStagingArea, wfid.toString());
        wfCopy.set("mapreduce.workflow.dir", submitWfDir.toString());

        WorkflowStatus status = null;
        // generate scheduling plan
        ClusterStatus clusterStatus = 
          workflowSubmitClient.getClusterStatus();
        // it is current a mix of map and reduce slots
        int maxSlots = clusterStatus.getMaxMapTasks()
                     + clusterStatus.getMaxReduceTasks();
        System.out.println("Before generating plan");
        wfCopy.generatePlan(maxSlots);

        System.out.println("Before copyAndConfigureFiles");
        copyAndConfigureFiles(wfCopy, submitWfDir);
        System.out.println("Before workflowSubmitClient.submitWorkflow");
        status = workflowSubmitClient.submitWorkflow(
            wfid, submitWfDir.toString(), ugi.getShortUserName());

        System.out.println("In WorkflowClient: status active Job " +
                           status.getActiveJobs().size());
        status.print();

        WorkflowProfile prof = 
          workflowSubmitClient.getWorkflowProfile(wfid);
        System.out.println("After workflowSubmitClient.getWorkflowProfile");

        // submit workflow job which submits all WJobs
        WJobSubmitter.submitSubmitterJobs(wfCopy, status);
        if (null != status && null != prof) {
          return new NetworkedWorkflow(status, prof, 
              workflowSubmitClient);
        } else {
          throw new IOException("Could not submit workflow");
        }
        //TODO: clean up the staging area if submission fails.
      }
    });
  }

  /**
   * 1. write WorkflowConf into a file
   * 2. copy jar files into HDFS
   * 3. modify WJobConfs to replace local path in WJobConf with 
   * hdfs path
   */
  private void copyAndConfigureFiles(WorkflowConf wf, Path submitWfDir) 
      throws IOException, InterruptedException {
    short replication = (short)wf.getInt("mapred.submit.replication", 1);

    FileSystem fs = submitWfDir.getFileSystem(wf);
    LOG.debug("wf default FileSystem: " + fs.getUri());

    if (fs.exists(submitWfDir)) {
      throw new IOException("Not submitting workflow. Workflow directory "
                            + submitWfDir + " already exists!! This is " +
                            "unexpected. Please check what's there in" +
                            " that directory!");
    }
    submitWfDir = fs.makeQualified(submitWfDir);
    FsPermission mapredSysPerms = 
        new FsPermission(WorkflowSubmissionFiles.WF_DIR_PERMISSION);
    FileSystem.mkdirs(fs, submitWfDir, mapredSysPerms);
    Path confsDir = WorkflowSubmissionFiles.getWfDistCacheConfs(submitWfDir);

    //write WorkflowConf Object into the confs dir
    FileSystem.mkdirs(fs, confsDir, mapredSysPerms);
    WorkflowSubmissionFiles.writeWfConf(fs, confsDir, wf, replication);
    //we don't need distributed cache for now as the conf will be small

    //copy jar files
    Hashtable<String, WJobConf> wJobs = wf.getWJobConfs();
    for (String name : wJobs.keySet()) {
      WJobConf wJobConf = wJobs.get(name);
      String strLocalJarPath = wJobConf.getJarPath();
      Path localJarPath = new Path(strLocalJarPath);

      File localJar = new File(strLocalJarPath);

      if (!localJar.exists()) {
        throw new IOException("Local jar file does not exist : " +
                              strLocalJarPath);
      }
      if (!localJar.isFile()) {
        throw new IOException(strLocalJarPath + " is not a file");
      }

      Path jarDirPath = new Path(submitWfDir, "jar/" + name);
      if (!fs.mkdirs(jarDirPath)) {
        throw new IOException("Cannot create jar path " 
                              + jarDirPath.toString());
      }

      Path jarPath = new Path(jarDirPath, localJarPath.getName());
      fs.copyFromLocalFile(localJarPath, jarPath);
      wJobConf.setJarPath(jarPath.toString());

    }

    System.out.println("Write done!!!");
    


  }


  /**
   * The implementation of the RunningWorkflow. Implements the detailed 
   * actions to pull WorkflowStatus related information from JobTrackers.
   */
  static class NetworkedWorkflow implements RunningWorkflow {

    private WorkflowSubmissionProtocol workflowSubmitClient;
    private WorkflowStatus status;
    private WorkflowProfile profile;
    private long statusTime;

    /**
     * The constructor gets WorkflowStatus and WorkflowSubmissionProtocol
     * from WorkflowSubmissionClient
     */
    public NetworkedWorkflow(
          WorkflowStatus status, WorkflowProfile profile, 
          WorkflowSubmissionProtocol workflowSubmitClient)
        throws IOException {
      this.status = status;
      this.profile = profile;
      this.workflowSubmitClient = workflowSubmitClient;
        
      if (null == this.status) {
        throw new IOException(
            "Workflow Status cannot be null " + 
            "when constructing NetworkedWorkflow object");
      }
      if (null == this.profile) {
        throw new IOException(
            "Workflow Profile cannot be null " +
            "when constructing NetworkedWorkflow object");
      }
      if (null == this.workflowSubmitClient) {
        throw new IOException(
            "Workflow Submission Protocol cannot be null " +
            "when constructing NetworkedWorkflow object");
      }
      this.statusTime = System.currentTimeMillis();
    }

    /**
     * pulling latest WorkflowStatus from JobTracker.
     */
    private synchronized void updateStatus() throws IOException {
      // TODO: implement getWorkflowStatus on JobTracker
      this.status = workflowSubmitClient.getWorkflowStatus(
                        profile.getWorkflowID());
      if (null == this.status) {
        throw new IOException(
            "JobTracker cannot find workflow " + 
            profile.getWorkflowID());
      }
      this.statusTime = System.currentTimeMillis();
    }

    public WorkflowStatus getWorkflowStatus() throws IOException {
      updateStatus();
      return status;
    }

  }

}
