package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WorkflowSubmissionFiles {
  private final static Log LOG = 
      LogFactory.getLog(WorkflowSubmissionFiles.class);

  // workflow submission directory is private
  final public static FsPermission WF_DIR_PERMISSION = 
    FsPermission.createImmutable((short) 0700);

  // workflow files are world-wide readable and owner writable
  final public static FsPermission WF_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644);

  // workflow configuration file name
  final public static String confFileName = "wf.conf";

  public static Path getWfDistCacheConfs(Path wfSubmitDir) {
    return new Path(wfSubmitDir, "confs");
  }

  /**
   * Here is a little bit different from JobSubmissionFiles, as I
   * do not pass WorkflowClient into this method.
   * 
   * @param stagingArea the staging area dir given by
   *                    WorkflowClient.getWorkflowStagingAreaDir()
   * @param conf        WorkflowConf
   */
  public static Path getStagingDir(Path stagingArea,
                                   Configuration conf) 
      throws IOException, InterruptedException {
    FileSystem fs = stagingArea.getFileSystem(conf);
    String realUser;
    String currentUser;

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    realUser = ugi.getShortUserName();
    currentUser = 
      UserGroupInformation.getCurrentUser().getShortUserName();
    if (fs.exists(stagingArea)) {
      FileStatus fsStatus = fs.getFileStatus(stagingArea);
      String owner = fsStatus.getOwner();
      if (!(owner.equals(currentUser) || owner.equals(realUser))) {
        throw new IOException(
            "The ownership on the workflow staging directory " +
            stagingArea + " is not as expected. " + 
            "It is owned by " + owner + ". The directory must " +
            "be owned by the submitter " + currentUser + " or " +
            "by " + realUser);
      }
      if (!fsStatus.getPermission().equals(WF_DIR_PERMISSION)) {
        LOG.info(
            "Permission on workflow staging directory " + 
            stagingArea +
            " are incorrect: " + fsStatus.getPermission() + 
            ". Fix it to correct value" + WF_DIR_PERMISSION);
      }
    } else {
      fs.mkdirs(stagingArea,
                new FsPermission(WF_DIR_PERMISSION));
    }
    return stagingArea;
  }

  // write the WorkflowConf object into the given confsDir
  public static void writeWfConf(FileSystem fs, 
                                 Path confsDir, 
                                 WorkflowConf wf,
                                 short replication) 
      throws IOException{
    Path outFile = new Path(confsDir, confFileName);
    if (fs.exists(outFile)) {
      throw new IOException("WorkflowConf file already exist in " +
                            outFile.toString() + " when attempting"
                            + " to write.");
    }

    FSDataOutputStream out = fs.create(outFile);
    wf.write(out);
    fs.setReplication(outFile, replication);
    out.close();
  }

  // read from the given confsDir to generate a WorkflowConf object
  public static WorkflowConf readWfConf(FileSystem fs,
                                        Path confsDir) 
      throws IOException, FileNotFoundException {
    Path inFile = new Path(confsDir, confFileName);
    if (!fs.exists(inFile)) {
      throw new IOException("WorkflowConf file does not exist in "
                            + inFile.toString() + " when attempting"
                            + " to read.");
    }
    if (!fs.isFile(inFile)) {
      throw new IOException(inFile.toString() + " is not a file!");
    }

    FSDataInputStream in = fs.open(inFile);
    WorkflowConf wf = new WorkflowConf();
    wf.readFields(in);
    in.close();
    return wf;
  }
}
