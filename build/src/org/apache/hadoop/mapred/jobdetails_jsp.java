package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.mapred.TaskGraphServlet;

public final class jobdetails_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d-MMM-yyyy HH:mm:ss");

	private static final long serialVersionUID = 1L;


 
  private void printTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                double completePercent,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    int failedTaskAttempts = 0;
    int killedTaskAttempts = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
      failedTaskAttempts += task.numTaskFailures();
      killedTaskAttempts += task.numKilledTasks();
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print("<tr><th><a href=\"jobtasks.jsp?jobid=" + jobId + 
              "&type="+ kind + "&pagenum=1\">" + kind + 
              "</a></th><td align=\"right\">" + 
              StringUtils.formatPercent(completePercent, 2) +
              ServletUtil.percentageGraph((int)(completePercent * 100), 80) +
              "</td><td align=\"right\">" + 
              totalTasks + 
              "</td><td align=\"right\">" + 
              ((pendingTasks > 0) 
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=pending\">" + pendingTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((runningTasks > 0) 
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=running\">" + runningTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((finishedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                "&pagenum=1" + "&state=completed\">" + finishedTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((killedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind +
                "&pagenum=1" + "&state=killed\">" + killedTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((failedTaskAttempts > 0) ? 
                  ("<a href=\"jobfailures.jsp?jobid=" + jobId + 
                   "&kind=" + kind + "&cause=failed\">" + failedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              " / " +
              ((killedTaskAttempts > 0) ? 
                  ("<a href=\"jobfailures.jsp?jobid=" + jobId + 
                   "&kind=" + kind + "&cause=killed\">" + killedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              "</td></tr>\n");
  }

  private void printJobLevelTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.isFailed()) {
        killedTasks += 1;
      }
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print(((runningTasks > 0)  
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=running\">" + " Running" + 
                 "</a>" 
               : ((pendingTasks > 0) ? " Pending" :
                 ((finishedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                "&pagenum=1" + "&state=completed\">" + " Successful"
                 + "</a>" 
               : ((killedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind +
                "&pagenum=1" + "&state=killed\">" + " Failed" 
                + "</a>" : "None")))));
  }
  
  private void printConfirm(JspWriter out, String jobId) throws IOException{
    String url = "jobdetails.jsp?jobid=" + jobId;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url+"\"></head>"
        + "<body><h3> Are you sure you want to kill " + jobId
        + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"kill\" />"
        + "<input type=\"submit\" name=\"kill\" value=\"Kill\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
  }
  

 
  public static class ErrorResponse {

    private final long errorCode;
    private final String errorDescription;

    // Constructor
    ErrorResponse(long ec, String ed) {

      errorCode = ec;
      errorDescription = ed;
    }

    // Getters
    public long getErrorCode() { return errorCode; }
    public String getErrorDescription() { return errorDescription; }
  }

  public static class JobDetailsResponse {

    /* Used internally by JobMetaInfo and JobTaskSummary. */
    public static class JobTaskStats {

      private final int numTotalTasks;
      private final int numPendingTasks;
      private final int numRunningTasks;
      private final int numFinishedTasks;
      private final int numKilledTasks;
      private final int numFailedTaskAttempts;
      private final int numKilledTaskAttempts;

      // Constructor
      JobTaskStats(JobInProgress jip, TaskType tt) {

        TaskInProgress[] tasks = jip.getTasks(tt);
        
        int totalTasks = tasks.length;

        int runningTasks = 0;
        int finishedTasks = 0;
        int killedTasks = 0;
        int failedTaskAttempts = 0;
        int killedTaskAttempts = 0;

        for (int i=0; i < totalTasks; ++i) {

          TaskInProgress task = tasks[i];

          if (task.isComplete()) {
            finishedTasks += 1;
          } else if (task.isRunning()) {
            runningTasks += 1;
          } else if (task.wasKilled()) {
            killedTasks += 1;
          }

          failedTaskAttempts += task.numTaskFailures();
          killedTaskAttempts += task.numKilledTasks();
        }

        int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 

        /* Done with calculations, now on to member assignments. */
        numTotalTasks = totalTasks;
        numPendingTasks = pendingTasks;
        numRunningTasks = runningTasks;
        numFinishedTasks = finishedTasks;
        numKilledTasks = killedTasks;
        numFailedTaskAttempts = failedTaskAttempts;
        numKilledTaskAttempts = killedTaskAttempts;
      }

      // Getters
      public int getNumTotalTasks() { return numTotalTasks; }
      public int getNumPendingTasks() { return numPendingTasks; }
      public int getNumRunningTasks() { return numRunningTasks; }
      public int getNumFinishedTasks() { return numFinishedTasks; }
      public int getNumKilledTasks() { return numKilledTasks; }
      public int getNumFailedTaskAttempts() { return numFailedTaskAttempts; }
      public int getNumKilledTaskAttempts() { return numKilledTaskAttempts; }
    }

    public static class JobMetaInfo {

      public static class EventTimingInfo {

        private final String timestamp;
        private final long durationSecs;

        // Constructor
        EventTimingInfo(long eventOccurrenceTimeMSecs, long previousEventOccurrenceTimeMSecs) {

          timestamp = dateFormat.format(new Date(eventOccurrenceTimeMSecs));
          durationSecs = (0 != previousEventOccurrenceTimeMSecs) ? 
            /* Pass the difference through Math.abs() to take care of cases 
             * where previousEventOccurrenceTimeMSecs is in the future (likely
             * used only as a hack, when an event is in-progress).
             */
            (Math.abs(eventOccurrenceTimeMSecs - previousEventOccurrenceTimeMSecs)/1000) : 
            0;
        }

        // Getters
        public String getTimestamp() { return timestamp; }
        public long getDurationSecs() { return durationSecs; }
      }

      private final String jobTrackerName;
      private final String jobName;
      private final String userName;
      private final String jobFileLocation;
      private final String jobSubmissionHostName;
      private final String jobSubmissionHostAddress;
      private final String jobSetupStatus;
      private final String jobCleanupStatus;
      private final String jobStatus;
      private final String jobStatusInfo;
      private final EventTimingInfo jobStartTimingInfo;
      private final EventTimingInfo jobFinishTimingInfo;
      private final int numFlakyTaskTrackers;
      private final String jobSchedulingInfo;

      // Constructor
      JobMetaInfo(JobInProgress jip, JobTracker jt) {

        jobTrackerName = StringUtils.simpleHostname(jt.getJobTrackerMachine());

        JobProfile jobProfile = jip.getProfile();
        jobName = jobProfile.getJobName();
        userName = jobProfile.getUser();
        jobFileLocation = jobProfile.getJobFile();
        jobSubmissionHostName = jip.getJobSubmitHostName();
        jobSubmissionHostAddress = jip.getJobSubmitHostAddress();

        JobStatus status = jip.getStatus();

        /* TODO XXX Expose JobACLs in a structured format. 
         * 
         * Can create a JobAclsInfo object that is a map, but that doesn't 
         * handle the case where this just prints "All users are allowed". 
         *
         *
         *        Map<JobACL, AccessControlList> jobAcls = status.getJobACLs();
         *        JSPUtil.printJobACLs(jt, jobAcls, out);
         */

        jobSetupStatus = deduceJobTaskStatus(jip, TaskType.JOB_SETUP);
        jobCleanupStatus = deduceJobTaskStatus(jip, TaskType.JOB_CLEANUP);

        switch (status.getRunState()) {
          
          case JobStatus.RUNNING:
            jobStatus = "Running";
            jobStatusInfo = null;
            jobStartTimingInfo = new EventTimingInfo(jip.getStartTime(), System.currentTimeMillis());
            /* A running job could not possibly have finished. */
            jobFinishTimingInfo = null;
            break;

          case JobStatus.SUCCEEDED:
            jobStatus = "Succeeded";
            jobStatusInfo = null;
            jobStartTimingInfo = new EventTimingInfo(jip.getStartTime(), 0);
            jobFinishTimingInfo = new EventTimingInfo(jip.getFinishTime(), jip.getStartTime());
            break;

          case JobStatus.FAILED:
            jobStatus = "Failed";
            jobStatusInfo = status.getFailureInfo();
            jobStartTimingInfo = new EventTimingInfo(jip.getStartTime(), 0);
            jobFinishTimingInfo = new EventTimingInfo(jip.getFinishTime(), jip.getStartTime());
            break;

          case JobStatus.KILLED:
            jobStatus = "Killed";
            jobStatusInfo = status.getFailureInfo();
            jobStartTimingInfo = new EventTimingInfo(jip.getStartTime(), 0);
            jobFinishTimingInfo = new EventTimingInfo(jip.getFinishTime(), jip.getStartTime());
            break;

          default:
            jobStatus = "Unknown";
            jobStatusInfo = "Unknown";
            jobStartTimingInfo = null;
            jobFinishTimingInfo = null;
        }

        numFlakyTaskTrackers = jip.getNoOfBlackListedTrackers();
        jobSchedulingInfo = (jip.getSchedulingInfo() != null) ? jip.getSchedulingInfo().toString() : null;
      }

      private String deduceJobTaskStatus(JobInProgress jip, TaskType tt) {

        JobTaskStats taskStats = new JobTaskStats(jip, tt);

        return ((taskStats.getNumRunningTasks() > 0)  
                   ? "Running" 
                   : ((taskStats.getNumPendingTasks() > 0) 
                     ? "Pending" 
                     : ((taskStats.getNumFinishedTasks() > 0) 
                       ?  "Successful"
                       : ((taskStats.getNumKilledTasks() > 0) 
                         ? "Failed" 
                         : "None"))));
      }

      // Getters
      public String getJobTrackerName() { return jobTrackerName; }
      public String getJobName() { return jobName; }
      public String getUserName() { return userName; }
      public String getJobFileLocation() { return jobFileLocation; }
      public String getJobSubmissionHostName() { return jobSubmissionHostName; }
      public String getJobSubmissionHostAddress() { return jobSubmissionHostAddress; }
      public String getJobSetupStatus() { return jobSetupStatus; }
      public String getJobCleanupStatus() { return jobCleanupStatus; }
      public String getJobStatus() { return jobStatus; }
      public String getJobStatusInfo() { return jobStatusInfo; }
      public EventTimingInfo getJobStartTimingInfo() { return jobStartTimingInfo; }
      public EventTimingInfo getJobFinishTimingInfo() { return jobFinishTimingInfo; }
      public int getNumFlakyTaskTrackers() { return numFlakyTaskTrackers; }
      public String getJobSchedulingInfo() { return jobSchedulingInfo; }
    }

    public static class JobTaskSummary {

      private final float progressPercentage;
      private final JobTaskStats taskStats;

      // Constructor
      JobTaskSummary(float pp, JobInProgress jip, TaskType tt) {

        progressPercentage = pp;
        taskStats = new JobTaskStats(jip, tt);
      }

      // Getters
      public float getProgressPercentage() { return progressPercentage; }
      public JobTaskStats getTaskStats() { return taskStats; }
    }

    public static class JobCounterGroupInfo {

      public static class JobCounterInfo {

        private final String name;
        private final long mapValue;
        private final long reduceValue;
        private final long totalValue;

        // Constructor
        JobCounterInfo(String n, long mv, long rv, long tv) {

          name = n;
          mapValue = mv;
          reduceValue = rv;
          totalValue = tv;
        }

        // Getters
        public String getName() { return name; }
        public long getMapValue() { return mapValue; }
        public long getReduceValue() { return reduceValue; }
        public long getTotalValue() { return totalValue; }
      }

      private final String groupName;
      private final Collection<JobCounterInfo> jobCountersInfo;

      // Constructor
      JobCounterGroupInfo(String gn) { 
        
        groupName = gn; 
      
        jobCountersInfo = new ArrayList<JobCounterInfo>();
      }

      // To add one JobCounterInfo object at a time.
      void addJobCounterInfo(JobCounterInfo jci) { jobCountersInfo.add(jci); }

      // Getters
      public String getGroupName() { return groupName; }
      public Collection<JobCounterInfo> getJobCountersInfo() { return jobCountersInfo; }
    }

    private final String jobId;
    private final JobMetaInfo metaInfo;
    private final JobTaskSummary mapTaskSummary;
    private final JobTaskSummary reduceTaskSummary;
    private final Collection<JobCounterGroupInfo> jobCounterGroupsInfo;

    private void populateJobCounterGroups(JobInProgress jip) {

      boolean isFine = true;

      Counters mapCounters = new Counters();
      isFine = jip.getMapCounters(mapCounters);
      mapCounters = (isFine? mapCounters: new Counters());

      Counters reduceCounters = new Counters();
      isFine = jip.getReduceCounters(reduceCounters);
      reduceCounters = (isFine? reduceCounters: new Counters());

      Counters totalCounters = new Counters();
      isFine = jip.getCounters(totalCounters);
      totalCounters = (isFine? totalCounters: new Counters());
          
      for (String groupName : totalCounters.getGroupNames()) {

        Counters.Group totalGroup = totalCounters.getGroup(groupName);
        Counters.Group mapGroup = mapCounters.getGroup(groupName);
        Counters.Group reduceGroup = reduceCounters.getGroup(groupName);

        JobCounterGroupInfo jobCounterGroupInfo = 
          new JobCounterGroupInfo(totalGroup.getDisplayName());

        for (Counters.Counter counter : totalGroup) {

          String name = counter.getDisplayName();
          long mapValue = mapGroup.getCounter(name);
          long reduceValue = reduceGroup.getCounter(name);
          long totalValue = counter.getCounter();

          jobCounterGroupInfo.addJobCounterInfo
            (new JobDetailsResponse.JobCounterGroupInfo.JobCounterInfo(name, mapValue, reduceValue, totalValue));
        }

        addJobCounterGroupInfo(jobCounterGroupInfo);
      }
    }

    // Constructor
    JobDetailsResponse(String ji, JobInProgress jip, JobTracker jt) {

      jobId = ji;
      
      metaInfo = new JobMetaInfo(jip, jt);

      JobStatus status = jip.getStatus();

      mapTaskSummary = new JobTaskSummary(status.mapProgress() * 100.0f, jip, TaskType.MAP);
      reduceTaskSummary = new JobTaskSummary(status.reduceProgress() * 100.0f, jip, TaskType.REDUCE);

      jobCounterGroupsInfo = new ArrayList<JobCounterGroupInfo>();
      populateJobCounterGroups(jip);
    }

    // To add one JobCounterGroupInfo object at a time.
    void addJobCounterGroupInfo(JobCounterGroupInfo jcgi) { jobCounterGroupsInfo.add(jcgi); }

    // Getters
    public String getJobId() { return jobId; }
    public JobMetaInfo getMetaInfo() { return metaInfo; }
    public JobTaskSummary getMapTaskSummary() { return mapTaskSummary; }
    public JobTaskSummary getReduceTaskSummary() { return reduceTaskSummary; }
    public Collection<JobCounterGroupInfo> getJobCounterGroupsInfo() { return jobCounterGroupsInfo; }
  }

  private static java.util.List _jspx_dependants;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    JspFactory _jspxFactory = null;
    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;


    try {
      _jspxFactory = JspFactory.getDefaultFactory();
      response.setContentType("text/html");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;

      out.write('\n');
      out.write('\n');
      out.write('\n');
      out.write('\n');
      out.write('\n');
      out.write("       \n\n");
      out.write('\n');
      out.write('\n');

  String response_format = request.getParameter("format");

  if (response_format != null) {
    /* Eventually, the HTML output should also be driven off of these *Response
     * objects. 
     * 
     * Someday. 
     */
    JobDetailsResponse theJobDetailsResponse = null;
    ErrorResponse theErrorResponse = null;

    JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
    String jobId = request.getParameter("jobid"); 

    if (jobId != null) {

      final JobID jobIdObj = JobID.forName(jobId);
      JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobIdObj,
          request, response);

      /* Proceed only if the user is authorized to view this job. */
      if (myJob.isViewJobAllowed()) {

        JobInProgress job = myJob.getJob();

        if (job != null) {

          theJobDetailsResponse = new JobDetailsResponse(jobId, job, tracker);

        } else {
          response.setStatus(HttpServletResponse.SC_NOT_FOUND);
          theErrorResponse = new ErrorResponse(4101, "JobID " + jobId + " Not Found");
        }
      } else {

        /* TODO XXX Try and factor out JSPUtil.setErrorAndForward() for re-use
         * (and to not make it blindly dispatch to job_authorization_error.jsp,
         * which is all HTML. 
         */

        /* TODO XXX Make this return JSON, not HTML. */

        /* Nothing to do here, since JSPUtil.setErrorAndForward() has already been 
         * called from inside JSPUtil.checkAccessAndGetJob().
         */
      }
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      theErrorResponse = new ErrorResponse(4100, "Missing JobID");
    }
    
    /* ------------ Response generation begins here ------------ */

    /* For now, "json" is the only supported format. 
     *
     * As more formats are supported, this should become a cascading 
     * if-elsif-else block.
     */
    if ("json".equals(response_format)) {

      response.setContentType("application/json");

      ObjectMapper responseObjectMapper = new ObjectMapper();
      /* A lack of an error response implies we have a meaningful 
       * application response? Why not!
       */
      out.println(responseObjectMapper.writeValueAsString
        ((theErrorResponse == null) ? theJobDetailsResponse : theErrorResponse));

    } else {
      response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }
  } else {

      out.write('\n');
   
    // Spit out HTML only in the absence of the "format" query parameter.
    response.setContentType("text/html; charset=UTF-8");

    final JobTracker tracker = (JobTracker) application.getAttribute(
      "job.tracker");
    String trackerName = 
      StringUtils.simpleHostname(tracker.getJobTrackerMachine());
    String jobId = request.getParameter("jobid"); 
    String refreshParam = request.getParameter("refresh");
    if (jobId == null) {
      out.println("<h2>Missing 'jobid'!</h2>");
      return;
    }
    
    int refresh = 60; // refresh every 60 seconds by default
    if (refreshParam != null) {
        try {
            refresh = Integer.parseInt(refreshParam);
        }
        catch (NumberFormatException ignored) {
        }
    }
    final JobID jobIdObj = JobID.forName(jobId);
    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobIdObj,
                                                     request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();

    final String newPriority = request.getParameter("prio");
    String user = request.getRemoteUser();
    UserGroupInformation ugi = null;
    if (user != null) {
      ugi = UserGroupInformation.createRemoteUser(user);
    }

    String action = request.getParameter("action");
    if(JSPUtil.privateActionsAllowed(tracker.conf) && 
        "changeprio".equalsIgnoreCase(action) 
        && request.getMethod().equalsIgnoreCase("POST")) {
      if (ugi != null) {
        try {
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws IOException{

              // checks job modify permission
              tracker.setJobPriority(jobIdObj, 
                  JobPriority.valueOf(newPriority));
              return null;
            }
          });
        } catch(AccessControlException e) {
          String errMsg = "User " + user + " failed to modify priority of " +
              jobIdObj + "!<br><br>" + e.getMessage() +
              "<hr><a href=\"jobdetails.jsp?jobid=" + jobId +
              "\">Go back to Job</a><br>";
          JSPUtil.setErrorAndForward(errMsg, request, response);
          return;
        }
      }
      else {// no authorization needed
        tracker.setJobPriority(jobIdObj,
             JobPriority.valueOf(newPriority));;
      }
    }
    
    if(JSPUtil.privateActionsAllowed(tracker.conf)) {
      action = request.getParameter("action");
      if(action!=null && action.equalsIgnoreCase("confirm")) {
        printConfirm(out, jobId);
        return;
      }
      else if(action != null && action.equalsIgnoreCase("kill") &&
          request.getMethod().equalsIgnoreCase("POST")) {
        if (ugi != null) {
          try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                // checks job modify permission
                tracker.killJob(jobIdObj);// checks job modify permission
                return null;
              }
            });
          } catch(AccessControlException e) {
            String errMsg = "User " + user + " failed to kill " + jobIdObj +
                "!<br><br>" + e.getMessage() +
                "<hr><a href=\"jobdetails.jsp?jobid=" + jobId +
                "\">Go back to Job</a><br>";
            JSPUtil.setErrorAndForward(errMsg, request, response);
            return;
          }
        }
        else {// no authorization needed
          tracker.killJob(jobIdObj);
        }
      }
    }

      out.write("\n\n\n<!DOCTYPE html>\n<html>\n<head>\n  ");
 
  if (refresh != 0) {
      
      out.write("\n      <meta http-equiv=\"refresh\" content=\"");
      out.print(refresh);
      out.write("\">\n      ");

  }
  
      out.write("\n<title>Hadoop ");
      out.print(jobId);
      out.write(" on ");
      out.print(trackerName);
      out.write("</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n<h1>Hadoop ");
      out.print(jobId);
      out.write(" on <a href=\"jobtracker.jsp\">");
      out.print(trackerName);
      out.write("</a></h1>\n\n");
 
    if (job == null) {
      String historyFile = JobHistory.getHistoryFilePath(jobIdObj);
      if (historyFile == null) {
        out.println("<h2>Job " + jobId + " not known!</h2>");
        return;
      }
      String historyUrl = JobHistoryServer.getHistoryUrlPrefix(tracker.conf) +
          "/jobdetailshistory.jsp?logFile=" +
          JobHistory.JobInfo.encodeJobHistoryFilePath(historyFile);
      response.sendRedirect(response.encodeRedirectURL(historyUrl));
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();
    int flakyTaskTrackers = job.getNoOfBlackListedTrackers();
    out.print("<b>User:</b> " +
        HtmlQuoting.quoteHtmlChars(profile.getUser()) + "<br>\n");
    out.print("<b>Job Name:</b> " +
        HtmlQuoting.quoteHtmlChars(profile.getJobName()) + "<br>\n");
    out.print("<b>Job File:</b> <a href=\"jobconf.jsp?jobid=" + jobId + "\">" +
        profile.getJobFile() + "</a><br>\n");
    out.print("<b>Submit Host:</b> " +
        HtmlQuoting.quoteHtmlChars(job.getJobSubmitHostName()) + "<br>\n");
    out.print("<b>Submit Host Address:</b> " +
        HtmlQuoting.quoteHtmlChars(job.getJobSubmitHostAddress()) + "<br>\n");

    Map<JobACL, AccessControlList> jobAcls = status.getJobACLs();
    JSPUtil.printJobACLs(tracker, jobAcls, out);
    out.print("<b>Job Setup:</b>");
    printJobLevelTaskSummary(out, jobId, "setup", 
                             job.getTasks(TaskType.JOB_SETUP));
    out.print("<br>\n");
    if (runState == JobStatus.RUNNING) {
      out.print("<b>Status:</b> Running<br>\n");
      out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
      out.print("<b>Running for:</b> " + StringUtils.formatTimeDiff(
          System.currentTimeMillis(), job.getStartTime()) + "<br>\n");
    } else {
      if (runState == JobStatus.SUCCEEDED) {
        out.print("<b>Status:</b> Succeeded<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Finished at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Finished in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.FAILED) {
        out.print("<b>Status:</b> Failed<br>\n");
        out.print("<b>Failure Info:</b>" + 
                   HtmlQuoting.quoteHtmlChars(status.getFailureInfo()) + "<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Failed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Failed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.KILLED) {
        out.print("<b>Status:</b> Killed<br>\n");
        out.print("<b>Failure Info:</b>" + 
                   HtmlQuoting.quoteHtmlChars(status.getFailureInfo()) + "<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Killed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Killed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      }
    }
    out.print("<b>Job Cleanup:</b>");
    printJobLevelTaskSummary(out, jobId, "cleanup", 
                             job.getTasks(TaskType.JOB_CLEANUP));
    out.print("<br>\n");
    if (flakyTaskTrackers > 0) {
      out.print("<b>Black-listed TaskTrackers:</b> " + 
          "<a href=\"jobblacklistedtrackers.jsp?jobid=" + jobId + "\">" +
          flakyTaskTrackers + "</a><br>\n");
    }
    if (job.getSchedulingInfo() != null) {
      out.print("<b>Job Scheduling information: </b>" +
          job.getSchedulingInfo().toString() +"\n");
    }
    out.print("<hr>\n");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>Kind</th><th>% Complete</th><th>Num Tasks</th>" +
              "<th>Pending</th><th>Running</th><th>Complete</th>" +
              "<th>Killed</th>" +
              "<th><a href=\"jobfailures.jsp?jobid=" + jobId + 
              "\">Failed/Killed<br>Task Attempts</a></th></tr>\n");
    printTaskSummary(out, jobId, "map", status.mapProgress(), 
                     job.getTasks(TaskType.MAP));
    printTaskSummary(out, jobId, "reduce", status.reduceProgress(),
                     job.getTasks(TaskType.REDUCE));
    out.print("</table>\n");
    
    
      out.write("\n    <p/>\n    <table border=2 cellpadding=\"5\" cellspacing=\"2\">\n    <tr>\n      <th><br/></th>\n      <th>Counter</th>\n      <th>Map</th>\n      <th>Reduce</th>\n      <th>Total</th>\n    </tr>\n    ");

    boolean isFine = true;
    Counters mapCounters = new Counters();
    isFine = job.getMapCounters(mapCounters);
    mapCounters = (isFine? mapCounters: new Counters());
    Counters reduceCounters = new Counters();
    isFine = job.getReduceCounters(reduceCounters);
    reduceCounters = (isFine? reduceCounters: new Counters());
    Counters totalCounters = new Counters();
    isFine = job.getCounters(totalCounters);
    totalCounters = (isFine? totalCounters: new Counters());
        
    for (String groupName : totalCounters.getGroupNames()) {
      Counters.Group totalGroup = totalCounters.getGroup(groupName);
      Counters.Group mapGroup = mapCounters.getGroup(groupName);
      Counters.Group reduceGroup = reduceCounters.getGroup(groupName);
      
      Format decimal = new DecimalFormat();
      
      boolean isFirst = true;
      for (Counters.Counter counter : totalGroup) {
        String displayName = counter.getDisplayName();
        String name = counter.getName();
        String mapValue = decimal.format(mapGroup.getCounter(name));
        String reduceValue = decimal.format(reduceGroup.getCounter(name));
        String totalValue = decimal.format(counter.getCounter());
        
      out.write("\n        <tr>\n          ");

          if (isFirst) {
            isFirst = false;
            
      out.write("\n            <td rowspan=\"");
      out.print(totalGroup.size());
      out.write("\">\n            ");
      out.print(HtmlQuoting.quoteHtmlChars(totalGroup.getDisplayName()));
      out.write("</td>\n            ");

          }
          
      out.write("\n          <td>");
      out.print(HtmlQuoting.quoteHtmlChars(displayName));
      out.write("</td>\n          <td align=\"right\">");
      out.print(mapValue);
      out.write("</td>\n          <td align=\"right\">");
      out.print(reduceValue);
      out.write("</td>\n          <td align=\"right\">");
      out.print(totalValue);
      out.write("</td>\n        </tr>\n        ");

      }
    }
    
      out.write("\n    </table>\n\n<hr>Map Completion Graph - \n");

if("off".equals(request.getParameter("map.graph"))) {
  session.setAttribute("map.graph", "off");
} else if("on".equals(request.getParameter("map.graph"))){
  session.setAttribute("map.graph", "on");
}
if("off".equals(request.getParameter("reduce.graph"))) {
  session.setAttribute("reduce.graph", "off");
} else if("on".equals(request.getParameter("reduce.graph"))){
  session.setAttribute("reduce.graph", "on");
}

if("off".equals(session.getAttribute("map.graph"))) { 
      out.write("\n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&map.graph=on\" > open </a>\n");
} else { 
      out.write(" \n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&map.graph=off\" > close </a>\n<br><embed src=\"/taskgraph?type=map&jobid=");
      out.print(jobId);
      out.write("\" \n       width=\"");
      out.print(TaskGraphServlet.width + 2 * TaskGraphServlet.xmargin);
      out.write("\" \n       height=\"");
      out.print(TaskGraphServlet.height + 3 * TaskGraphServlet.ymargin);
      out.write("\"\n       style=\"width:100%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />\n");
}
      out.write('\n');
      out.write('\n');
if(job.getTasks(TaskType.REDUCE).length > 0) { 
      out.write("\n<hr>Reduce Completion Graph -\n");
if("off".equals(session.getAttribute("reduce.graph"))) { 
      out.write("\n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&reduce.graph=on\" > open </a>\n");
} else { 
      out.write(" \n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&reduce.graph=off\" > close </a>\n \n <br><embed src=\"/taskgraph?type=reduce&jobid=");
      out.print(jobId);
      out.write("\" \n       width=\"");
      out.print(TaskGraphServlet.width + 2 * TaskGraphServlet.xmargin);
      out.write("\" \n       height=\"");
      out.print(TaskGraphServlet.height + 3 * TaskGraphServlet.ymargin);
      out.write("\" \n       style=\"width:100%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />\n");
} }
      out.write("\n\n<hr>\n");
 if(JSPUtil.privateActionsAllowed(tracker.conf)) { 
      out.write("\n  <table border=\"0\"> <tr> <td>\n  Change priority from ");
      out.print(job.getPriority());
      out.write(" to:\n  <form action=\"jobdetails.jsp\" method=\"post\">\n  <input type=\"hidden\" name=\"action\" value=\"changeprio\"/>\n  <input type=\"hidden\" name=\"jobid\" value=\"");
      out.print(jobId);
      out.write("\"/>\n  </td><td> <select name=\"prio\"> \n  ");

    JobPriority jobPrio = job.getPriority();
    for (JobPriority prio : JobPriority.values()) {
      if(jobPrio != prio) {
        
      out.write(" <option value=");
      out.print(prio);
      out.write('>');
      out.print(prio);
      out.write("</option> ");

      }
    }
  
      out.write("\n  </select> </td><td><input type=\"submit\" value=\"Submit\"> </form></td></tr> </table>\n");
 } 
      out.write("\n\n<table border=\"0\"> <tr>\n    \n");
 if(JSPUtil.privateActionsAllowed(tracker.conf) 
    	&& runState == JobStatus.RUNNING) { 
      out.write("\n\t<br/><a href=\"jobdetails.jsp?action=confirm&jobid=");
      out.print(jobId);
      out.write("\"> Kill this job </a>\n");
 } 
      out.write("\n\n<hr>\n\n<hr>\n<a href=\"jobtracker.jsp\">Go back to JobTracker</a><br>\n");

out.println(ServletUtil.htmlFooter());

      out.write('\n');

} // if (response_format != null) 

      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      if (_jspxFactory != null) _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
