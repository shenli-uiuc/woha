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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.codehaus.jackson.map.ObjectMapper;

public final class jobtracker_jsp extends org.apache.jasper.runtime.HttpJspBase
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


  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");
  
  public void generateSummaryTable(JspWriter out, ClusterMetrics metrics,
                                   JobTracker tracker) throws IOException {
    String tasksPerNode = metrics.getTaskTrackerCount() > 0 ?
      percentFormat.format(((double)(metrics.getMapSlotCapacity() +
      metrics.getReduceSlotCapacity())) / metrics.getTaskTrackerCount()):
      "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"+
              "<tr><th>Running Map Tasks</th><th>Running Reduce Tasks</th>" + 
              "<th>Total Submissions</th>" +
              "<th>Nodes</th>" + 
              "<th>Occupied Map Slots</th><th>Occupied Reduce Slots</th>" + 
              "<th>Reserved Map Slots</th><th>Reserved Reduce Slots</th>" + 
              "<th>Map Task Capacity</th>" +
              "<th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th>" + 
              "<th>Blacklisted Nodes</th>" +
              "<th>Graylisted Nodes</th>" +
              "<th>Excluded Nodes</th></tr>\n");
    out.print("<tr><td>" + metrics.getRunningMaps() + "</td><td>" +
              metrics.getRunningReduces() + "</td><td>" + 
              metrics.getTotalJobSubmissions() +
              "</td><td><a href=\"machines.jsp?type=active\">" +
              metrics.getTaskTrackerCount() + "</a></td><td>" + 
              metrics.getOccupiedMapSlots() + "</td><td>" +
              metrics.getOccupiedReduceSlots() + "</td><td>" + 
              metrics.getReservedMapSlots() + "</td><td>" +
              metrics.getReservedReduceSlots() + "</td><td>" + 
              metrics.getMapSlotCapacity() +
              "</td><td>" + metrics.getReduceSlotCapacity() +
              "</td><td>" + tasksPerNode +
              "</td><td><a href=\"machines.jsp?type=blacklisted\">" +
              metrics.getBlackListedTaskTrackerCount() + "</a>" +
              "</td><td><a href=\"machines.jsp?type=graylisted\">" +
              metrics.getGrayListedTaskTrackerCount() + "</a>" +
              "</td><td><a href=\"machines.jsp?type=excluded\">" +
              metrics.getDecommissionedTaskTrackerCount() + "</a>" +
              "</td></tr></table>\n");

    out.print("<br>");
    if (tracker.hasRestarted()) {
      out.print("<span class=\"small\"><i>");
      if (tracker.hasRecovered()) {
        out.print("The JobTracker got restarted and recovered back in " );
        out.print(StringUtils.formatTime(tracker.getRecoveryDuration()));
      } else {
        out.print("The JobTracker got restarted and is still recovering");
      }
      out.print("</i></span>");
    }
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

  public static class JobTrackerResponse {

    public static class JobTrackerMetaInfo {

      private final String jobTrackerName;
      private final String status;
      private final String startTimestamp;
      private final String version;
      private final String compilationInfo;
      private final String identifier;
      private final String safeModeStatus;
      private final boolean hasRestarted;
      private final boolean hasRecovered;
      private final long recoveryDurationSecs;

      // Constructor
      JobTrackerMetaInfo(JobTracker jt) {

        jobTrackerName = StringUtils.simpleHostname(jt.getJobTrackerMachine());
        status = jt.getClusterStatus().getJobTrackerState().toString();
        startTimestamp = dateFormat.format(new Date(jt.getStartTime()));
        version = VersionInfo.getVersion() + ", revision " + VersionInfo.getRevision();
        compilationInfo = VersionInfo.getDate() + " by " + VersionInfo.getUser();
        identifier = jt.getTrackerIdentifier();
        safeModeStatus = jt.getSafeModeText();
        hasRestarted = jt.hasRestarted();
        hasRecovered = jt.hasRecovered();

        if (hasRestarted && hasRecovered) {
          recoveryDurationSecs = jt.getRecoveryDuration() / 1000;
        } else {
          recoveryDurationSecs = 0;
        }
      }

      // Getters
      public String getJobTrackerName() { return jobTrackerName; }
      public String getStatus() { return status; }
      public String getStartTimestamp() { return startTimestamp; }
      public String getVersion() { return version; }
      public String getCompilationInfo() { return compilationInfo; }
      public String getIdentifier() { return identifier; }
      public String getSafeModeStatus() { return safeModeStatus; }
      public boolean getHasRestarted() { return hasRestarted; }
      public boolean getHasRecovered() { return hasRecovered; }
      public long getRecoveryDurationSecs() { return recoveryDurationSecs; }
    }

    public static class JobTrackerClusterSummary {

      private final long usedHeapMemoryBytes;
      private final long totalHeapMemoryBytes;
      private final long numTotalTaskTrackers;
      private final long numBlackListedTaskTrackers;
      private final long numGrayListedTaskTrackers;
      private final long numDecommissionedTaskTrackers;
      private final long runningMapTasks;
      private final long runningReduceTasks;
      private final long totalJobSubmissions;
      private final long occupiedMapSlots;
      private final long occupiedReduceSlots;
      private final long reservedMapSlots;
      private final long reservedReduceSlots;
      private final long mapTaskCapacity;
      private final long reduceTaskCapacity;
      private final float avgTasksPerTaskTracker;

      // Constructor
      JobTrackerClusterSummary(JobTracker jt) {

        usedHeapMemoryBytes = Runtime.getRuntime().totalMemory();
        totalHeapMemoryBytes = Runtime.getRuntime().maxMemory();

        ClusterMetrics metrics = jt.getClusterMetrics();

        numTotalTaskTrackers = metrics.getTaskTrackerCount();
        numBlackListedTaskTrackers = metrics.getBlackListedTaskTrackerCount();
        numGrayListedTaskTrackers = metrics.getGrayListedTaskTrackerCount();
        numDecommissionedTaskTrackers = metrics.getDecommissionedTaskTrackerCount();
        runningMapTasks = metrics.getRunningMaps();
        runningReduceTasks = metrics.getRunningReduces();
        totalJobSubmissions = metrics.getTotalJobSubmissions();
        occupiedMapSlots = metrics.getOccupiedMapSlots();
        occupiedReduceSlots = metrics.getOccupiedReduceSlots();
        reservedMapSlots = metrics.getReservedMapSlots();
        reservedReduceSlots = metrics.getReservedReduceSlots();
        mapTaskCapacity = metrics.getMapSlotCapacity();
        reduceTaskCapacity = metrics.getReduceSlotCapacity();
        avgTasksPerTaskTracker = (numTotalTaskTrackers > 0) ? 
          (float)(((double)(mapTaskCapacity + reduceTaskCapacity)) / numTotalTaskTrackers) : 0;
      }

      // Getters
      public long getUsedHeapMemoryBytes() { return usedHeapMemoryBytes; }
      public long getTotalHeapMemoryBytes() { return totalHeapMemoryBytes; }
      public long getNumTotalTaskTrackers() { return numTotalTaskTrackers; }
      public long getNumBlackListedTaskTrackers() { return numBlackListedTaskTrackers; }
      public long getNumGrayListedTaskTrackers() { return numGrayListedTaskTrackers; }
      public long getNumDecommissionedTaskTrackers() { return numDecommissionedTaskTrackers; }
      public long getRunningMapTasks() { return runningMapTasks; }
      public long getRunningReduceTasks() { return runningReduceTasks; }
      public long getTotalJobSubmissions() { return totalJobSubmissions; }
      public long getOccupiedMapSlots() { return occupiedMapSlots; }
      public long getOccupiedReduceSlots() { return occupiedReduceSlots; }
      public long getReservedMapSlots() { return reservedMapSlots; }
      public long getReservedReduceSlots() { return reservedReduceSlots; }
      public long getMapTaskCapacity() { return mapTaskCapacity; }
      public long getReduceTaskCapacity() { return reduceTaskCapacity; }
      public float getAvgTasksPerTaskTracker(){ return avgTasksPerTaskTracker; }
    }

    public static class JobSummaryInfo {

      public static class JobTaskStats {

        private final int numCompleted;
        private final int numTotal;
        private final float completionPercentage;

        // Constructor
        JobTaskStats(int nc, int nt, float cp) {

          numCompleted = nc;
          numTotal = nt;
          completionPercentage = cp;
        }

        // Getters
        public int getNumCompleted() { return numCompleted; }
        public int getNumTotal() { return numTotal; }
        public float getCompletionPercentage() { return completionPercentage; }
      }

      private final String jobId;
      private final String jobName;
      private final String userName;
      private final String jobPriority;
      private final JobTaskStats mapStats;
      private final JobTaskStats reduceStats;
      private final String jobSchedulingInfo;

      // Constructor
      JobSummaryInfo(JobInProgress jip) {

        JobProfile jobProfile = jip.getProfile();

        jobId = jobProfile.getJobID().toString();
        jobName = jobProfile.getJobName();
        userName = jobProfile.getUser();
        jobPriority = jip.getPriority().toString();
        
        JobStatus jobStatus = jip.getStatus();

        mapStats = new JobTaskStats(jip.finishedMaps(), jip.desiredMaps(), jobStatus.mapProgress() * 100.0f);
        reduceStats = new JobTaskStats(jip.finishedReduces(), jip.desiredReduces(), jobStatus.reduceProgress() * 100.0f);

        jobSchedulingInfo = jip.getStatus().getSchedulingInfo();
      }

      // Getters
      public String getJobId() { return jobId; }
      public String getJobName() { return jobName; }
      public String getUserName() { return userName; }
      public String getJobPriority() { return jobPriority; }
      public JobTaskStats getMapStats() { return mapStats; }
      public JobTaskStats getReduceStats() { return reduceStats; }
      public String getJobSchedulingInfo() { return jobSchedulingInfo; }
    }

    private final JobTrackerMetaInfo metaInfo;
    private final JobTrackerClusterSummary clusterSummary;
    private final Collection<JobSummaryInfo> runningJobsSummaryInfo;
    private final Collection<JobSummaryInfo> completedJobsSummaryInfo;
    private final Collection<JobSummaryInfo> failedJobsSummaryInfo;

    private void populateJobsSummaryInfo
      (Collection<JobInProgress> jips, Collection<JobSummaryInfo> jsis) {

      for (JobInProgress jip : jips) {
        jsis.add(new JobSummaryInfo(jip));
      }
    }

    // Constructor
    JobTrackerResponse(JobTracker jt) {

      metaInfo = new JobTrackerMetaInfo(jt);
      clusterSummary = new JobTrackerClusterSummary(jt);

      Collection<JobInProgress> runningJobs = jt.runningJobs();
      runningJobsSummaryInfo = (runningJobs.size() > 0) ? 
        new ArrayList<JobSummaryInfo>() : null;
      populateJobsSummaryInfo(runningJobs, runningJobsSummaryInfo);

      Collection<JobInProgress> completedJobs = jt.completedJobs();
      completedJobsSummaryInfo = (completedJobs.size() > 0) ? 
        new ArrayList<JobSummaryInfo>() : null;
      populateJobsSummaryInfo(completedJobs, completedJobsSummaryInfo);

      Collection<JobInProgress> failedJobs = jt.failedJobs();
      failedJobsSummaryInfo = (failedJobs.size() > 0) ? 
        new ArrayList<JobSummaryInfo>() : null;
      populateJobsSummaryInfo(failedJobs, failedJobsSummaryInfo);
    }

    // Getters
    public JobTrackerMetaInfo getMetaInfo() { return metaInfo; }
    public JobTrackerClusterSummary getClusterSummary() { return clusterSummary; }
    public Collection<JobSummaryInfo> getRunningJobsSummaryInfo() { return runningJobsSummaryInfo; }
    public Collection<JobSummaryInfo> getCompletedJobsSummaryInfo() { return completedJobsSummaryInfo; }
    public Collection<JobSummaryInfo> getFailedJobsSummaryInfo() { return failedJobsSummaryInfo; }
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
      out.write('\n');
      out.write('\n');
      out.write('\n');

  String response_format = request.getParameter("format");

  if (response_format != null) {
    /* Eventually, the HTML output should also be driven off of these *Response
     * objects. 
     * 
     * Someday. 
     */
    JobTrackerResponse theJobTrackerResponse = null;
    ErrorResponse theErrorResponse = null;

    JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");

    theJobTrackerResponse = new JobTrackerResponse(tracker);

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
        ((theErrorResponse == null) ? theJobTrackerResponse : theErrorResponse));

    } else {
      response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }
  } else {

      out.write('\n');
   
  // Spit out HTML only in the absence of the "format" query parameter.
  response.setContentType("text/html; charset=UTF-8");

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  ClusterMetrics metrics = tracker.getClusterMetrics();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  JobQueueInfo[] queues = tracker.getQueues();
  Vector<JobInProgress> runningJobs = tracker.runningJobs();
  Vector<JobInProgress> completedJobs = tracker.completedJobs();
  Vector<JobInProgress> failedJobs = tracker.failedJobs();

      out.write("\n\n<!DOCTYPE html>\n<html>\n<head>\n<title>");
      out.print( trackerName );
      out.write(" Hadoop Map/Reduce Administration</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<script type=\"text/javascript\" src=\"/static/jobtracker.js\"></script>\n<script type='text/javascript' src='/static/sorttable.js'></script>\n</head>\n<body>\n\n");
 JSPUtil.processButtons(request, response, tracker); 
      out.write("\n\n<h1>");
      out.print( trackerName );
      out.write(" Hadoop Map/Reduce Administration</h1>\n\n<div id=\"quicklinks\">\n  <a href=\"#quicklinks\" onclick=\"toggle('quicklinks-list'); return false;\">Quick Links</a>\n  <ul id=\"quicklinks-list\">\n    <li><a href=\"#scheduling_info\">Scheduling Info</a></li>\n    <li><a href=\"#running_jobs\">Running Jobs</a></li>\n    <li><a href=\"#retired_jobs\">Retired Jobs</a></li>\n    <li><a href=\"#local_logs\">Local Logs</a></li>\n  </ul>\n</div>\n\n<b>State:</b> ");
      out.print( status.getJobTrackerState() );
      out.write("<br>\n<b>Started:</b> ");
      out.print( new Date(tracker.getStartTime()));
      out.write("<br>\n<b>Version:</b> ");
      out.print( VersionInfo.getVersion());
      out.write(",\n                r");
      out.print( VersionInfo.getRevision());
      out.write("<br>\n<b>Compiled:</b> ");
      out.print( VersionInfo.getDate());
      out.write(" by \n                 ");
      out.print( VersionInfo.getUser());
      out.write("<br>\n<b>Identifier:</b> ");
      out.print( tracker.getTrackerIdentifier());
      out.write("<br>                 \n<b>SafeMode:</b> ");
      out.print( tracker.getSafeModeText());
      out.write("<br>                    \n<hr>\n<h2>Cluster Summary (Heap Size is ");
      out.print( StringUtils.byteDesc(Runtime.getRuntime().totalMemory()) );
      out.write('/');
      out.print( StringUtils.byteDesc(Runtime.getRuntime().maxMemory()) );
      out.write(")</h2>\n");
 
 generateSummaryTable(out, metrics, tracker); 

      out.write("\n<hr>\n<h2 id=\"scheduling_info\">Scheduling Information</h2>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\" class=\"sortable\">\n<thead style=\"font-weight: bold\">\n<tr>\n<td> Queue Name </td>\n<td> State </td>\n<td> Scheduling Information</td>\n</tr>\n</thead>\n<tbody>\n");

for(JobQueueInfo queue: queues) {
  String queueName = queue.getQueueName();
  String state = queue.getQueueState();
  String schedulingInformation = queue.getSchedulingInfo();
  if(schedulingInformation == null || schedulingInformation.trim().equals("")) {
    schedulingInformation = "NA";
  }

      out.write("\n<tr>\n<td><a href=\"jobqueue_details.jsp?queueName=");
      out.print(queueName);
      out.write('"');
      out.write('>');
      out.print(queueName);
      out.write("</a></td>\n<td>");
      out.print(state);
      out.write("</td>\n<td>");
      out.print(HtmlQuoting.quoteHtmlChars(schedulingInformation).replaceAll("\n","<br/>") );
      out.write("\n</td>\n</tr>\n");

}

      out.write("\n</tbody>\n</table>\n<hr/>\n<b>Filter (Jobid, Priority, User, Name)</b> <input type=\"text\" id=\"filter\" onkeyup=\"applyfilter()\"> <br>\n<span class=\"small\">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>\n<hr>\n\n<h2 id=\"running_jobs\">Running Jobs</h2>\n");
      out.print(JSPUtil.generateJobTable("Running", runningJobs, 30, 0, tracker.conf));
      out.write("\n<hr>\n\n");

if (completedJobs.size() > 0) {
  out.print("<h2 id=\"completed_jobs\">Completed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Completed", completedJobs, 0, 
    runningJobs.size(), tracker.conf));
  out.print("<hr>");
}

      out.write('\n');
      out.write('\n');

if (failedJobs.size() > 0) {
  out.print("<h2 id=\"failed_jobs\">Failed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Failed", failedJobs, 0, 
    (runningJobs.size()+completedJobs.size()), tracker.conf));
  out.print("<hr>");
}

      out.write("\n\n<h2 id=\"retired_jobs\">Retired Jobs</h2>\n");
      out.print(JSPUtil.generateRetiredJobTable(tracker, 
  (runningJobs.size()+completedJobs.size()+failedJobs.size())));
      out.write("\n<hr>\n\n<h2 id=\"local_logs\">Local Logs</h2>\n<a href=\"logs/\">Log</a> directory,\n<a href=\"");
      out.print(JobHistoryServer.getHistoryUrlPrefix(tracker.conf));
      out.write("/jobhistoryhome.jsp\">\nJob Tracker History</a>\n\n");

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
