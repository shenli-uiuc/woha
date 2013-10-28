package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapred.JobHistory.*;

public final class taskdetailshistory_jsp extends org.apache.jasper.runtime.HttpJspBase
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

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ; 
	private static final long serialVersionUID = 1L;


  private void printTaskAttempt(JobHistory.TaskAttempt taskAttempt,
                                String type, JspWriter out,
                                String logFile) 
  throws Exception {
    out.print("<tr>"); 
    out.print("<td>" + taskAttempt.get(Keys.TASK_ATTEMPT_ID) + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getLong(Keys.START_TIME), 0 ) + "</td>"); 
    if (Values.REDUCE.name().equals(type)) {
      JobHistory.ReduceAttempt reduceAttempt = 
            (JobHistory.ReduceAttempt)taskAttempt; 
      out.print("<td>" + 
                StringUtils.getFormattedTimeWithDiff(dateFormat, 
                reduceAttempt.getLong(Keys.SHUFFLE_FINISHED), 
                reduceAttempt.getLong(Keys.START_TIME)) + "</td>"); 
      out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
                reduceAttempt.getLong(Keys.SORT_FINISHED), 
                reduceAttempt.getLong(Keys.SHUFFLE_FINISHED)) + "</td>"); 
    }
    out.print("<td>"+ StringUtils.getFormattedTimeWithDiff(dateFormat,
              taskAttempt.getLong(Keys.FINISH_TIME), 
              taskAttempt.getLong(Keys.START_TIME) ) + "</td>"); 
    out.print("<td>" + taskAttempt.get(Keys.HOSTNAME) + "</td>");
    out.print("<td>" + HtmlQuoting.quoteHtmlChars(taskAttempt.get(Keys.ERROR)) +
        "</td>");

    // Print task log urls
    out.print("<td>");	
    String taskLogsUrl = JobHistory.getTaskLogsUrl(taskAttempt);
    if (taskLogsUrl != null) {
	    String tailFourKBUrl = taskLogsUrl + "&start=-4097";
	    String tailEightKBUrl = taskLogsUrl + "&start=-8193";
	    String entireLogUrl = taskLogsUrl + "&all=true";
	    out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
	    out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
	    out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
    } else {
        out.print("n/a");
    }
    out.print("</td>");
    Counters counters = 
      Counters.fromEscapedCompactString(taskAttempt.get(Keys.COUNTERS));
    if (counters != null) {
      TaskAttemptID attemptId = 
        TaskAttemptID.forName(taskAttempt.get(Keys.TASK_ATTEMPT_ID));
      TaskID tipid = attemptId.getTaskID();
      org.apache.hadoop.mapreduce.JobID jobId = tipid.getJobID();
      out.print("<td>" 
       + "<a href=\"taskstatshistory.jsp?attemptid=" + attemptId
           + "&logFile=" + logFile + "\">"
           + counters.size() + "</a></td>");
    } else {
      out.print("<td></td>");
    }
    out.print("</tr>"); 
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
      response.setContentType("text/html; charset=UTF-8");
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
	
  String logFile = request.getParameter("logFile");
  String tipid = request.getParameter("tipid");
  if (logFile == null || tipid == null) {
    out.println("Missing job!!");
    return;
  }
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String jobid = JSPUtil.getJobID(new Path(encodedLogFileName).getName());
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobConf jobConf = (JobConf) application.getAttribute("jobConf");
  ACLsManager aclsManager = (ACLsManager) application.getAttribute("aclManager");
  JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobConf, aclsManager, fs, new Path(logFile));
  if (job == null) {
    return;
  }
  JobHistory.Task task = job.getAllTasks().get(tipid); 
  String type = task.get(Keys.TASK_TYPE);

      out.write("\n<!DOCTYPE html>\n<html>\n<body>\n<h2>");
      out.print(tipid );
      out.write(" attempts for <a href=\"jobdetailshistory.jsp?logFile=");
      out.print(encodedLogFileName);
      out.write('"');
      out.write('>');
      out.write(' ');
      out.print(jobid );
      out.write(" </a></h2>\n<center>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n<tr><td>Task Id</td><td>Start Time</td>\n");
	
  if (Values.REDUCE.name().equals(type)) {

      out.write("\n    <td>Shuffle Finished</td><td>Sort Finished</td>\n");

  }

      out.write("\n<td>Finish Time</td><td>Host</td><td>Error</td><td>Task Logs</td>\n<td>Counters</td></tr>\n");

  for (JobHistory.TaskAttempt attempt : task.getTaskAttempts().values()) {
    printTaskAttempt(attempt, type, out, encodedLogFileName);
  }

      out.write("\n</table>\n</center>\n");
	
  if (Values.MAP.name().equals(type)) {

      out.write("\n<h3>Input Split Locations</h3>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");

    for (String split : StringUtils.split(task.get(Keys.SPLITS)))
    {
      out.println("<tr><td>" + split + "</td></tr>");
    }

      out.write("\n</table>    \n");

  }

      out.write('\n');
      out.write("\n</body>\n</html>\n");
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
