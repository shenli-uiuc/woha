package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck;

public final class gethistory_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

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
      response.setContentType("application/octet-stream; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

      out.write('\n');
      out.write('\n');
      out.write('\n');

    String jobId = request.getParameter("jobid");
    if (jobId == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing 'jobid'!");
      return;
    }
    
    final JobTracker tracker = (JobTracker) application.getAttribute(
        "job.tracker");

    final JobID jobIdObj = JobID.forName(jobId);
    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobIdObj,
                                                     request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();

    if (job != null && !job.getStatus().isJobComplete()) {
      response.getWriter().print("Job " + jobId + " is still in running");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    String historyFile = JobHistory.getHistoryFilePath(jobIdObj);
    if (historyFile == null) {
      response.getWriter().print("Job " + jobId + " not known");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    String historyUrl = "http://" + JobHistoryServer.getAddress(tracker.conf) +
      "/historyfile?logFile=" +
      JobHistory.JobInfo.encodeJobHistoryFilePath(historyFile);
    response.sendRedirect(response.encodeRedirectURL(historyUrl));

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
