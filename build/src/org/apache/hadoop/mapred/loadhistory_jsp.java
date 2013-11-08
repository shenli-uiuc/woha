package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobHistory.*;

public final class loadhistory_jsp extends org.apache.jasper.runtime.HttpJspBase
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

	private static final long serialVersionUID = 1L;

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

    PathFilter jobLogFileFilter = new PathFilter() {
      public boolean accept(Path path) {
        return !(path.getName().endsWith(".xml"));
      }
    };

    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String jobId = request.getParameter("jobid");
    JobHistory.JobInfo job = (JobHistory.JobInfo)
                               request.getSession().getAttribute("job");
    // if session attribute of JobInfo exists and is of different job's,
    // then remove the attribute
    // if the job has not yet finished, remove the attribute sothat it 
    // gets refreshed.
    boolean isJobComplete = false;
    if (null != job) {
      String jobStatus = job.get(Keys.JOB_STATUS);
      isJobComplete = Values.SUCCESS.name() == jobStatus
                      || Values.FAILED.name() == jobStatus
                      || Values.KILLED.name() == jobStatus;
    }
    if (null != job && 
       (!jobId.equals(job.get(Keys.JOBID)) 
         || !isJobComplete)) {
      // remove jobInfo from session, keep only one job in session at a time
      request.getSession().removeAttribute("job"); 
      job = null ; 
    }
	
    if (null == job) {
      String jobLogFile = request.getParameter("logFile");
      job = new JobHistory.JobInfo(jobId); 
      DefaultJobHistoryParser.parseJobTasks(jobLogFile, job, fs) ; 
      request.getSession().setAttribute("job", job);
      request.getSession().setAttribute("fs", fs);
    }

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
