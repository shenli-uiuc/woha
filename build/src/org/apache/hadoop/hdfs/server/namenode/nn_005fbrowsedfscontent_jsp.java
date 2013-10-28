package org.apache.hadoop.hdfs.server.namenode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.delegation.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.*;
import java.text.DateFormat;
import java.net.InetAddress;
import java.net.URLEncoder;

public final class nn_005fbrowsedfscontent_jsp extends org.apache.jasper.runtime.HttpJspBase
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


  static String getDelegationToken(final NameNode nn,
                                   final UserGroupInformation ugi,
                                   HttpServletRequest request, Configuration conf) 
                                   throws IOException, InterruptedException {
    Token<DelegationTokenIdentifier> token =
      ugi.doAs(
              new PrivilegedExceptionAction<Token<DelegationTokenIdentifier>>()
          {
            public Token<DelegationTokenIdentifier> run() throws IOException {
              return nn.getDelegationToken(new Text(ugi.getUserName()));
            }
          });
    return token.encodeToUrlString();
  }

  public void redirectToRandomDataNode(
                            ServletContext context, 
                            HttpServletRequest request,
                            HttpServletResponse resp
                           ) throws IOException, InterruptedException {
    Configuration conf = (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
    NameNode nn = (NameNode)context.getAttribute("name.node");
    final UserGroupInformation ugi = JspHelper.getUGI(context, request, conf);
    String tokenString = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      tokenString = getDelegationToken(nn, ugi, request, conf);
    }
    FSNamesystem fsn = nn.getNamesystem();
    String datanode = fsn.randomDataNode();
    String redirectLocation;
    String nodeToRedirect;
    int redirectPort;
    if (datanode != null) {
      redirectPort = Integer.parseInt(datanode.substring(datanode.indexOf(':')
                     + 1));
      nodeToRedirect = datanode.substring(0, datanode.indexOf(':'));
    }
    else {
      nodeToRedirect = nn.getHttpAddress().getHostName();
      redirectPort = nn.getHttpAddress().getPort();
    }
    String fqdn = InetAddress.getByName(nodeToRedirect).getCanonicalHostName();
    redirectLocation = "http://" + fqdn + ":" + redirectPort + 
                       "/browseDirectory.jsp?namenodeInfoPort=" + 
                       nn.getHttpAddress().getPort() +
                       "&dir=/" + 
                       (tokenString == null ? "" :
                        JspHelper.getDelegationTokenUrlParam(tokenString));
    resp.sendRedirect(redirectLocation);
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
      out.write("\n\n<!DOCTYPE html>\n<html>\n\n<title></title>\n\n<body>\n");
 
  redirectToRandomDataNode(application, request, response); 

      out.write("\n<hr>\n\n<h2>Local logs</h2>\n<a href=\"/logs/\">Log</a> directory\n\n");

out.println(ServletUtil.htmlFooter());

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
