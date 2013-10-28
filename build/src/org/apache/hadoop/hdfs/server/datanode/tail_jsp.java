package org.apache.hadoop.hdfs.server.datanode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.util.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.http.HtmlQuoting;
import java.text.DateFormat;

public final class tail_jsp extends org.apache.jasper.runtime.HttpJspBase
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


  static JspHelper jspHelper = new JspHelper();

  public void generateFileChunks(JspWriter out, HttpServletRequest req,
                                 Configuration conf
                                ) throws IOException, InterruptedException {
    long startOffset = 0;
    
    int chunkSizeToView = 0;
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    String referrer = req.getParameter("referrer");
    boolean noLink = false;
    if (referrer == null) {
      noLink = true;
    }

    String filename = HtmlQuoting.unquoteHtmlChars(req.getParameter("filename"));
    if (filename == null) {
      out.print("Invalid input (file name absent)");
      return;
    }

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);
    
    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && Integer.parseInt(chunkSizeToViewStr) > 0)
      chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    else chunkSizeToView = JspHelper.getDefaultChunkSize(conf);

    if (!noLink) {
      out.print("<h3>Tail of File: ");
      JspHelper.printPathWithLinks(HtmlQuoting.quoteHtmlChars(filename),
                                   out, namenodeInfoPort, tokenString);
      out.print("</h3><hr>");
      out.print("<a href=\"" + referrer + "\">Go Back to File View</a><hr>");
    }
    else {
      out.print("<h3>" + HtmlQuoting.quoteHtmlChars(filename) + "</h3>");
    }
    out.print("<b>Chunk size to view (in bytes, up to file's DFS block size): </b>");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value=" +
              chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\"><hr>");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + 
              HtmlQuoting.quoteHtmlChars(filename) + "\">");
    out.print("<input type=\"hidden\" name=\"namenodeInfoPort\" value=\"" + namenodeInfoPort +
    "\">");
    if (!noLink)
      out.print("<input type=\"hidden\" name=\"referrer\" value=\"" + 
                referrer+ "\">");

    //fetch the block from the datanode that has the last block for this file
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    DFSClient dfs = JspHelper.getDFSClient(ugi, jspHelper.nameNodeAddr, conf);
    List<LocatedBlock> blocks = 
      dfs.namenode.getBlockLocations(filename, 0, Long.MAX_VALUE
                                     ).getLocatedBlocks();
    if (blocks == null || blocks.size() == 0) {
      out.print("No datanodes contain blocks of file "+filename);
      dfs.close();
      return;
    }
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    long blockSize = lastBlk.getBlock().getNumBytes();
    long blockId = lastBlk.getBlock().getBlockId();
    Token<BlockTokenIdentifier> accessToken = lastBlk.getBlockToken();
    long genStamp = lastBlk.getBlock().getGenerationStamp();
    DatanodeInfo chosenNode;
    try {
      chosenNode = jspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }      
    InetSocketAddress addr = NetUtils.createSocketAddr(chosenNode.getName());
    //view the last chunkSizeToView bytes while Tailing
    if (blockSize >= chunkSizeToView)
      startOffset = blockSize - chunkSizeToView;
    else startOffset = 0;

    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" style=\"width:100%\" READONLY>");
    jspHelper.streamBlockInAscii(addr, blockId, accessToken, genStamp, 
                                 blockSize, startOffset, chunkSizeToView, 
                                 out, conf);
    out.print("</textarea>");
    dfs.close();
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
      out.write("\n\n\n\n<!DOCTYPE html>\n<html>\n<head>\n");
JspHelper.createTitle(out, request, request.getParameter("filename")); 
      out.write("\n</head>\n<body>\n<form action=\"/tail.jsp\" method=\"GET\">\n");
 
   Configuration conf = 
     (Configuration) application.getAttribute(JspHelper.CURRENT_CONF);
   generateFileChunks(out, request, conf);

      out.write("\n</form>\n<hr>\n\n<h2>Local logs</h2>\n<a href=\"/logs/\">Log</a> directory\n\n");

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
