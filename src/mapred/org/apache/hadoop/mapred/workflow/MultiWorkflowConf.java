package org.apache.hadoop.mapred.workflow;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import org.xml.sax.SAXException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Given the user-defined workflow configuration (UDC) file, the WorkflowConf
 * class provides following functionalities:
 *  1. Constructor(): Read the UDC XML, check XML format, and generate DAG.
 *  2. getJarPaths(): Get the paths of user jar packages in the local disk.
 *  3. getInputDatasetPaths(): get the paths of input datasets in the HDFS
 *
 * WorkflowClient needs the above information to perform the checking steps
 * and submit the workflow to JobTracker.
 *
 * Remember to delete the jar files from jar_store when the workflow
 * is done.
 */

public class MultiWorkflowConf extends Configuration implements Writable{

    public static final Log LOG = LogFactory.getLog(MultiWorkflowConf.class);
    public static final long INF = 9999999999L;

    private TreeMap<Long, WorkflowConf> wfs;

    private MultiWorkflowConf() {
    }

    /**
     * Create a workflow object from an XML file. It only handles
     * the first workflow in each xml file for now, and ignore all
     * the following workflow configurations. 
     */
    public MultiWorkflowConf(String xmlName) {
      this.wfs = new TreeMap<Long, WorkflowConf>(WF_COMPARATOR);
      if (readXML(xmlName)) {
      } else {
        System.err.println("Failed to parse workflow XML file");
        System.exit(-1);
      }
    }

    public Collection<WorkflowConf> getWorkflowConfs() {
      return wfs.values();
    }

    static final Comparator<Long> WF_COMPARATOR =
      new Comparator<Long>() {
        public int compare(Long o1, Long o2) {
          if (o1.longValue() < o2.longValue()) {
            return -1;
          } else if(o1.longValue() > o2.longValue()) {
            return 1;
          } else {
            return 0;
          }
        }
      };

    /**
     * Read the XML from file, it implicitly checks the file format.
     *
     * @param xmlName XML file path in the local disk.
     */ 
    private boolean readXML(String xmlName) {
      long curTime = System.currentTimeMillis();
      try {
        // open xml file
        File f = new File(xmlName);

        DocumentBuilderFactory factory = 
            DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(f);

        doc.getDocumentElement().normalize();
        // read workflow attribute
        Element ws = 
          (Element)doc.getElementsByTagName("workflows").item(0);

        NodeList wList = ws.getElementsByTagName("workflow");

        for (int wi = 0; wi < wList.getLength(); ++wi) {
          String name;
          String user;
          long deadline;
          long submitAt;
          long minPara;
          Hashtable<String, WJobConf> jobs;
          Hashtable<String, String> datasets;
          HashSet<String> inDs;

          Element w = (Element) wList.item(wi);
          System.out.println("workflow name = " + 
              w.getAttribute("name"));
          name = w.getAttribute("name");
          user = w.getAttribute("user");
          String strDeadline = w.getAttribute("deadline");
          System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
          submitAt = Long.parseLong(w.getAttribute("submitat"));
          if ('+' == strDeadline.charAt(0)) {
            deadline = curTime + submitAt + 
              Long.parseLong(strDeadline.substring(1));
          } else {
            deadline = 
              Long.parseLong(strDeadline.substring(1));
          }
          System.out.println("deadline is " + strDeadline + ", " + deadline);

          submitAt += curTime; 

          // read datasets
          datasets = new Hashtable<String, String>();
          inDs = new HashSet<String>();
          NodeList dList = w.getElementsByTagName("dataset");

          String dsName, dsPath;
          for (int di = 0; di < dList.getLength(); ++di) {
            Element d = (Element) dList.item(di);

            dsName = d.getAttribute("name");
            dsPath = d.getAttribute("path");
            System.out.println("dataset " + dsName + " with path " + dsPath);
            datasets.put(dsName, dsPath);

            // later, a dataset will be removed from inDs if it is the 
            // output of some job.
            if (inDs.contains(dsName)) {
              System.err.println("Duplicate dataset names: " + dsName);
              return false;
            }
            inDs.add(dsName);
          }

          // read job information
          jobs = new Hashtable<String, WJobConf>();
          NodeList jList = w.getElementsByTagName("job");

          minPara = INF;

          // help to check if one dataset only comes from one
          // job
          Hashtable<String, String> dsToJobName = 
            new Hashtable<String, String>();

          for (int ji = 0; ji < jList.getLength(); ++ji) {
            Element j = (Element) jList.item(ji);

            String jobName = j.getAttribute("name");

            if (jobs.keySet().contains(jobName)) {
              throw new IOException("Job name " + jobName 
                  + " is not unique");
            }
            if (WJobStatus.isSubmitter(jobName)) {
              throw new IOException("Name prefix" + 
                  WJobStatus.SUBMITTER_PREFIX + " is reserved.");
            }

            String jarPath = j.getAttribute("jarpath");
            String jarClass = j.getAttribute("jarclass");
            int mapNum = Integer.parseInt(j.getAttribute("mapnum"));
            int redNum = Integer.parseInt(j.getAttribute("rednum"));
            long mapEstTime = 
              Long.parseLong(j.getAttribute("mapesttime"));
            long redEstTime = 
              Long.parseLong(j.getAttribute("redesttime"));

            if (0 == mapNum || 0 == redNum) {
              throw new IOException("mapnum and rednum of job " +
                  jobName + " cannot be 0");
            }

            if (minPara > redNum) {
              minPara = redNum;
            }
            if (minPara > mapNum) {
              minPara = mapNum;
            }

            System.out.println("Job " + jobName
                + ", jarpath = " + jarPath
                + ", jarclass = " + jarClass);

            String inputs[]  = getIODatasets(j, "input");
            String outputs[] = getIODatasets(j, "output");

            // check if input dataset of this job are defined 
            // as datasets in the XML
            for (String input : inputs) {
              if (!datasets.keySet().contains(input)) {
                System.err.println("unrecognized input dataset: " + input +
                    " of job " + jobName + 
                    ". Please define the dataset.");
                return false;
              }
            }

            // check if the output dataset of this job are defined
            // as datasets in the XML
            for (String output : outputs) {
              if (!datasets.keySet().contains(output)) {
                System.err.println("unrecognized output dataset: " + output +
                    " of job " + jobName +
                    ". Please define the dataset.");
                //TODO: define our own exception class
                return false;
              }
              inDs.remove(output);
              if (dsToJobName.contains(output)) {
                System.err.println("Dataset " + output + " comes from" +
                    "multiple sources: " + jobName + ", " + 
                    dsToJobName.get(output));
                return false;
              } else {
                dsToJobName.put(output, jobName);
              }
            }

            // read parameter
            String param = null;
            NodeList pList = j.getElementsByTagName("param");
            if (null != pList && null != pList.item(0)) {
              param = pList.item(0).getTextContent();
              // replace consecutive white spaces into one. 
              param = param.replaceAll("[\t\n ]+", " ");
            }

            WJobConf wJob = new WJobConf(jobName, jarPath, jarClass, 
                inputs, outputs, param, 
                mapNum, redNum, mapEstTime, redEstTime);

            jobs.put(jobName, wJob);
          }

          WorkflowConf wfConf = new WorkflowConf(
                                      name, user, deadline, submitAt,
                                      minPara, jobs, datasets, inDs);
          this.wfs.put(submitAt, wfConf);
        }

        // XML is parsed successfully
        return true;
      } catch (Exception ex) {
        System.err.println(ex.getMessage());
        ex.printStackTrace();
        System.exit(-1);
      }
      return false;
    }

    /**
     * Split the input or output datasets string into an array
     * of dataset names.
     */ 
    private String [] getIODatasets(Element e, String tag){
      NodeList nl = e.getElementsByTagName(tag);
      if (null == nl || nl.getLength() <= 0) {
        return new String[0];
      }
      String str = nl.item(0).getTextContent();
      // dataset are separated by comma. Here, we remove all whitespaces.
      str = str.replaceAll("[\t\n ]", "");
      return str.split(",");
    }
}
