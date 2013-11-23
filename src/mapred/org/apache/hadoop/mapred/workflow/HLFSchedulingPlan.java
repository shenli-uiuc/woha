package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

/**
 * Maximum Benifits First scheduling algorithm. 
 * Benifit is the amount of computational demand activated by the
 * current job minus the computational demand of the current job.
 */
public class HLFSchedulingPlan extends SchedulingPlan {

  private ArrayList<SchedulingEvent> schedEvents;
  private int slotNum;

  public HLFSchedulingPlan() {
  }

  @Override
  public int getSlotNum() {
    return slotNum;
  }

  @Override
  public boolean generatePlan(int maxSlots, 
                              WorkflowConf wfConf) {

    Hashtable<String, WJobConf> wjobs = wfConf.getWJobConfs();
    Hashtable<String, HashSet<String> > deps = 
      new Hashtable<String, HashSet<String> >();
    Hashtable<String, HashSet<String> > pres =
      new Hashtable<String, HashSet<String> >();

    try {
      WorkflowUtil.buildDepsAndPres(wjobs, deps, pres);
      Hashtable<String, Integer> preCounts = 
        WorkflowUtil.countPres(pres);
      LinkedList<String> queue = new LinkedList<String>();
      // calculate node level
      for (String jobName : wjobs.keySet()) {
        if (!deps.keySet().contains(jobName) || 
            deps.get(jobName).size() <= 0) {
          queue.addLast(jobName);
        }
      }

      while (queue.size() > 0) {
        String jobName = queue.pollFirst();
        WJobConf jobConf = wjobs.get(jobName);
        double maxLevel = 0;
        if (deps.contains(jobName)) {
          for (String dep : deps.get(jobName)) {
            WJobConf depConf = wjobs.get(dep);
            if (maxLevel < depConf.getPriority()) {
              maxLevel = depConf.getPriority();
            }
          }
        }
        double selfLen = maxLevel + 1; 
        jobConf.setPriority(selfLen);
        //add predecessors into the queue
        for (String pre : pres.get(jobName)) {
          if (preCounts.get(pre) <=0) {
            continue;
          }
          int newCount = preCounts.get(pre) - 1;
          preCounts.put(pre, new Integer(newCount));
          if (newCount <= 0) {
            queue.addLast(pre);
          }
          System.out.println();
        }
      }

      for (String jobName : wjobs.keySet()) {
        System.out.println(jobName + ": " +
            wjobs.get(jobName).getPriority() + ", ++");
      }
      System.out.println();

      WorkflowUtil.generateUniquePriority(wjobs, deps, pres);

      preCounts = WorkflowUtil.countPres(pres);
      // search for the minimum possible slots and 
      // generate scheduling plan
      int minSlots = 1;
      int midSlots = 0;
      int oriMaxSlots = maxSlots;
      slotNum = 0;
      while (minSlots < maxSlots) {
        System.out.println("**" + minSlots + ", " + maxSlots);
        midSlots = (minSlots + maxSlots) / 2;
        ArrayList<SchedulingEvent> curSched = 
          WorkflowUtil.checkFeasibility(wfConf, deps, preCounts, midSlots);
        System.out.println("After checkFeasibility, " + curSched);
        if (null == curSched) {
          minSlots = midSlots + 1;
        } else {
          System.out.println("not null!!");
          schedEvents = curSched;
          slotNum = midSlots;
          maxSlots = midSlots - 1;
        }
      }

      if (slotNum > 0 && null != schedEvents) {
        return true;
      }
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println(ex.getMessage());
      System.exit(-1);
    }
    return false;
  }

  @Override
  public long getRequirement (long ttd) {
    if (null == schedEvents) {
      return -1;
    } else {
      int head = 0;
      int tail = schedEvents.size();
      int mid = 0;
      SchedulingEvent event;
      while (head < tail) {
        mid = (head + tail) / 2;
        event = schedEvents.get(mid);
        if (event.ttd < ttd) {
          //schedEvents is in the descending order ot ttd
          tail = mid - 1;
        } else if (event.ttd > ttd) {
          head = mid + 1;
        } else {
          return event.schedRequirement;
        }
      }

      if (schedEvents.get(mid).ttd < ttd) {
        --mid;
        if (mid < 0) {
          return 0;
        }
      }

      return schedEvents.get(mid).schedRequirement;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(slotNum);
    System.out.println("PPPPPPPPPPPPPPPPPPPPP wrote slot number");
    if (null == schedEvents) {
      out.writeInt(0);
    } else {
      out.writeInt(schedEvents.size());
      for (int i = 0 ; i < schedEvents.size(); ++i) {
        schedEvents.get(i).write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    slotNum = in.readInt();
    int size = in.readInt();

    if (null == schedEvents) {
      schedEvents = new ArrayList<SchedulingEvent>();
    }
    for (int i = 0 ; i < size; ++i) {
      SchedulingEvent event = new SchedulingEvent();
      event.readFields(in);
      schedEvents.add(i, event);
    }
  }
}
