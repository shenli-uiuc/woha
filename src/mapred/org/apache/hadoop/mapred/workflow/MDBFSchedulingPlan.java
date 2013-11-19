package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;

/**
 * Maximum Dynamic Benifits First scheduling algorithm. 
 * Benifit is the amount of computational demand activated by the
 * current job minus the computational demand of the current job.
 */
public class MDBFSchedulingPlan extends SchedulingPlan {

  private ArrayList<SchedulingEvent> schedEvents;
  private int slotNum;

  public MDBFSchedulingPlan() {
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
      // calculate benefits
      Hashtable<String, Long> benefits = 
        new Hashtable<String, Long> ();
      for (String jobName : wjobs.keySet()) {
        WJobConf wJobConf = wjobs.get(jobName);
        long work = wJobConf.getMapNum() * wJobConf.getMapEstTime()
                  + wJobConf.getRedNum() * wJobConf.getRedEstTime();
        benefits.put(jobName, new Long(work));
      }

      for (String jobName : wjobs.keySet()) {
        HashSet<String> curDeps = deps.get(jobName);
        double benefit = benefits.get(jobName);
        if (null != curDeps) {
          for (String dep : curDeps) {
            benefit += (benefits.get(dep) / preCounts.get(dep));
          }
        }
        wjobs.get(jobName).setPriority(benefit);
      }

      // calculate the dyanmic priority, such that when one job finishes
      // its benifits from its deps get shared by other pres
      WorkflowUtil.getSchedOrder(wjobs, deps, pres);


      // search for the minimum possible slots and 
      // generate scheduling plan
      int minSlots = 0;
      int midSlots = 0;
      int oriMaxSlots = maxSlots;
      slotNum = 0;
      while (minSlots < maxSlots) {
        System.out.println("**" + minSlots + ", " + maxSlots);
        midSlots = (minSlots + maxSlots) / 2;
        ArrayList<SchedulingEvent> curSched = 
          WorkflowUtil.checkFeasibility(wfConf, deps, preCounts, midSlots);
        System.out.println("After checkFeasibility");
        if (null == curSched) {
          System.out.println("infeasible");
          minSlots = midSlots + 1;
        } else {
          System.out.println("feasible");
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
