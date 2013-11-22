package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map;
import java.util.Comparator;
import java.util.Iterator;

/**
 * offer some common methods to process workflows
 */
public class WorkflowUtil {

  static class StaticPriorityComparator implements Comparator<String> {
    
    Hashtable<String, Double> priorities;

    private StaticPriorityComparator(){}

    public StaticPriorityComparator(Hashtable<String, Double> priorities) {
      this.priorities = priorities;
    }

    @Override
    public int compare(String o1, String o2) {
      if (priorities.get(o1) > priorities.get(o2)) {
        return -1;
      } else if (priorities.get(o1) < priorities.get(o2)) {
        return 1;
      } else {
        return 0;
      }
    }
    
  }

  static class PriorityComparator implements Comparator<String> {

    Hashtable<String, WJobConf> wJobConfs;

    private PriorityComparator(){}

    public PriorityComparator(Hashtable<String, WJobConf> wJobConfs) {
      this.wJobConfs = wJobConfs;
    }

    @Override
    public int compare(String o1, String o2) {
      WJobConf conf1 = wJobConfs.get(o1);
      WJobConf conf2 = wJobConfs.get(o2);

      if (conf1.getPriority() > conf2.getPriority()) {
        return -1;
      } else if (conf1.getPriority() < conf2.getPriority()) {
        return 1;
      } else {
        return 0;
      }
    }
  
  }

  private static ArrayList<SchedulingEvent> 
    simulate(Hashtable<String, WJobConf> wJobConfs,
             TreeSet<String> activeSet,
             TreeSet<String> inactiveSet,
             Hashtable<String, HashSet<String> > deps,
             Hashtable<String, Integer> preCounts,
             int slot,
             long relativeDeadline) {
    ArrayList<SchedulingEvent> schedEvents = 
      new ArrayList<SchedulingEvent>();

    // init wjob status
    Hashtable<String, WJobStatus> wJobStatuses = 
      new Hashtable<String, WJobStatus>();
    for (String name : wJobConfs.keySet()) {
      WJobConf wJobConf = wJobConfs.get(name);
      wJobStatuses.put(name, new WJobStatus(null, 
                                            wJobConf.getMapNum(),
                                            wJobConf.getRedNum()));
    }
  
    TreeMap<Long, Integer> freeSlotEvents = 
      new TreeMap<Long, Integer>();
    TreeMap<Long, String> jobDoneEvents = 
      new TreeMap<Long, String>();
    TreeMap<Long, String> redStartEvents = 
      new TreeMap<Long, String>();

    long curTime = 0;
    int curSlot = 0;
    freeSlotEvents.put(new Long(0), slot);
    Long eventTime = null;
    long prevRequirement = 0;
    System.out.println("Start simulate");
    while (!freeSlotEvents.isEmpty() ||
           !jobDoneEvents.isEmpty() ||
           !redStartEvents.isEmpty()) {

      //System.out.println(freeSlotEvents.size() + ", " +
      //                   jobDoneEvents.size() + ", " +
      //                   redStartEvents.size());
      while (!jobDoneEvents.isEmpty() && 
          jobDoneEvents.firstKey().longValue() <= curTime) {
        eventTime = jobDoneEvents.firstKey();
        WJobConf wJobConf = 
          wJobConfs.get(jobDoneEvents.get(eventTime));
        //activates jobs
        String name = wJobConf.getName();
        HashSet<String> curDeps = deps.get(name);
        if (null != curDeps) {
          for (String dep : curDeps) {
            int preCount = preCounts.get(dep) - 1;
            preCounts.put(dep, preCount);
            if (0 == preCount) {
              inactiveSet.remove(dep);
              activeSet.add(dep);
            }
          }
        }
        jobDoneEvents.remove(eventTime);
      }

      while (!redStartEvents.isEmpty() && 
          redStartEvents.firstKey().longValue() <= curTime) {
        eventTime = redStartEvents.firstKey();
        activeSet.add(redStartEvents.get(eventTime));
        redStartEvents.remove(eventTime);
      }

      // handle free slot events
      while (!freeSlotEvents.isEmpty() && 
          freeSlotEvents.firstKey().longValue() <= curTime) {
        eventTime = freeSlotEvents.firstKey();
        curSlot += freeSlotEvents.get(eventTime).intValue();
        freeSlotEvents.remove(eventTime);
      }

      // shedule tasks
      long schedRequirement = 0;
      if (curSlot > 0) {
        ArrayList<String> activeJobs = new ArrayList<String>(activeSet);

        // order jobs based on priority


        for (String activeJob : activeJobs) {
          WJobConf wJobConf = wJobConfs.get(activeJob);
          String name = wJobConf.getName();
          WJobStatus status = wJobStatuses.get(name);
          int map = status.getRemainingMap();
          int red = status.getRemainingRed();
          if (map > 0) {
            int assignedMap = map > curSlot ? curSlot : map;
            map -= assignedMap;
            curSlot -= assignedMap;
            schedRequirement += wJobConf.getMapEstTime() * assignedMap;
            status.setRemainingMap(map);
            long nextEventTime = curTime + wJobConf.getMapEstTime();
            freeSlotEvents.put(new Long(nextEventTime), assignedMap);
            if (map <= 0) {
              redStartEvents.put(new Long(nextEventTime), name);
              // remove the wjob for now, and add it back when the
              // reduce start event fires.
              activeSet.remove(name);
            }

            if (curSlot <= 0) {
              break;
            }
          } else if (red > 0) {
            int assignedRed = red > curSlot ? curSlot : red;
            red -= assignedRed;
            curSlot -= assignedRed;
            schedRequirement += wJobConf.getRedEstTime() * assignedRed;
            status.setRemainingRed(red);
            long nextEventTime = curTime + wJobConf.getRedEstTime();
            freeSlotEvents.put(new Long(nextEventTime), assignedRed);
            if (red <= 0) {
              jobDoneEvents.put(new Long(nextEventTime), name);
              activeSet.remove(name);
            }

            if (curSlot <= 0) {
              break;
            }
          }
        }
      }

      // for now ttd is not real ttd, it is actually the time
      // from the begining. Need one more pass to calculate the 
      // real ttd.
      if (schedRequirement > 0) {
        SchedulingEvent schedEvent = 
          new SchedulingEvent(curTime, schedRequirement + prevRequirement);
        prevRequirement = schedRequirement;
        schedEvents.add(schedEvent);
      }

      if (freeSlotEvents.isEmpty()) {
        break;
      } else {
        curTime = freeSlotEvents.firstKey().longValue();
      }
    }
  
    // post process schedEvents to regulate times.
    // curTime is when the workflow finishes
    System.out.println(curTime + ", " + relativeDeadline);
    if (curTime > relativeDeadline) {
      System.out.println("infeasible in simulate");
      return null;
    } else {
      for (SchedulingEvent event : schedEvents) {
        event.ttd = curTime - event.ttd;
      }

      // for debug
      //for (SchedulingEvent event : schedEvents) {
      //  System.out.println(event.ttd + ", " + event.schedRequirement);
      //}
      System.out.println("feasible in simulate");
      return schedEvents;
    }
  }

  /**
   * Check the feasibility of the given workflow with the given number
   * of slots. If infeasible, it returns null. Otherwise, returns the 
   * scheduling plan. WJobs are scheduled with the given priority in
   * the work-conserving manner.
   *
   * @param wJobConfs the hashtable of wjob configurations (with priority)
   * @param deps the workflow topoloty
   * @param preCounts together with deps forms the workflow topology.
   * @param maxSlots the maximum available slots
   *
   * @return null if it is infeasible. If it is feasible, returns the
   *         scheudling plan.
   */
  public static ArrayList<SchedulingEvent> 
    checkFeasibility(WorkflowConf wfConf,
                     Hashtable<String, HashSet<String> > deps,
                     Hashtable<String, Integer> preCounts,
                     int maxSlots) {
    Hashtable<String, WJobConf> wJobConfs = wfConf.getWJobConfs();
    TreeSet<String> activeSet = 
      new TreeSet<String>(new PriorityComparator(wJobConfs));
    TreeSet<String> inactiveSet = 
      new TreeSet<String>(new PriorityComparator(wJobConfs));

    long relativeDeadline = 
      wfConf.getDeadline() - System.currentTimeMillis();
    for (String name : wJobConfs.keySet()) {
      if (preCounts.get(name) <= 0) {
        activeSet.add(name);
      } else {
        inactiveSet.add(name);
      }
    }

    return simulate(wJobConfs, activeSet, inactiveSet, deps, 
                    preCounts, maxSlots, relativeDeadline);
  }

  public static Hashtable<String, Integer> countPres(
      Hashtable<String, HashSet<String> > pres) {
    Hashtable<String, Integer> preCounts = 
      new Hashtable<String, Integer>();

    for (String name : pres.keySet()) {
      preCounts.put(name, pres.get(name).size());
    }
    return preCounts;
  }

  /**
   * build up dependency graph with the given WJobConf data
   *
   * @param wJobConfs the hashtable stores all wjob information
   * @param deps the result dependency information. must not be null.
   * @param preCounts the number of prerequisites of each job. must not be null
   */
  public static void buildDepsAndPres(
      Hashtable<String, WJobConf> wJobConfs,
      Hashtable<String, HashSet<String> > deps,
      Hashtable<String, HashSet<String> > pres) throws IOException {
    if (null == wJobConfs ||
        null == deps ||
        null == pres) {
      throw new IOException(
          "all parameters of WorkflowUtil.buildDepsAndPres" 
          + "must not be null");
    }

    Hashtable<String, String> dsToJobName = 
      new Hashtable<String, String> ();

    for (String name : wJobConfs.keySet()) {
      WJobConf wJobConf = wJobConfs.get(name);
      for (String output : wJobConf.getOutputs()) {
        dsToJobName.put(output, name);
      }
    }

    for (String name : wJobConfs.keySet()) {
      WJobConf wJobConf = wJobConfs.get(name);
      HashSet<String> preSet = new HashSet<String>();
      for (String input : wJobConf.getInputs()) {
        String preName = dsToJobName.get(input);
        if (null != preName) {
          if (!deps.keySet().contains(preName)) {
            HashSet<String> depsSet = new HashSet<String> ();
            deps.put(preName, depsSet);
          }
          deps.get(preName).add(name);
          preSet.add(preName);
        }
      }
      pres.put(name, preSet);
    }
  }

  public static void buildDeps(
      Hashtable<String, WJobConf> wJobConfs,
      Hashtable<String, HashSet<String> > deps,
      Hashtable<String, Integer> preCounts) throws IOException {

    if (null == preCounts) {
      throw new IOException(
          "all paramters of WorkflowUtil.buildDeps must not be null");
    }
    Hashtable<String, HashSet<String> > pres = 
      new Hashtable<String, HashSet<String> >();

    buildDepsAndPres(wJobConfs, deps, pres);
    for (String name : pres.keySet()) {
      preCounts.put(name, pres.get(name).size());
    }
  }


  public static void recursivelyUpdatePriority(
      Hashtable<String, WJobConf> wJobConfs,
      Hashtable<String, HashSet<String> > pres,
      TreeSet<String> activeSet,
      String name,
      double delta) {
    System.out.println(name);

    boolean addBack = false;
    if (activeSet.contains(name)) {
      activeSet.remove(name);
      addBack = true;

    }
    WJobConf jobConf = wJobConfs.get(name);
    jobConf.setPriority(jobConf.getPriority() + delta);
    HashSet<String> preSet = pres.get(name);

    if (addBack) {
      activeSet.add(name);
    }
    for (String preName : preSet) {
      recursivelyUpdatePriority(wJobConfs, pres, activeSet, 
          preName, delta / preSet.size());
    }
  }

  /**
   * This method will update wJobConfs priority with the sched order
   *
   * @return the order that the jobs are scheduled based on the 
   * dynamically changing priority.
   */
  public static void getSchedOrder(
      Hashtable<String, WJobConf> wJobConfs,
      Hashtable<String, HashSet<String> > deps,
      Hashtable<String, HashSet<String> > pres) throws IOException {
    if (null == wJobConfs || 
        null == deps ||
        null == pres) {
      throw new IOException(
          "all paramters of WorkflowUtil.getSchedOrder must not be null");
    }

    TreeSet<String> activeSet = 
      new TreeSet<String>(new PriorityComparator(wJobConfs));

    for (String name : wJobConfs.keySet()) {
      if (pres.get(name).size() <= 0) {
        activeSet.add(name);
      }
    }

    int index = 0;
    Hashtable<String, Integer> order = new Hashtable<String, Integer>();

    //copy pres
    Hashtable<String, HashSet<String> > presCopy = 
      new Hashtable<String, HashSet<String> >();
    for (String name : pres.keySet()) {
      presCopy.put(name, 
                   (HashSet<String>)pres.get(name).clone());
    }

    while (activeSet.size() > 0) {
      String name = activeSet.first();
      System.out.println("active" + name);
      order.put(name, index);
      index++;
      System.out.println(activeSet.remove(name) + ", " +  activeSet.contains(name));
      HashSet<String> curDeps = deps.get(name);
      if (null == curDeps) {
        continue;
      }
      for (String dep : curDeps) {
        HashSet<String> curPres = presCopy.get(dep);
        curPres.remove(name);
        if (curPres.size() <= 0) {
          System.out.println("actove + " + dep);
          activeSet.add(dep);
        } else {
          double delta = wJobConfs.get(dep).getPriority();
          delta = delta / (curPres.size() * (curPres.size() + 1));
          for (String remainPre : curPres) {
            recursivelyUpdatePriority(wJobConfs, presCopy, activeSet,
                remainPre, delta);
          }
        }
      }
    }

    for (String name : order.keySet()) {
      index = order.get(name);
      wJobConfs.get(name).setPriority(order.size() - index);
    }
  }
}
