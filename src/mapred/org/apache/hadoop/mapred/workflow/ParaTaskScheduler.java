package org.apache.hadoop.mapred;

/**
 * When a job in a workflow becomes active, this 
 * TaskScheduler is responsible for calling
 * TaskTrackerManager.initJob(). JobTracker is the
 * implementation of TaskTrackerManager.
 *
 */
abstract class ParaTaskScheduler extends TaskScheduler {
  //abstract for now
}
