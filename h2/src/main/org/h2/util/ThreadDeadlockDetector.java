/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.h2.mvstore.db.MVTable;

/**
 * Detects deadlocks between threads. Prints out data in the same format as the CTRL-BREAK handler,
 * but includes information about table locks.
 */
public class ThreadDeadlockDetector
{
	private static String INDENT = "    ";

	private final ThreadMXBean tmbean;

	private final Timer threadCheck = new Timer("ThreadDeadlockDetector", true/* isDaemon */);

	private static ThreadDeadlockDetector detector = null;

	public synchronized static void init() {
		if (detector == null) {
			detector = new ThreadDeadlockDetector();
		}
	}

	private ThreadDeadlockDetector() {
		this.tmbean = ManagementFactory.getThreadMXBean();
		threadCheck.schedule(new TimerTask() {
			@Override
			public void run() {
				checkForDeadlocks();
			}
		}, 10/*delay(ms)*/, 10000/*period(ms)*/);
	}

	/**
	 * Checks if any threads are deadlocked. If any, print the thread dump information.
	 */
	private void checkForDeadlocks() {
		long[] tids = tmbean.findDeadlockedThreads();
		if (tids == null) {
			return;
		}

		final StringWriter stringWriter = new StringWriter();
		final PrintWriter print = new PrintWriter(stringWriter);

		print.println("ThreadDeadlockDetector - deadlock found :");
		final ThreadInfo[] infos = tmbean.getThreadInfo(tids, true, true);
		final HashMap<Long,String> mvtableWaitingForLockMap =
				MVTable.WAITING_FOR_LOCK.getSnapshotOfAllThreads();
		final HashMap<Long,ArrayList<String>> mvtableExclusiveLocksMap =
				MVTable.EXCLUSIVE_LOCKS.getSnapshotOfAllThreads();
		final HashMap<Long,ArrayList<String>> mvtableSharedLocksMap =
				MVTable.SHARED_LOCKS.getSnapshotOfAllThreads();
		for (ThreadInfo ti : infos) {
			printThreadInfo(print, ti);
			printLockInfo(print, ti.getLockedSynchronizers(),
					mvtableWaitingForLockMap.get(ti.getThreadId()),
					mvtableExclusiveLocksMap.get(ti.getThreadId()),
					mvtableSharedLocksMap.get(ti.getThreadId()));
		}

		print.flush();
		// Dump it to system.out in one block, so it doesn't get mixed up with other stuff when we're
		// using a logging subsystem.
		System.out.println(stringWriter.getBuffer());
	}

	private static void printThreadInfo(PrintWriter print, ThreadInfo ti) {
		// print thread information
		printThread(print, ti);

		// print stack trace with locks
		StackTraceElement[] stacktrace = ti.getStackTrace();
		MonitorInfo[] monitors = ti.getLockedMonitors();
		for (int i = 0; i < stacktrace.length; i++) {
			StackTraceElement ste = stacktrace[i];
			print.println(INDENT + "at " + ste.toString());
			for (MonitorInfo mi : monitors) {
				if (mi.getLockedStackDepth() == i) {
					print.println(INDENT + "  - locked " + mi);
				}
			}
		}
		print.println();
	}

	private static void printThread(PrintWriter print, ThreadInfo ti) {
		print.print("\"" + ti.getThreadName() + "\"" + " Id="
				+ ti.getThreadId() + " in " + ti.getThreadState());
		if (ti.getLockName() != null) {
			print.append(" on lock=" + ti.getLockName());
		}
		if (ti.isSuspended()) {
			print.append(" (suspended)");
		}
		if (ti.isInNative()) {
			print.append(" (running in native)");
		}
		print.println();
		if (ti.getLockOwnerName() != null) {
			print.println(INDENT + " owned by " + ti.getLockOwnerName() + " Id="
					+ ti.getLockOwnerId());
		}
	}

	private static void printLockInfo(PrintWriter print, LockInfo[] locks,
			String mvtableWaitingForLock,
			ArrayList<String> mvtableExclusiveLocks,
			ArrayList<String> mvtableSharedLocksMap) {
		print.println(INDENT + "Locked synchronizers: count = " + locks.length);
		for (LockInfo li : locks) {
			print.println(INDENT + "  - " + li);
		}
		if (mvtableWaitingForLock != null) {
			print.println(INDENT + "Waiting for table: " + mvtableWaitingForLock);
		}
		if (mvtableExclusiveLocks != null) {
			print.println(INDENT + "Exclusive table locks: count = " + mvtableExclusiveLocks.size());
			for (String name : mvtableExclusiveLocks) {
				print.println(INDENT + "  - " + name);
			}
		}
		if (mvtableSharedLocksMap != null) {
			print.println(INDENT + "Shared table locks: count = " + mvtableSharedLocksMap.size());
			for (String name : mvtableSharedLocksMap) {
				print.println(INDENT + "  - " + name);
			}
		}
		print.println();
	}

}