/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class RandomUtils {

    private static SecureRandom secureRandom;
    private static Random random  = new Random();
    private static volatile boolean seeded;

    private static synchronized SecureRandom getSecureRandom() {
        if (secureRandom != null) {
            return secureRandom;
        }
        // Workaround for SecureRandom problem as described in
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6202721
        // Can not do that in a static initializer block, because
        // threads are not started after the initializer block exits
        try {
            secureRandom = SecureRandom.getInstance("SHA1PRNG");
            // On some systems, secureRandom.generateSeed() is very slow.
            // In this case it is initialized using our own seed implementation
            // and afterwards (in the thread) using the regular algorithm.
            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
                        byte[] seed = sr.generateSeed(20);
                        synchronized (secureRandom) {
                            secureRandom.setSeed(seed);
                            seeded = true;
                        }
                    } catch (NoSuchAlgorithmException e) {
                        warn("SecureRandom", e);
                    }
                }
            };
            Thread t = new Thread(runnable);
            t.start();
            Thread.yield();
            try {
                // normally, generateSeed takes less than 200 ms
                t.join(400);
            } catch (InterruptedException e) {
                warn("InterruptedException", e);
            }
            if (!seeded) {
                byte[] seed = generateAlternativeSeed();
                // this never reduces randomness
                synchronized (secureRandom) {
                    secureRandom.setSeed(seed);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            warn("SecureRandom", e);
            secureRandom = new SecureRandom();
        }
        return secureRandom;
    }

    private static byte[] generateAlternativeSeed() {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            // milliseconds
            out.writeLong(System.currentTimeMillis());

            // nanoseconds if available
            try {
                Method m = System.class.getMethod("nanoTime", new Class[0]);
                if (m != null) {
                    Object o = m.invoke(null, (java.lang.Object[]) null);
                    out.writeUTF(o.toString());
                }
            } catch (Exception e) {
                // nanoTime not found, this is ok (only exists for JDK 1.5 and higher)
            }

            // memory
            out.writeInt(new Object().hashCode());
            Runtime runtime = Runtime.getRuntime();
            out.writeLong(runtime.freeMemory());
            out.writeLong(runtime.maxMemory());
            out.writeLong(runtime.totalMemory());

            // environment
            try {
                out.writeUTF(System.getProperties().toString());
            } catch (Exception e) {
                warn("generateAlternativeSeed", e);
            }

            // host name and ip addresses (if any)
            try {
                String hostName = InetAddress.getLocalHost().getHostName();
                out.writeUTF(hostName);
                InetAddress[] list = InetAddress.getAllByName(hostName);
                for (int i = 0; i < list.length; i++) {
                    out.write(list[i].getAddress());
                }
            } catch (Exception e) {
                // on some system, InetAddress.getLocalHost() doesn't work
                // for some reason (incorrect configuration)
            }

            // timing (a second thread is already running)
            for (int j = 0; j < 16; j++) {
                int i = 0;
                long end = System.currentTimeMillis();
                while (end == System.currentTimeMillis()) {
                    i++;
                }
                out.writeInt(i);
            }

            out.close();
            return bout.toByteArray();
        } catch (IOException e) {
            warn("generateAlternativeSeed", e);
            return new byte[1];
        }
    }

    public static long getSecureLong() {
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            return sr.nextLong();
        }
    }

    public static byte[] getSecureBytes(int len) {
        if (len <= 0) {
            len = 1;
        }
        byte[] buff = new byte[len];
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            sr.nextBytes(buff);
        }
        return buff;
    }

    public static int nextInt(int max) {
        return random.nextInt(max);
    }

    private static void warn(String s, Throwable t) {
        // not a fatal problem, but maybe reduced security
        System.out.println("RandomUtils warning: " + s);
        if (t != null) {
            t.printStackTrace();
        }
    }

}
