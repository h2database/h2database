/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.h2.test.trace;

/**
 * A function call argument used by the statement.
 *
 * @author Thomas Mueller
 *
 */
class Arg {
    private Class clazz;
    private Object obj;
    private Statement stat;

    Arg(Player player, Class clazz, Object obj) {
        this.clazz = clazz;
        this.obj = obj;
    }

    Arg(Statement stat) {
        this.stat = stat;
    }

    public String toString() {
        if (stat != null) {
            return stat.toString();
        } else {
            return StringTools.quote(clazz, getValue());
        }
    }

    void execute() throws Exception {
        if (stat != null) {
            stat.execute();
            clazz = stat.getReturnClass();
            obj = stat.getReturnObject();
            stat = null;
        }
    }

    Class getValueClass() {
        return clazz;
    }

    Object getValue() {
        return obj;
    }
}
