/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Sven Uhlig <git@resident-uhlig.de>
 */
package org.h2.store.fs.niomapped;

/**
 * list of OS messages
 */
abstract class Message {
    static final String USER_MAPPED_SECTION_OPEN_EN = "user-mapped section open";
    static final String USER_MAPPED_SECTION_OPEN_DE = "ffneten Bereich, der einem Benutzer zugeordnet";

    private Message() {
    }
}
