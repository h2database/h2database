/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Sven Uhlig <git@resident-uhlig.de>
 */
package org.h2.util;

/**
 * list of Windows error messages that are localized and independent of Java's Locale
 */
public abstract class WindowsLocalizedMessages {
    // "The requested operation cannot be performed on a file with a user-mapped section open"
    private static final String USER_MAPPED_SECTION_OPEN_EN = "user-mapped section open";
    // "Der Vorgang ist bei einer Datei mit einem geöffneten Bereich, der einem Benutzer zugeordnet ist, nicht anwendbar"
    private static final String USER_MAPPED_SECTION_OPEN_DE = "ffneten Bereich, der einem Benutzer zugeordnet";

    // "The process cannot access the file because another process has locked a portion of the file"
    private static final String CANNOT_ACCESS_LOCKED_FILE_EN = "locked";
    // "Der Prozess kann nicht auf die Datei zugreifen, da ein anderer Prozess einen Teil der Datei gesperrt hat"
    private static final String CANNOT_ACCESS_LOCKED_FILE_DE = "gesperrt";

    private WindowsLocalizedMessages() {
    }

    public static boolean isUserMappedSectionOpenMessage(final String message) {
        return message.contains(USER_MAPPED_SECTION_OPEN_EN) ||
            message.contains(USER_MAPPED_SECTION_OPEN_DE);
    }

    public static boolean isCannotAccessLockedFileMessage(final String message) {
        return message.contains(CANNOT_ACCESS_LOCKED_FILE_EN) ||
            message.contains(CANNOT_ACCESS_LOCKED_FILE_DE);
    }
}
