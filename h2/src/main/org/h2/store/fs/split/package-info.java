/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

/**
 * A file system that may split files into multiple smaller files (required for
 * a FAT32 because it only support files up to 2 GiB).
 */
package org.h2.store.fs.split;
