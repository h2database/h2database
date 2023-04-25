/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.ZipOutputStream;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.db.Store;
import org.h2.result.ResultInterface;
import org.h2.store.FileLister;
import org.h2.store.fs.FileUtils;

/**
 * This class represents the statement
 * BACKUP
 */
public class BackupCommand extends Prepared {

    private Expression fileNameExpr;

    public BackupCommand(SessionLocal session) {
        super(session);
    }

    public void setFileName(Expression fileName) {
        this.fileNameExpr = fileName;
    }

    @Override
    public long update() {
        String name = fileNameExpr.getValue(session).getString();
        session.getUser().checkAdmin();
        backupTo(name);
        return 0;
    }

    private void backupTo(String fileName) {
        Database db = getDatabase();
        if (!db.isPersistent()) {
            throw DbException.get(ErrorCode.DATABASE_IS_NOT_PERSISTENT);
        }
        try {
            Store store = db.getStore();
            store.flush();
            String name = db.getName();
            name = FileUtils.getName(name);
            try (OutputStream zip = FileUtils.newOutputStream(fileName, false)) {
                ZipOutputStream out = new ZipOutputStream(zip);
                db.flush();
                // synchronize on the database, to avoid concurrent temp file
                // creation / deletion / backup
                synchronized (db.getLobSyncObject()) {
                    String prefix = db.getDatabasePath();
                    String dir = FileUtils.getParent(prefix);
                    dir = FileLister.getDir(dir);
                    ArrayList<String> fileList = FileLister.getDatabaseFiles(dir, name, true);
                    for (String n : fileList) {
                        if (n.endsWith(Constants.SUFFIX_MV_FILE)) {
                            store.getMvStore().getFileStore().backup(out);
                        }
                    }
                }
                out.close();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    /**
     * Fix the file name, replacing backslash with slash.
     *
     * @param f the file name
     * @return the corrected file name
     */
    public static String correctFileName(String f) {
        f = f.replace('\\', '/');
        if (f.startsWith("/")) {
            f = f.substring(1);
        }
        return f;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.BACKUP;
    }

}
