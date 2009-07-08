/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.api.DatabaseEventListener;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.log.LogFile;
import org.h2.log.LogSystem;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.store.DiskFile;
import org.h2.store.FileLister;
import org.h2.store.PageStore;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * BACKUP
 */
public class BackupCommand extends Prepared {

    private Expression fileNameExpr;

    public BackupCommand(Session session) {
        super(session);
    }

    public void setFileName(Expression fileName) {
        this.fileNameExpr = fileName;
    }

    public int update() throws SQLException {
        String name = fileNameExpr.getValue(session).getString();
        session.getUser().checkAdmin();
        backupTo(name);
        return 0;
    }

    private void backupTo(String fileName) throws SQLException {
        Database db = session.getDatabase();
        if (!db.isPersistent()) {
            throw Message.getSQLException(ErrorCode.DATABASE_IS_NOT_PERSISTENT);
        }
        try {
            String name = db.getName();
            name = FileUtils.getFileName(name);
            OutputStream zip = FileUtils.openFileOutputStream(fileName, false);
            ZipOutputStream out = new ZipOutputStream(zip);
            LogSystem log = db.getLog();
            try {
                log.flush();
                log.updateKeepFiles(1);
                String fn;
                if (db.isPageStoreEnabled()) {
                    fn = db.getName() + Constants.SUFFIX_PAGE_FILE;
                    backupPageStore(out, fn, db.getPageStore());
                } else {
                    fn = db.getName() + Constants.SUFFIX_DATA_FILE;
                    backupDiskFile(out, fn, db.getDataFile());
                    fn = db.getName() + Constants.SUFFIX_INDEX_FILE;
                    backupDiskFile(out, fn, db.getIndexFile());
                }
                // synchronize on the database, to avoid concurrent temp file
                // creation / deletion / backup
                String base = FileUtils.getParent(fn);
                synchronized (db.getLobSyncObject()) {
                    if (!db.isPageStoreEnabled()) {
                        ObjectArray<LogFile> list = log.getActiveLogFiles();
                        int max = list.size();
                        for (int i = 0; i < list.size(); i++) {
                            LogFile lf = list.get(i);
                            fn = lf.getFileName();
                            backupFile(out, base, fn);
                            db.setProgress(DatabaseEventListener.STATE_BACKUP_FILE, name, i, max);
                        }
                    }
                    String prefix = db.getDatabasePath();
                    String dir = FileUtils.getParent(prefix);
                    ArrayList<String> fileList = FileLister.getDatabaseFiles(dir, name, true);
                    for (String n : fileList) {
                        if (n.endsWith(Constants.SUFFIX_LOB_FILE)) {
                            backupFile(out, base, n);
                        }
                    }
                }
                out.close();
                zip.close();
            } finally {
                log.updateKeepFiles(-1);
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, fileName);
        }
    }

    private void backupPageStore(ZipOutputStream out, String fileName, PageStore store) throws SQLException, IOException {
        Database db = session.getDatabase();
        fileName = FileUtils.getFileName(fileName);
        out.putNextEntry(new ZipEntry(fileName));
        int max = store.getPageCount();
        int pos = 0;
        while (true) {
            pos = store.copyDirect(pos, out);
            if (pos < 0) {
                break;
            }
            db.setProgress(DatabaseEventListener.STATE_BACKUP_FILE, fileName, pos, max);
        }
        out.closeEntry();
    }

    private void backupDiskFile(ZipOutputStream out, String fileName, DiskFile file) throws SQLException, IOException {
        Database db = session.getDatabase();
        fileName = FileUtils.getFileName(fileName);
        out.putNextEntry(new ZipEntry(fileName));
        int pos = -1;
        int max = file.getReadCount();
        while (true) {
            pos = file.copyDirect(pos, out);
            if (pos < 0) {
                break;
            }
            db.setProgress(DatabaseEventListener.STATE_BACKUP_FILE, fileName, pos, max);
        }
        out.closeEntry();
    }

    private void backupFile(ZipOutputStream out, String base, String fn) throws IOException {
        String f = FileUtils.getAbsolutePath(fn);
        base = FileUtils.getAbsolutePath(base);
        if (!f.startsWith(base)) {
            Message.throwInternalError(f + " does not start with " + base);
        }
        f = f.substring(base.length());
        f = correctFileName(f);
        out.putNextEntry(new ZipEntry(f));
        InputStream in = FileUtils.openFileInputStream(fn);
        IOUtils.copyAndCloseInput(in, out);
        out.closeEntry();
    }

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

    public boolean needRecompile() {
        return false;
    }

    public LocalResult queryMeta() {
        return null;
    }

}
