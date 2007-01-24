package org.h2.command.dml;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.h2.api.DatabaseEventListener;
import org.h2.command.Prepared;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.store.DiskFile;
import org.h2.store.LogFile;
import org.h2.store.LogSystem;
import org.h2.tools.FileBase;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.ObjectArray;

public class Backup extends Prepared {
    
    private String fileName;
    
    public Backup(Session session) {
        super(session);
    }
    
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
    public int update() throws SQLException {
        session.getUser().checkAdmin();
        backupTo(fileName);
        return 0;
    }

    private void backupTo(String fileName) throws SQLException {
        Database db = session.getDatabase();
        if(!db.isPersistent()) {
            throw Message.getSQLException(Message.DATABASE_IS_NOT_PERSISTENT);
        }
        try {
            String name = db.getName();
            name = FileUtils.getFileName(name);
            FileOutputStream zip = new FileOutputStream(fileName);
            ZipOutputStream out = new ZipOutputStream(zip);
            out.putNextEntry(new ZipEntry(name + Constants.SUFFIX_DATA_FILE));
            DiskFile file = db.getDataFile();
            LogSystem log = db.getLog();
            try {
                log.updateKeepFiles(1);
                int pos = -1;
                int max = file.getReadCount();
                while(true) {
                    pos = file.readDirect(pos, out);
                    if(pos < 0) {
                        break;
                    }
                    db.setProgress(DatabaseEventListener.STATE_BACKUP_FILE, name, pos, max);
                }
                out.closeEntry();
                ObjectArray list = log.getActiveLogFiles();
                max = list.size();
                for(int i=0; i<list.size(); i++) {
                    LogFile lf = (LogFile) list.get(i);
                    String fn = lf.getFileName();
                    out.putNextEntry(new ZipEntry(FileUtils.getFileName(fn)));
                    FileInputStream in = new FileInputStream(fn);
                    IOUtils.copyAndCloseInput(in, out);
                    out.closeEntry();
                    db.setProgress(DatabaseEventListener.STATE_BACKUP_FILE, name, i, max);
                }
                int todoLockDatabaseSomehow;
                ArrayList fileList = FileBase.getDatabaseFiles(db.getDatabasePath(), name, true);
                for(int i=0; i<fileList.size(); i++) {
                    String fn = (String) fileList.get(i);
                }
                int todoCopyLobFiles;
                out.close();
                zip.close();
            } finally {
                log.updateKeepFiles(-1);
            }
        } catch(IOException e) {
            throw Message.convert(e);
        }
    }

    public boolean isTransactional() {
        return true;
    }

    public boolean needRecompile() {
        return false;
    }

}
