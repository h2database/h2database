/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.tools.Server;
import org.h2.util.StartBrowser;

import snoozesoft.systray4j.SysTrayMenu;
import snoozesoft.systray4j.SysTrayMenuEvent;
import snoozesoft.systray4j.SysTrayMenuIcon;
import snoozesoft.systray4j.SysTrayMenuItem;
import snoozesoft.systray4j.SysTrayMenuListener;

public class SysTray implements SysTrayMenuListener {

    Server tcp;
    Server web;
    Server odbc;

    public static void main(String[] args) throws Exception {
        new SysTray().run(args);
    }

    private void startBrowser() {
        if(web != null) {
            StartBrowser.openURL(web.getURL());
        }
    }
    
    private void run(String[] args) {
        try {
            web = Server.createWebServer(args);
            web.start();
            tcp = Server.createTcpServer(args).start();
            odbc = Server.createOdbcServer(args).start();
            createMenu();
        } catch(SQLException e) {
            if(e.getErrorCode() == Message.EXCEPTION_OPENING_PORT_1) {
                System.out.println("Port is in use, maybe another server server already running on " + web.getURL());
            } else {
                e.printStackTrace();
            }
        }
        // start browser anyway (even if the server is already running) 
        // because some people don't look at the output, 
        // but are wondering why nothing happens 
        StartBrowser.openURL(web.getURL());
        if(!web.isRunning()) {
            System.exit(1);
        }
    }

    public void menuItemSelected(SysTrayMenuEvent e) {
        if (e.getActionCommand().equals("exit")) {
            System.exit(0);
        } else if (e.getActionCommand().equals("open")) {
            startBrowser();
        }
    }

    public void iconLeftClicked(SysTrayMenuEvent e) {
        startBrowser();
    }

    public void iconLeftDoubleClicked(SysTrayMenuEvent e) {
        startBrowser();
    }

    void createMenu() {
        SysTrayMenuItem itemExit = new SysTrayMenuItem("Exit", "exit");
        itemExit.addSysTrayMenuListener(this);
        SysTrayMenuItem itemOpen = new SysTrayMenuItem("H2 Console", "open");
        itemOpen.addSysTrayMenuListener(this);
        SysTrayMenuIcon icon;
        icon = new SysTrayMenuIcon(getClass().getResource("/org/h2/h2.ico"));
        SysTrayMenu menu = new SysTrayMenu(icon, "H2 Console");
        icon.addSysTrayMenuListener(this);
        menu.addItem(itemExit);
        menu.addSeparator();
        menu.addItem(itemOpen);
    }

}
