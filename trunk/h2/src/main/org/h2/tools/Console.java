/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.awt.Button;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Label;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemColor;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.InputStream;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.tools.Server;
import org.h2.util.IOUtils;
import org.h2.util.StartBrowser;

/**
 * This tool starts the H2 Console (web-) server, as well as the TCP and ODBC server.
 * For JDK 1.6, a system tray icon is created, for platforms that support it.
 * Otherwise, a small window opens.
 * 
 */
public class Console implements ActionListener, MouseListener {

    private static final Font FONT = new Font("Dialog", Font.PLAIN, 11);
    private Server web;

    /**
     * The command line interface for this tool.
     * The command line options are the same as in the Server tool.
     * 
     * @param args the command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new Console().run(args);
    }

    private void run(String[] args) {
        try {
            web = Server.createWebServer(args);
            web.start();
            Server.createTcpServer(args).start();
            Server.createOdbcServer(args).start();
        } catch (SQLException e) {
            if (e.getErrorCode() == Message.EXCEPTION_OPENING_PORT_1) {
                System.out.println("Port is in use, maybe another server server already running on " + web.getURL());
            } else {
                e.printStackTrace();
            }
        }
        try {
            InputStream in = getClass().getResourceAsStream("/org/h2/res/h2.png");
            Image image = null;
            if(in != null) {
                byte[] imageData = IOUtils.readBytesAndClose(in, -1);
                image = Toolkit.getDefaultToolkit().createImage(imageData);
            }
            if(!createTrayIcon(image)) {
                showWindow(image);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // start browser anyway (even if the server is already running)
        // because some people don't look at the output,
        // but are wondering why nothing happens
        StartBrowser.openURL(web.getURL());
        if (!web.isRunning()) {
            System.exit(1);
        }
    }

    private boolean createTrayIcon(Image image) {
        try {
            // SystemTray.isSupported();
            Boolean supported = (Boolean) Class.forName("java.awt.SystemTray").
                getMethod("isSupported", new Class[0]).
                invoke(null, new Object[0]);
            
            if(!supported.booleanValue()) {
                return false;
            }
            
            PopupMenu menuConsole = new PopupMenu();
            MenuItem itemConsole = new MenuItem("H2 Console");
            itemConsole.setActionCommand("console");
            itemConsole.addActionListener(this);
            itemConsole.setFont(FONT);
            menuConsole.add(itemConsole);
            MenuItem itemExit = new MenuItem("Exit");
            itemExit.setFont(FONT);
            itemExit.setActionCommand("exit");
            itemExit.addActionListener(this);
            menuConsole.add(itemExit);

            // TrayIcon icon = new TrayIcon(image, "H2 Database Engine", menuConsole);
            Object icon = Class.forName("java.awt.TrayIcon").
                getConstructor(new Class[] { Image.class, String.class, PopupMenu.class }).
                newInstance(new Object[] { image, "H2 Database Engine", menuConsole });

            // SystemTray tray = SystemTray.getSystemTray();
            Object tray = Class.forName("java.awt.SystemTray").
                getMethod("getSystemTray", new Class[0]).
                invoke(null, new Object[0]);

            // icon.addMouseListener(this);
            icon.getClass().
                 getMethod("addMouseListener", new Class[]{MouseListener.class}).
                 invoke(icon, new Object[]{this});
             
             //             tray.add(icon);
             tray.getClass().
                getMethod("add", new Class[] { Class.forName("java.awt.TrayIcon") }).
                invoke(tray, new Object[] { icon });
             
             return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void showWindow(Image image) {
        Frame frame = new Frame("H2 Console");
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent we) {
                System.exit(0);
            }
        });
        if(image != null) {
            frame.setIconImage(image);
        }
        frame.setResizable(false);
        frame.setBackground(SystemColor.control);
        
        GridBagLayout layout = new GridBagLayout();
        frame.setLayout(layout);

        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.EAST;
        c.insets.left = 2;
        c.insets.right = 2;
        c.insets.top = 2;
        c.insets.bottom = 2;

        Label label = new Label("H2 Console URL:", Label.LEFT);
        label.setFont(FONT);
        c.anchor = GridBagConstraints.WEST;
        c.gridwidth = GridBagConstraints.EAST;
        frame.add(label, c);

        TextField text = new TextField();
        text.setEditable(false);
        text.setFont(FONT);
        text.setText(web.getURL());
        text.setFocusable(false);
        c.anchor = GridBagConstraints.EAST;
        c.gridwidth = GridBagConstraints.REMAINDER;
        frame.add(text, c);
        
        Label label2 = new Label();
        c.anchor = GridBagConstraints.WEST;
        c.gridwidth = GridBagConstraints.EAST;
        frame.add(label2, c);
        
        Button startBrowser = new Button("Start Browser");
        startBrowser.setFocusable(false);
        startBrowser.setActionCommand("console");
        startBrowser.addActionListener(this);
        startBrowser.setFont(FONT);
        c.anchor = GridBagConstraints.EAST;
        c.gridwidth = GridBagConstraints.REMAINDER;
        frame.add(startBrowser, c);
        
        int width = 250, height = 120;
        frame.setSize(width, height);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        frame.setLocation((screenSize.width - width) / 2, (screenSize.height - height) / 2);
        frame.setVisible(true);
    }

    /**
     * INTERNAL
     */
    public void actionPerformed(ActionEvent e) {
        if (e.getActionCommand().equals("exit")) {
            System.exit(0);
        } else if (e.getActionCommand().equals("console")) {
            startBrowser();
        }
    }

    private void startBrowser() {
        if (web != null) {
            StartBrowser.openURL(web.getURL());
        }
    }

    /**
     * INTERNAL
     */
    public void mouseClicked(MouseEvent e) {
        if(e.getButton() == MouseEvent.BUTTON1) {
            startBrowser();
        }
    }

    /**
     * INTERNAL
     */
    public void mouseEntered(MouseEvent e) {
    }

    /**
     * INTERNAL
     */
    public void mouseExited(MouseEvent e) {
    }

    /**
     * INTERNAL
     */
    public void mousePressed(MouseEvent e) {
    }

    /**
     * INTERNAL
     */
    public void mouseReleased(MouseEvent e) {
    }

}
