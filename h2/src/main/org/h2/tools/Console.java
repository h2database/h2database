/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

//## AWT begin ##
import java.awt.Button;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GraphicsEnvironment;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.Label;
import java.awt.MenuItem;
import java.awt.Panel;
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
import org.h2.util.Resources;

import java.io.IOException;
//## AWT end ##
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.server.ShutdownHandler;
import org.h2.util.StartBrowser;

/**
 * This tool starts the H2 Console (web-) server, as well as the TCP and PG
 * server. For JDK 1.6, a system tray icon is created, for platforms that
 * support it. Otherwise, a small window opens.
 * 
 * @author Thomas Mueller, Ridvan Agar
 */
public class Console implements
//## AWT begin ##
ActionListener, MouseListener,
//## AWT end ##
ShutdownHandler {

    private static final int EXIT_ERROR = 1;

//## AWT begin ##
    Frame frame;
    private Font font;
    private Image icon16, icon24;
    private Button startBrowser;
//## AWT end ##
    private Server web, tcp, pg;
    private boolean isWindows;

    /**
     * The command line interface for this tool.
     * The command line options are the same as in the Server tool,
     * but this tool will always start the TCP, TCP and PG server.
     * Options are case sensitive.
     *
     * @param args the command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = new Console().run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private int run(String[] args) {
        isWindows = SysProperties.getStringSetting("os.name", "").startsWith("Windows");
        int exitCode = 0;
        try {
            web = Server.createWebServer(args);
            web.setShutdownHandler(this);
            web.start();
        } catch (SQLException e) {
            if (web == null) {
                e.printStackTrace();
            } else {
                System.out.println(web.getStatus());
            }
        }
        try {
            tcp = Server.createTcpServer(args);
            tcp.start();
        } catch (SQLException e) {
            if (tcp == null) {
                e.printStackTrace();
            } else {
                System.out.println(tcp.getStatus());
            }
        }
        try {
            pg = Server.createPgServer(args);
            pg.start();
        } catch (SQLException e) {
            if (pg == null) {
                e.printStackTrace();
            } else {
                System.out.println(pg.getStatus());
            }
        }
//## AWT begin ##
        if (!GraphicsEnvironment.isHeadless()) {
            if (isWindows) {
                font = new Font("Dialog", Font.PLAIN, 11);
            } else {
                font = new Font("Dialog", Font.PLAIN, 12);
            }
            try {
                icon16 = loadImage("/org/h2/res/h2.png");
                icon24 = loadImage("/org/h2/res/h2b.png");
                if (!createTrayIcon()) {
                    showWindow(true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//## AWT end ##

        // start browser anyway (even if the server is already running)
        // because some people don't look at the output,
        // but are wondering why nothing happens
        StartBrowser.openURL(web.getURL());
        if (!web.isRunning(true)) {
            exitCode = EXIT_ERROR;
        }
        return exitCode;
    }

    private Image loadImage(String name) throws IOException {
        byte[] imageData = Resources.get(name);
        if (imageData == null) {
            return null;
        }
        return Toolkit.getDefaultToolkit().createImage(imageData);
    }

    /**
     * INTERNAL
     */
    public void shutdown() {
        stopAll();
    }

    void stopAll() {
        if (web != null && web.isRunning(false)) {
            web.stop();
            web = null;
        }
        if (tcp != null && tcp.isRunning(false)) {
            tcp.stop();
            tcp = null;
        }
        if (pg != null && pg.isRunning(false)) {
            pg.stop();
            pg = null;
        }
//## AWT begin ##
        if (frame != null) {
            frame.dispose();
            frame = null;
        }
//## AWT end ##
        System.exit(0);
    }

//## AWT begin ##
    private boolean createTrayIcon() {
        try {
            // SystemTray.isSupported();
            Boolean supported = (Boolean) Class.forName("java.awt.SystemTray").
                getMethod("isSupported", new Class[0]).
                invoke(null, new Object[0]);

            if (!supported.booleanValue()) {
                return false;
            }

            PopupMenu menuConsole = new PopupMenu();
            MenuItem itemConsole = new MenuItem("H2 Console");
            itemConsole.setActionCommand("console");
            itemConsole.addActionListener(this);
            itemConsole.setFont(font);
            menuConsole.add(itemConsole);
            MenuItem itemStatus = new MenuItem("Status");
            itemStatus.setActionCommand("status");
            itemStatus.addActionListener(this);
            itemStatus.setFont(font);
            menuConsole.add(itemStatus);
            MenuItem itemExit = new MenuItem("Exit");
            itemExit.setFont(font);
            itemExit.setActionCommand("exit");
            itemExit.addActionListener(this);
            menuConsole.add(itemExit);

            // SystemTray tray = SystemTray.getSystemTray();
            Object tray = Class.forName("java.awt.SystemTray").
                getMethod("getSystemTray", new Class[0]).
                invoke(null, new Object[0]);

            // Dimension d = tray.getTrayIconSize();
            Dimension d = (Dimension) Class.forName("java.awt.SystemTray").
                getMethod("getTrayIconSize", new Class[0]).
                invoke(tray, new Object[0]);

            Image icon = (d.width >= 24 && d.height >= 24) ? icon24 : icon16;

            // TrayIcon icon = new TrayIcon(image, "H2 Database Engine", menuConsole);
            Object trayIcon = Class.forName("java.awt.TrayIcon").
                getConstructor(new Class[] { Image.class, String.class, PopupMenu.class }).
                newInstance(new Object[] { icon, "H2 Database Engine", menuConsole });

            // trayIcon.addMouseListener(this);
            trayIcon.getClass().
                 getMethod("addMouseListener", new Class[]{MouseListener.class}).
                 invoke(trayIcon, new Object[]{this});

             // tray.add(icon);
             tray.getClass().
                getMethod("add", new Class[] { Class.forName("java.awt.TrayIcon") }).
                invoke(tray, new Object[] { trayIcon });

             return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void showWindow(final boolean exit) {
        frame = new Frame("H2 Console");
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent we) {
                if (exit) {
                    stopAll();
                } else {
                    frame.dispose();
                }
            }
        });
        if (icon16 != null) {
            frame.setIconImage(icon16);
        }
        frame.setResizable(false);
        frame.setBackground(SystemColor.control);

        GridBagLayout layout = new GridBagLayout();
        frame.setLayout(layout);

        // the main panel keeps everything together
        Panel mainPanel = new Panel(layout);

        GridBagConstraints constraintsPanel = new GridBagConstraints();
        constraintsPanel.gridx = 0;
        constraintsPanel.weightx = 1.0D;
        constraintsPanel.weighty = 1.0D;
        constraintsPanel.fill = GridBagConstraints.BOTH;
        constraintsPanel.insets = new Insets(0, 10, 0, 10);
        constraintsPanel.gridy = 0;

        GridBagConstraints constraintsButton = new GridBagConstraints();
        constraintsButton.gridx = 0;
        constraintsButton.gridwidth = 2;
        constraintsButton.insets = new Insets(10, 0, 0, 0);
        constraintsButton.gridy = 1;
        constraintsButton.anchor = GridBagConstraints.EAST;

        GridBagConstraints constraintsTextField = new GridBagConstraints();
        constraintsTextField.fill = GridBagConstraints.HORIZONTAL;
        constraintsTextField.gridy = 0;
        constraintsTextField.weightx = 1.0;
        constraintsTextField.insets = new Insets(0, 5, 0, 0);
        constraintsTextField.gridx = 1;

        GridBagConstraints constraintsLabel = new GridBagConstraints();
        constraintsLabel.gridx = 0;
        constraintsLabel.gridy = 0;

        Label label = new Label("H2 Console URL:", Label.LEFT);
        label.setFont(font);
        mainPanel.add(label, constraintsLabel);

        TextField text = new TextField();
        text.setEditable(false);
        text.setFont(font);
        text.setText(web.getURL());
        if (isWindows) {
            text.setFocusable(false);
        }
        mainPanel.add(text, constraintsTextField);

        startBrowser = new Button("Start Browser");
        startBrowser.setFocusable(false);
        startBrowser.setActionCommand("console");
        startBrowser.addActionListener(this);
        startBrowser.setFont(font);
        mainPanel.add(startBrowser, constraintsButton);
        frame.add(mainPanel, constraintsPanel);

        int width = 300, height = 120;
        frame.setSize(width, height);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        frame.setLocation((screenSize.width - width) / 2, (screenSize.height - height) / 2);
        try {
            frame.setVisible(true);
        } catch (Throwable t) {
            // ignore
            // some systems don't support this method, for example IKVM
            // however it still works
        }
    }

    private void startBrowser() {
        if (web != null) {
            StartBrowser.openURL(web.getURL());
        }
    }
//## AWT end ##

    /**
     * INTERNAL
     */
//## AWT begin ##
    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();
        if ("exit".equals(command)) {
            stopAll();
        } else if ("console".equals(command)) {
            startBrowser();
        } else if ("status".equals(command)) {
            showWindow(false);
        } else if (startBrowser == e.getSource()) {
            // for some reason, IKVM ignores setActionCommand
            startBrowser();
        }
    }
//## AWT end ##

    /**
     * INTERNAL
     */
//## AWT begin ##
    public void mouseClicked(MouseEvent e) {
        if (e.getButton() == MouseEvent.BUTTON1) {
            startBrowser();
        }
    }
//## AWT end ##

    /**
     * INTERNAL
     */
//## AWT begin ##
    public void mouseEntered(MouseEvent e) {
        // nothing to do
    }
//## AWT end ##

    /**
     * INTERNAL
     */
//## AWT begin ##
    public void mouseExited(MouseEvent e) {
        // nothing to do
    }
//## AWT end ##

    /**
     * INTERNAL
     */
//## AWT begin ##
    public void mousePressed(MouseEvent e) {
        // nothing to do
    }
//## AWT end ##

    /**
     * INTERNAL
     */
//## AWT begin ##
    public void mouseReleased(MouseEvent e) {
        // nothing to do
    }
//## AWT end ##

}
