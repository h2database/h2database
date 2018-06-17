/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.test.auth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.sql.DataSource;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.security.auth.DefaultAuthenticator;
import org.h2.security.auth.impl.AssignRealmNameRole;
import org.h2.security.auth.impl.JaasCredentialsValidator;
import org.h2.security.auth.impl.StaticRolesMapper;
import org.h2.security.auth.impl.StaticUserCredentialsValidator;
import org.h2.test.TestBase;

/**
 * Test for custom authentication.
 */
public class TestAuthentication extends TestBase {

    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    String externalUserPassword;


    String getExternalUserPassword() {
        if (externalUserPassword == null) {
            externalUserPassword = UUID.randomUUID().toString();
        }
        return externalUserPassword;
    }

    String getRealmName() {
        return "testRealm";
    }

    String getJaasConfigName() {
        return "testJaasH2";
    }

    String getStaticRoleName() {
        return "staticRole";
    }

    DefaultAuthenticator defaultAuthenticator;

    void configureAuthentication(Database database) {
        defaultAuthenticator = new DefaultAuthenticator(true);
        defaultAuthenticator.setAllowUserRegistration(true);
        defaultAuthenticator.setCreateMissingRoles(true);
        defaultAuthenticator.addRealm(getRealmName(), new JaasCredentialsValidator(getJaasConfigName()));
        defaultAuthenticator.addRealm(getRealmName() + "_STATIC",
                new StaticUserCredentialsValidator("staticuser[0-9]", "staticpassword"));
        defaultAuthenticator.setUserToRolesMappers(new AssignRealmNameRole("@%s"),
                new StaticRolesMapper(getStaticRoleName()));
        database.setAuthenticator(defaultAuthenticator);
    }

    void configureJaas() {
        final Configuration innerConfiguration = Configuration.getConfiguration();
        Configuration.setConfiguration(new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                if (name.equals(getJaasConfigName())) {
                    HashMap<String, String> options = new HashMap<>();
                    options.put("password", getExternalUserPassword());
                    return new AppConfigurationEntry[] { new AppConfigurationEntry(MyLoginModule.class.getName(),
                            LoginModuleControlFlag.REQUIRED, options) };
                }
                return innerConfiguration.getAppConfigurationEntry(name);
            }
        });
    }

    protected String getDatabaseURL() {
        return "jdbc:h2:mem:" + getClass().getSimpleName();
    }

    protected String getExternalUser() {
        return "user";
    }

    Session session;
    Database database;

    @Override
    public void test() throws Exception {
        Configuration oldConfiguration = Configuration.getConfiguration();
        try {
            configureJaas();
            Properties properties = new Properties();
            properties.setProperty("USER", "dba");
            ConnectionInfo connectionInfo = new ConnectionInfo(getDatabaseURL(), properties);
            session = Engine.getInstance().createSession(connectionInfo);
            database = session.getDatabase();
            configureAuthentication(database);
            try {
                allTests();
            } finally {
                session.close();
            }
        } finally {
            Configuration.setConfiguration(oldConfiguration);
        }
    }

    protected void allTests() throws Exception {
        testInvalidPassword();
        testExternalUserWithoutRealm();
        testExternalUser();
        testAssignRealNameRole();
        testStaticRole();
        testStaticUserCredentials();
        testUserRegistration();
        testSet();
        testDatasource();
    }

    protected void testInvalidPassword() throws Exception {
        try {
            Connection wrongLoginConnection = DriverManager.getConnection(
                    getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(), "");
            wrongLoginConnection.close();
            throw new Exception("user should not be able to login with an invalid password");
        } catch (SQLException e) {
        }
    }

    protected void testExternalUserWithoutRealm() throws Exception {
        try {
            Connection wrongLoginConnection = DriverManager.getConnection(getDatabaseURL(), getExternalUser(),
                    getExternalUserPassword());
            wrongLoginConnection.close();
            throw new Exception("user should not be able to login without a realm");
        } catch (SQLException e) {
        }
    }

    protected void testExternalUser() throws Exception {
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword());
        try {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
        } finally {
            rightConnection.close();
        }
    }

    protected void testDatasource() throws Exception {

        DataSource dataSource = JdbcConnectionPool.create(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword());
        Connection rightConnection = dataSource.getConnection();
        try {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
        } finally {
            rightConnection.close();
        }
    }

    protected void testAssignRealNameRole() throws Exception {
        String realmNameRoleName = "@" + getRealmName().toUpperCase();
        Role realmNameRole = database.findRole(realmNameRoleName);
        if (realmNameRole == null) {
            realmNameRole = new Role(database, database.allocateObjectId(), realmNameRoleName, false);
            session.getDatabase().addDatabaseObject(session, realmNameRole);
            session.commit(false);
        }
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword());
        try {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
            assertTrue(user.isRoleGranted(realmNameRole));
        } finally {
            rightConnection.close();
        }
    }

    protected void testStaticRole() throws Exception {
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword());
        try {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
            Role staticRole = session.getDatabase().findRole(getStaticRoleName());
            if (staticRole != null) {
                assertTrue(user.isRoleGranted(staticRole));
            }
        } finally {
            rightConnection.close();
        }
    }

    protected void testUserRegistration() throws Exception {
        boolean initialValueAllow = defaultAuthenticator.isAllowUserRegistration();
        defaultAuthenticator.setAllowUserRegistration(false);
        try {
            try {
                Connection wrongLoginConnection = DriverManager.getConnection(
                        getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), "___" + getExternalUser(),
                        "");
                wrongLoginConnection.close();
                throw new Exception(
                        "unregistered external users should not be able to login when allowUserRegistration=false");
            } catch (SQLException e) {
            }
            String validUserName = "new_" + getExternalUser();
            User validUser = new User(database, database.allocateObjectId(),
                    (validUserName.toUpperCase() + "@" + getRealmName()).toUpperCase(), false);
            validUser.setUserPasswordHash(new byte[] {});
            database.addDatabaseObject(session, validUser);
            session.commit(false);
            Connection connectionWithRegisterUser = DriverManager.getConnection(
                    getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), validUserName,
                    getExternalUserPassword());
            connectionWithRegisterUser.close();
        } finally {
            defaultAuthenticator.setAllowUserRegistration(initialValueAllow);
        }
    }

    public void testStaticUserCredentials() throws Exception {
        String userName="STATICUSER3";
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase()+"_STATIC",userName,
                "staticpassword");
        try {
            User user = session.getDatabase().findUser(userName+ "@" + getRealmName().toUpperCase()+"_STATIC");
            assertNotNull(user);
        } finally {
            rightConnection.close();
        }
    }

    protected void testSet() throws Exception{
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL()+";AUTHENTICATOR=FALSE","DBA","");
        try {
            try {
                testExternalUser();
                throw new Exception("External user shouldn't be allowed");
            } catch (Exception e) {
            }
        } finally {
            configureAuthentication(database);
            rightConnection.close();
        }
        testExternalUser();
    }
}