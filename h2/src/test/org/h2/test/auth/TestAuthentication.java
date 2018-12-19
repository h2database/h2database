/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.test.auth;

import java.io.ByteArrayInputStream;
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
import org.h2.security.auth.H2AuthConfig;
import org.h2.security.auth.H2AuthConfigXml;
import org.h2.security.auth.impl.AssignRealmNameRole;
import org.h2.security.auth.impl.JaasCredentialsValidator;
import org.h2.security.auth.impl.StaticRolesMapper;
import org.h2.security.auth.impl.StaticUserCredentialsValidator;
import org.h2.test.TestBase;

/**
 * Test for custom authentication.
 */
public class TestAuthentication extends TestBase {

    private static final String REALM_NAME = "testRealm"; 
    private static final String JAAS_CONFIG_NAME = "testJaasH2";
    private static final String STATIC_ROLE_NAME = "staticRole"; 
    private static final String EXTERNAL_USER = "user"; 

    private String externalUserPassword;
    private DefaultAuthenticator defaultAuthenticator;
    private Session session;
    private Database database;

    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

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

    private void allTests() throws Exception {
        testInvalidPassword();
        testExternalUserWithoutRealm();
        testExternalUser();
        testAssignRealNameRole();
        testStaticRole();
        testStaticUserCredentials();
        testUserRegistration();
        testSet();
        testDatasource();
        testXmlConfig();
    }

    /**
     * random user password
     */
    String getExternalUserPassword() {
        if (externalUserPassword == null) {
            externalUserPassword = UUID.randomUUID().toString();
        }
        return externalUserPassword;
    }

    private void configureAuthentication(Database database) {
        defaultAuthenticator = new DefaultAuthenticator(true);
        defaultAuthenticator.setAllowUserRegistration(true);
        defaultAuthenticator.setCreateMissingRoles(true);
        defaultAuthenticator.addRealm(REALM_NAME, new JaasCredentialsValidator(JAAS_CONFIG_NAME));
        defaultAuthenticator.addRealm(REALM_NAME + "_STATIC",
                new StaticUserCredentialsValidator("staticuser[0-9]", "staticpassword"));
        defaultAuthenticator.setUserToRolesMappers(new AssignRealmNameRole("@%s"),
                new StaticRolesMapper(STATIC_ROLE_NAME));
        database.setAuthenticator(defaultAuthenticator);
    }

    private void configureJaas() {
        final Configuration innerConfiguration = Configuration.getConfiguration();
        Configuration.setConfiguration(new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                if (name.equals(JAAS_CONFIG_NAME)) {
                    HashMap<String, String> options = new HashMap<>();
                    options.put("password", getExternalUserPassword());
                    return new AppConfigurationEntry[] { new AppConfigurationEntry(MyLoginModule.class.getName(),
                            LoginModuleControlFlag.REQUIRED, options) };
                }
                return innerConfiguration.getAppConfigurationEntry(name);
            }
        });
    }

    private String getDatabaseURL() {
        return "jdbc:h2:mem:" + getClass().getSimpleName();
    }

    private void testInvalidPassword() throws Exception {
        try {
            Connection wrongLoginConnection = DriverManager.getConnection(
                    getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), EXTERNAL_USER, "");
            wrongLoginConnection.close();
            throw new Exception("user should not be able to login with an invalid password");
        } catch (SQLException e) {
        }
    }

    private void testExternalUserWithoutRealm() throws Exception {
        try {
            Connection wrongLoginConnection = DriverManager.getConnection(getDatabaseURL(), EXTERNAL_USER,
                    getExternalUserPassword());
            wrongLoginConnection.close();
            throw new Exception("user should not be able to login without a realm");
        } catch (SQLException e) {
        }
    }

    private void testExternalUser() throws Exception {
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), EXTERNAL_USER,
                getExternalUserPassword());
        try {
            User user = session.getDatabase().findUser((EXTERNAL_USER + "@" + REALM_NAME).toUpperCase());
            assertNotNull(user);
        } finally {
            rightConnection.close();
        }
    }

    private void testDatasource() throws Exception {

        DataSource dataSource = JdbcConnectionPool.create(
                getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), EXTERNAL_USER,
                getExternalUserPassword());
        Connection rightConnection = dataSource.getConnection();
        try {
            User user = session.getDatabase().findUser((EXTERNAL_USER + "@" + REALM_NAME).toUpperCase());
            assertNotNull(user);
        } finally {
            rightConnection.close();
        }
    }

    private void testAssignRealNameRole() throws Exception {
        String realmNameRoleName = "@" + REALM_NAME.toUpperCase();
        Role realmNameRole = database.findRole(realmNameRoleName);
        if (realmNameRole == null) {
            realmNameRole = new Role(database, database.allocateObjectId(), realmNameRoleName, false);
            session.getDatabase().addDatabaseObject(session, realmNameRole);
            session.commit(false);
        }
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), EXTERNAL_USER,
                getExternalUserPassword());
        try {
            User user = session.getDatabase().findUser((EXTERNAL_USER + "@" + REALM_NAME).toUpperCase());
            assertNotNull(user);
            assertTrue(user.isRoleGranted(realmNameRole));
        } finally {
            rightConnection.close();
        }
    }

    private void testStaticRole() throws Exception {
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), EXTERNAL_USER,
                getExternalUserPassword());
        try {
            User user = session.getDatabase().findUser((EXTERNAL_USER + "@" + REALM_NAME).toUpperCase());
            assertNotNull(user);
            Role staticRole = session.getDatabase().findRole(STATIC_ROLE_NAME);
            if (staticRole != null) {
                assertTrue(user.isRoleGranted(staticRole));
            }
        } finally {
            rightConnection.close();
        }
    }

    private void testUserRegistration() throws Exception {
        boolean initialValueAllow = defaultAuthenticator.isAllowUserRegistration();
        defaultAuthenticator.setAllowUserRegistration(false);
        try {
            try {
                Connection wrongLoginConnection = DriverManager.getConnection(
                        getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), "___" + EXTERNAL_USER,
                        "");
                wrongLoginConnection.close();
                throw new Exception(
                        "unregistered external users should not be able to login when allowUserRegistration=false");
            } catch (SQLException e) {
            }
            String validUserName = "new_" + EXTERNAL_USER;
            User validUser = new User(database, database.allocateObjectId(),
                    (validUserName.toUpperCase() + "@" + REALM_NAME).toUpperCase(), false);
            validUser.setUserPasswordHash(new byte[] {});
            database.addDatabaseObject(session, validUser);
            session.commit(false);
            Connection connectionWithRegisterUser = DriverManager.getConnection(
                    getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase(), validUserName,
                    getExternalUserPassword());
            connectionWithRegisterUser.close();
        } finally {
            defaultAuthenticator.setAllowUserRegistration(initialValueAllow);
        }
    }

    private void testStaticUserCredentials() throws Exception {
        String userName="STATICUSER3";
        Connection rightConnection = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + REALM_NAME.toUpperCase()+"_STATIC",userName,
                "staticpassword");
        try {
            User user = session.getDatabase().findUser(userName+ "@" + REALM_NAME.toUpperCase()+"_STATIC");
            assertNotNull(user);
        } finally {
            rightConnection.close();
        }
    }

    private void testSet() throws Exception{
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

    private static final String TESTXML="<h2Auth allowUserRegistration=\"true\" createMissingRoles=\"false\">"
            + "<realm name=\"ciao\" validatorClass=\"myclass\"/>"
            + "<realm name=\"miao\" validatorClass=\"myclass1\">"
            + "<property name=\"prop1\" value=\"value1\"/>"
            + "<userToRolesMapper className=\"class1\">"
            + "<property name=\"prop2\" value=\"value2\"/>"
            + "</userToRolesMapper>"
            + "</realm>"
            + "</h2Auth>";

    private void testXmlConfig() throws Exception {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(TESTXML.getBytes());
        H2AuthConfig config = H2AuthConfigXml.parseFrom(inputStream);
        assertTrue(config.isAllowUserRegistration());
        assertFalse(config.isCreateMissingRoles());
        assertEquals("ciao",config.getRealms().get(0).getName());
        assertEquals("myclass",config.getRealms().get(0).getValidatorClass());
        assertEquals("prop1",config.getRealms().get(1).getProperties().get(0).getName());
        assertEquals("value1",config.getRealms().get(1).getProperties().get(0).getValue());
        assertEquals("class1",config.getUserToRolesMappers().get(0).getClassName());
        assertEquals("prop2",config.getUserToRolesMappers().get(0).getProperties().get(0).getName());
        assertEquals("value2",config.getUserToRolesMappers().get(0).getProperties().get(0).getValue());
    }
}