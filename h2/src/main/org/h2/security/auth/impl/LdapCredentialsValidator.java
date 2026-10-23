/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth.impl;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.ldap.Rdn;

import org.h2.api.CredentialsValidator;
import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.ConfigProperties;

/**
 * Validate credentials by performing an LDAP bind
 * <p>
 * Configuration parameters:
 * </p>
 * <ul>
 *    <li>bindDnPattern bind dn pattern with %u instead of username
 *    (example: uid=%u,ou=users,dc=example,dc=com)</li>
 *    <li>host ldap server</li>
 *    <li>port of ldap service; optional, by default 389 for insecure, 636 for secure</li>
 *    <li>secure, optional by default is true (use SSL)</li>
 * </ul>
 */
public class LdapCredentialsValidator implements CredentialsValidator {

    private String bindDnPattern;
    private String host;
    private int port;
    private boolean secure;
    private String url;

    @Override
    public void configure(ConfigProperties configProperties) {
        bindDnPattern = configProperties.getStringValue("bindDnPattern");
        host = configProperties.getStringValue("host");
        secure = configProperties.getBooleanValue("secure", true);
        port = configProperties.getIntValue("port", secure ? 636 : 389);
        url = "ldap" + (secure ? "s" : "") + "://" + host + ":" + port;
    }

    @Override
    public boolean validateCredentials(AuthenticationInfo authenticationInfo) throws Exception {
        String password = authenticationInfo.getPassword();
        if (password == null || password.isEmpty()) {
            // An empty password with a non-empty bind DN is an LDAP unauthenticated
            // bind (RFC 4513, 5.1.2) that many servers accept, which would let a
            // client authenticate without a valid password.
            return false;
        }
        DirContext dirContext = null;
        try {
            String dn = buildBindDn(bindDnPattern, authenticationInfo.getUserName());
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, url);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, dn);
            env.put(Context.SECURITY_CREDENTIALS, password);
            if (secure) {
                env.put(Context.SECURITY_PROTOCOL,"ssl");
            }
            dirContext = new InitialDirContext(env);
            authenticationInfo.setNestedIdentity(dn);
            return true;
        } finally {
            if (dirContext != null) {
                dirContext.close();
            }
        }

    }

    /**
     * Build the bind DN by substituting the user name into the configured
     * pattern. The user name is supplied by the connecting client, so it is
     * escaped with RFC 4514 DN escaping before substitution; otherwise a name
     * containing DN metacharacters (such as a comma) would change the structure
     * of the DN used for the bind.
     *
     * @param bindDnPattern the pattern with the {@code %u} placeholder
     * @param userName the user name to escape and substitute
     * @return the bind DN
     */
    public static String buildBindDn(String bindDnPattern, String userName) {
        return bindDnPattern.replace("%u", Rdn.escapeValue(userName));
    }

}
