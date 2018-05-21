/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import org.h2.api.Authenticator;

public class AuthenticatorBuilder {

    private static String getAuthenticatorClassNameFrom(String authenticatorString) {
        if (authenticatorString==null) {
            return null;
        }
        switch (authenticatorString) {
        case "":
        case "0":
        case "no":
        case "off":
        case "disable":
        case "false":
            return null;
        case "1":
        case "yes":
        case "on":
        case "enable":
        case "true":
        case "default":
            return "org.h2.security.auth.DefaultAuthenticator";
        default:
            return authenticatorString;
        }
    }

    public static Authenticator buildAuthenticator(String authenticatorStringValue) throws Exception {
        String authenticatorClassName=getAuthenticatorClassNameFrom(authenticatorStringValue);
        return authenticatorClassName==null ? null : (Authenticator) Class.forName(authenticatorClassName).newInstance();
    }
}
