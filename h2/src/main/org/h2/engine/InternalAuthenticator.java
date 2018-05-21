/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.engine;

import org.h2.api.Authenticator;
import org.h2.security.auth.AuthConfigException;
import org.h2.security.auth.AuthenticationException;
import org.h2.security.auth.AuthenticationInfo;

/**
 *    Default authentication implementation. It validate user and password internally on database
 */
public class InternalAuthenticator implements Authenticator {

    public static final InternalAuthenticator INSTANCE = new InternalAuthenticator();

    @Override
    public User authenticate(AuthenticationInfo authenticationInfo, Database database) throws AuthenticationException {
        User user = database.findUser(authenticationInfo.getUserName());
        if (user != null) {
            if (!user.validateUserPasswordHash(authenticationInfo.getConnectionInfo().getUserPasswordHash())) {
                user = null;
            }
        }
        return user;
    }

    @Override
    public void init(Database database) throws AuthConfigException {
    }

}
