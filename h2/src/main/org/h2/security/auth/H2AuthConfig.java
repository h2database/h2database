/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import java.util.ArrayList;
import java.util.List;

/**
 * Describe configuration of H2 DefaultAuthenticator
 */
public class H2AuthConfig {

    private boolean allowUserRegistration=true;

    public boolean isAllowUserRegistration() {
        return allowUserRegistration;
    }

    public void setAllowUserRegistration(boolean allowUserRegistration) {
        this.allowUserRegistration = allowUserRegistration;
    }
    
    boolean createMissingRoles=true;

    public boolean isCreateMissingRoles() {
        return createMissingRoles;
    }

    public void setCreateMissingRoles(boolean createMissingRoles) {
        this.createMissingRoles = createMissingRoles;
    }

    List<RealmConfig> realms;

    public List<RealmConfig> getRealms() {
        if (realms == null) {
            realms = new ArrayList<>();
        }
        return realms;
    }

    public void setRealms(List<RealmConfig> realms) {
        this.realms = realms;
    }

    List<UserToRolesMapperConfig> userToRolesMappers;

    public List<UserToRolesMapperConfig> getUserToRolesMappers() {
        if (userToRolesMappers == null) {
            userToRolesMappers = new ArrayList<>();
        }
        return userToRolesMappers;
    }

    public void setUserToRolesMappers(List<UserToRolesMapperConfig> userToRolesMappers) {
        this.userToRolesMappers = userToRolesMappers;
    }
}
