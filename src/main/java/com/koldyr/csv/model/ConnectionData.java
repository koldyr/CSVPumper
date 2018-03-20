/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.model;

/**
 * Description of class ConnectionData
 *
 * @created: 2018.03.09
 */
public class ConnectionData {
    private final String url;
    private final String schema;
    private final String user;
    private final String password;

    public ConnectionData(String url, String schema, String user, String password) {
        this.url = url;
        this.schema = schema;
        this.user = user;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getSchema() {
        return schema;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
