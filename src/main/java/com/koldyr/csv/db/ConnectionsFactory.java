/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.db;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.koldyr.csv.model.ConnectionData;

/**
 * Description of class ConnectionsFactory
 *
 * @created: 2018.03.09
 */
public class ConnectionsFactory extends BasePooledObjectFactory<Connection> {

    private final ConnectionData connectionData;

    public ConnectionsFactory(ConnectionData connectionData) {
        super();
        this.connectionData = connectionData;
    }

    @Override
    public Connection create() throws Exception {
        return DriverManager.getConnection(connectionData.getUrl(), connectionData.getUser(), connectionData.getPassword());
    }

    @Override
    public PooledObject<Connection> wrap(Connection obj) {
        return new DefaultPooledObject<>(obj);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        if (p != null && p.getObject() != null) {
            p.getObject().close();
        }
    }
}
