package com.koldyr.csv.db;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.koldyr.csv.model.ConnectionData;
import com.koldyr.csv.model.PoolType;

/**
 * Description of class ConnectionsFactory
 *
 * @created: 2018.03.09
 */
public class ConnectionsFactory extends BaseKeyedPooledObjectFactory<PoolType, Connection> {

    private final ConnectionData srcConfig;
    private final ConnectionData dstConfig;

    public ConnectionsFactory(ConnectionData srcConfig, ConnectionData dstConfig) {
        super();
        this.srcConfig = srcConfig;
        this.dstConfig = dstConfig;
    }

    @Override
    public Connection create(PoolType key) throws Exception {
        if (key == PoolType.SOURCE) {
            Connection connection = DriverManager.getConnection(srcConfig.getUrl(), srcConfig.getUser(), srcConfig.getPassword());
            connection.setAutoCommit(false);
            return connection;
        }

        Connection connection = DriverManager.getConnection(dstConfig.getUrl(), dstConfig.getUser(), dstConfig.getPassword());
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    public PooledObject<Connection> wrap(Connection obj) {
        return new DefaultPooledObject<>(obj);
    }

    @Override
    public void destroyObject(PoolType key, PooledObject<Connection> p) throws Exception {
        if (p != null && p.getObject() != null) {
            p.getObject().close();
        }
    }
}
