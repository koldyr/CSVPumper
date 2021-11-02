package com.koldyr.csv.db

import com.koldyr.csv.model.ConnectionData
import com.koldyr.csv.model.PoolType
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import java.sql.Connection
import java.sql.DriverManager

/**
 * Description of class ConnectionsFactory
 *
 * @created: 2018.03.09
 */
class ConnectionsFactory(private val srcConfig: ConnectionData, private val dstConfig: ConnectionData) : BaseKeyedPooledObjectFactory<PoolType, Connection>() {

    @Throws(Exception::class)
    override fun create(key: PoolType): Connection {
        if (key === PoolType.SOURCE) {
            val connection = DriverManager.getConnection(srcConfig.url, srcConfig.user, srcConfig.password)
            connection.autoCommit = false
            return connection
        }

        val connection = DriverManager.getConnection(dstConfig.url, dstConfig.user, dstConfig.password)
        connection.autoCommit = false
        return connection
    }

    override fun wrap(obj: Connection): PooledObject<Connection> {
        return DefaultPooledObject(obj)
    }

    @Throws(Exception::class)
    override fun destroyObject(key: PoolType, p: PooledObject<Connection>) {
        if (p.getObject() != null) {
            p.getObject().close()
        }
    }
}
