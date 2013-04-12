/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator;

import com.netflix.curator.ensemble.EnsembleProvider;
import com.netflix.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HandleHolder
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ZookeeperFactory zookeeperFactory;
    private final Watcher watcher;
    private final EnsembleProvider ensembleProvider;
    private final int sessionTimeout;
    private final boolean canBeReadOnly;

    private volatile Helper helper;

    private interface Helper
    {
        ZooKeeper getZooKeeper() throws Exception;
        
        String getConnectionString();

        void setConnectionString(String connectionString) throws Exception;
    }

    HandleHolder(ZookeeperFactory zookeeperFactory, Watcher watcher, EnsembleProvider ensembleProvider, int sessionTimeout, boolean canBeReadOnly)
    {
        this.zookeeperFactory = zookeeperFactory;
        this.watcher = watcher;
        this.ensembleProvider = ensembleProvider;
        this.sessionTimeout = sessionTimeout;
        this.canBeReadOnly = canBeReadOnly;
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        return helper.getZooKeeper();
    }

    String  getConnectionString()
    {
        return (helper != null) ? helper.getConnectionString() : null;
    }

    void handleNewConnectionString() throws Exception
    {
        Helper h = helper;
        if (h != null)
            h.setConnectionString(ensembleProvider.getConnectionString());
        else
            log.info("No Helper to inform about connection string.");
    }

    void closeAndClear() throws Exception
    {
        internalClose();
        helper = null;
    }

    void closeAndReset() throws Exception
    {
        internalClose();

        // first helper is synchronized when getZooKeeper is called. Subsequent calls
        // are not synchronized.
        helper = new Helper()
        {
            private volatile ZooKeeper zooKeeperHandle = null;
            private volatile String connectionString = null;
            private final Object lock = this;

            @Override
            public ZooKeeper getZooKeeper() throws Exception
            {
                log.info("Getting ZooKeeper handle: this=" + System.identityHashCode(lock));
                synchronized(lock)
                {
                    if ( zooKeeperHandle == null )
                    {
                        connectionString = ensembleProvider.getConnectionString();
                        log.info("Opening new ZooKeeper handle to " + connectionString);
                        zooKeeperHandle = zookeeperFactory.newZooKeeper(connectionString, sessionTimeout, watcher, canBeReadOnly);
                    }

                    helper = new Helper()
                    {
                        @Override
                        public ZooKeeper getZooKeeper() throws Exception
                        {
                            return zooKeeperHandle;
                        }

                        @Override
                        public String getConnectionString()
                        {
                            return connectionString;
                        }

                        @Override
                        public void setConnectionString(String value) throws Exception
                        {
                            log.info("Setting ZooKeeper hosts (inner): this=" + System.identityHashCode(lock));
                            // It's not possible to specify "outer-Helper.this" here.
                            synchronized (lock) {
                                log.info("ZooKeeper handle has a session; updating server list to " + value);
                                zooKeeperHandle.updateServerList(value);
                                connectionString = value;
                            }
                        }
                    };

                    return zooKeeperHandle;
                }
            }

            @Override
            public String getConnectionString()
            {
                return connectionString;
            }

            @Override
            public void setConnectionString(String connectionString) throws Exception
            {
                log.info("Setting ZooKeeper hosts (outer): this=" + System.identityHashCode(lock));
                // No-op because we will get it from the EnsembleProvider anyway.
                synchronized(lock) {
                    if (zooKeeperHandle == null) {
                        log.debug("No ZooKeeper handle; discarding connection string " + connectionString);
                        return;
                    }
                    if (helper == this)
                        throw new IllegalStateException("I have a ZooKeeper handle, but am still the helper; discarding connection string " + connectionString);
                    helper.setConnectionString(connectionString);
                }
            }
        };
    }

    private void internalClose() throws Exception
    {
        try
        {
            ZooKeeper zooKeeper = (helper != null) ? helper.getZooKeeper() : null;
            if ( zooKeeper != null )
            {
                Watcher dummyWatcher = new Watcher()
                {
                    @Override
                    public void process(WatchedEvent event)
                    {
                    }
                };
                zooKeeper.register(dummyWatcher);   // clear the default watcher so that no new events get processed by mistake
                zooKeeper.close();
            }
        }
        catch ( InterruptedException dummy )
        {
            Thread.currentThread().interrupt();
        }
    }
}
