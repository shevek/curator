/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.locks;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.ensemble.EnsembleListener;
import com.netflix.curator.ensemble.EnsembleProvider;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingCluster;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestInterProcessSemaphoreCluster
{
    @Test
    public void     testKilledServerWithEnsembleProvider() throws Exception
    {
        final int           CLIENT_QTY = 10;
        final Timing        timing = new Timing();
        final String        PATH = "/foo/bar/lock";

        ExecutorService                 executorService = Executors.newFixedThreadPool(CLIENT_QTY);
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        TestingCluster                  cluster = new TestingCluster(3);
        try
        {
            cluster.start();

            final AtomicReference<String>   connectionString = new AtomicReference<String>(cluster.getConnectString());
            final EnsembleProvider          provider = new EnsembleProvider()
            {
                @Override
                public void start() throws Exception
                {
                }

                @Override
                public String getConnectionString()
                {
                    return connectionString.get();
                }

                @Override
                public void close() throws IOException
                {
                }

				@Override
				public void addListener(EnsembleListener listener) {
				}
            };

            final Semaphore             acquiredSemaphore = new Semaphore(0);
            final AtomicInteger         acquireCount = new AtomicInteger(0);
            final CountDownLatch        suspendedLatch = new CountDownLatch(CLIENT_QTY);
            for ( int i = 0; i < CLIENT_QTY; ++i )
            {
                completionService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            CuratorFramework        client = CuratorFrameworkFactory.builder()
                                .ensembleProvider(provider)
                                .sessionTimeoutMs(timing.session())
                                .connectionTimeoutMs(timing.connection())
                                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                                .build();
                            try
                            {
                                final Semaphore     suspendedSemaphore = new Semaphore(0);
                                client.getConnectionStateListenable().addListener
                                (
                                    new ConnectionStateListener()
                                    {
                                        @Override
                                        public void stateChanged(CuratorFramework client, ConnectionState newState)
                                        {
                                            if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) )
                                            {
                                                suspendedLatch.countDown();
                                                suspendedSemaphore.release();
                                            }
                                        }
                                    }
                                );

                                client.start();

                                InterProcessSemaphoreV2   semaphore = new InterProcessSemaphoreV2(client, PATH, 1);

                                while ( !Thread.currentThread().isInterrupted() )
                                {
                                    Lease   lease = null;
                                    try
                                    {
                                        lease = semaphore.acquire();
                                        acquiredSemaphore.release();
                                        acquireCount.incrementAndGet();
                                        suspendedSemaphore.acquire();
                                    }
                                    catch ( Exception e )
                                    {
                                        // just retry
                                    }
                                    finally
                                    {
                                        if ( lease != null )
                                        {
                                            acquireCount.decrementAndGet();
                                            Closeables.close(lease, true);
                                        }
                                    }
                                }
                            }
                            finally
                            {
                                Closeables.close(client, true);
                            }
                            return null;
                        }
                    }
                );
            }

            Assert.assertTrue(timing.acquireSemaphore(acquiredSemaphore));
            Assert.assertEquals(1, acquireCount.get());

            cluster.close();
            timing.awaitLatch(suspendedLatch);
            timing.forWaiting().sleepABit();
            Assert.assertEquals(0, acquireCount.get());

            cluster = new TestingCluster(3);
            cluster.start();

            connectionString.set(cluster.getConnectString());
            timing.forWaiting().sleepABit();

            Assert.assertTrue(timing.acquireSemaphore(acquiredSemaphore));
            timing.forWaiting().sleepABit();
            Assert.assertEquals(1, acquireCount.get());
        }
        finally
        {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
            executorService.shutdownNow();
            Closeables.close(cluster, true);
        }
    }

    @Test
    public void     testCluster() throws Exception
    {
        final int           QTY = 20;
        final int           OPERATION_TIME_MS = 1000;
        final String        PATH = "/foo/bar/lock";

        ExecutorService                 executorService = Executors.newFixedThreadPool(QTY);
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        final Timing                    timing = new Timing();
        TestingCluster                  cluster = new TestingCluster(3);
        List<SemaphoreClient>           semaphoreClients = Lists.newArrayList();
        try
        {
            cluster.start();

            final AtomicInteger         opCount = new AtomicInteger(0);
            for ( int i = 0; i < QTY; ++i )
            {
                SemaphoreClient semaphoreClient = new SemaphoreClient
                (
                    cluster.getConnectString(),
                    PATH,
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            opCount.incrementAndGet();
                            Thread.sleep(OPERATION_TIME_MS);
                            return null;
                        }
                    }
                );
                completionService.submit(semaphoreClient);
                semaphoreClients.add(semaphoreClient);
            }

            timing.forWaiting().sleepABit();

            Assert.assertNotNull(SemaphoreClient.getActiveClient());

            final CountDownLatch    latch = new CountDownLatch(1);
            CuratorFramework        client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        latch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            client.start();
            try
            {
                client.getZookeeperClient().blockUntilConnectedOrTimedOut();

                cluster.stop();

                latch.await();
            }
            finally
            {
                Closeables.close(client, true);
            }

            long        startTicks = System.currentTimeMillis();
            for(;;)
            {
                int     thisOpCount = opCount.get();
                Thread.sleep(2 * OPERATION_TIME_MS);
                if ( thisOpCount == opCount.get() )
                {
                    break;  // checking that the op count isn't increasing
                }
                Assert.assertTrue((System.currentTimeMillis() - startTicks) < timing.forWaiting().milliseconds());
            }

            int     thisOpCount = opCount.get();

            Iterator<InstanceSpec> iterator = cluster.getInstances().iterator();
            cluster = new TestingCluster(iterator.next(), iterator.next());
            cluster.start();
            timing.forWaiting().sleepABit();

            startTicks = System.currentTimeMillis();
            for(;;)
            {
                Thread.sleep(2 * OPERATION_TIME_MS);
                if ( opCount.get() > thisOpCount )
                {
                    break;  // checking that semaphore has started working again
                }
                Assert.assertTrue((System.currentTimeMillis() - startTicks) < timing.forWaiting().milliseconds());
            }
        }
        finally
        {
            for ( SemaphoreClient semaphoreClient : semaphoreClients )
            {
                Closeables.close(semaphoreClient, true);
            }
            Closeables.close(cluster, true);
            executorService.shutdownNow();
        }
    }
}
