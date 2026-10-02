package com.simplekafka.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Client for interacting with ZooKeeper
 */
public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;
    
    private final String host;
    private final int port;
    private final long sessionTimeoutMillis;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private volatile SessionListener sessionListener;

    public ZookeeperClient(String host, int port) {
        this(host, port, SESSION_TIMEOUT);
    }

    /**
     * Connect with a shorter wait than the session timeout. Exists so a test does not have
     * to sit out the real one to watch a connection that never succeeds give up.
     */
    ZookeeperClient(String host, int port, long sessionTimeoutMillis) {
        this.host = host;
        this.port = port;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }
    
    /**
     * Connect to ZooKeeper
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        if (!connectedSignal.await(sessionTimeoutMillis, TimeUnit.MILLISECONDS)) {
            // Without a bound this waits for ever when ZooKeeper never answers, and the
            // broker binds its port and sits there, listening and doing nothing, with no
            // line in its log to say why.
            throw new IOException("Timed out after " + sessionTimeoutMillis + "ms connecting to ZooKeeper at "
                    + getConnectString());
        }
        
        // Create required paths if they don't exist
        createPath("/brokers");
        createPath("/topics");
        // /controller is deliberately NOT created here. It has to be the ephemeral
        // node of whichever broker won the election; an empty persistent placeholder
        // would sit in front of it forever and make every broker fight over who gets
        // to delete it before it can elect anyone.
    }
    
    /**
     * Get connection string
     */
    public String getConnectString() {
        return host + ":" + port;
    }
    
    /**
     * Told when this client's session ends and a new one has been established.
     *
     * <p>A session that expired takes everything registered on it: every ephemeral node
     * and every watch. This class cannot put those back, because it does not know what
     * its owner registered - so it says what happened and lets the owner do it.
     */
    public interface SessionListener {
        void onSessionExpired();
    }

    /**
     * Register the listener. Called after the replacement session is connected.
     */
    public void setSessionListener(SessionListener listener) {
        this.sessionListener = listener;
    }

    /**
     * Close connection
     */
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
    
    /**
     * Create a persistent node
     */
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }
    
    /**
     * Create an ephemeral node, but only if it does not exist yet.
     *
     * <p>The create <em>is</em> the existence check: ZooKeeper rejects a create that
     * lost a race with another broker, and that rejection is reported as
     * {@code false} instead of being thrown. "Somebody else got there first" is the
     * normal outcome for every participant but one, so it must not surface as an
     * exception the caller has to log as a failure.
     *
     * @return true when this call created the node
     */
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        try {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    /**
     * Delete a stale, empty placeholder node.
     *
     * <p>Older versions of this broker created {@code /controller} as an empty
     * persistent node on startup, which would block every later election. The delete
     * carries the version that was read so it fails instead of removing a node that
     * somebody replaced in between, and only persistent nodes qualify, so a live
     * controller's ephemeral node can never be deleted this way.
     *
     * @return true when this call removed the placeholder
     */
    public boolean deleteEmptyNode(String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data;
        try {
            data = zooKeeper.getData(path, false, stat);
        } catch (KeeperException.NoNodeException e) {
            return false;
        }

        if (stat.getEphemeralOwner() != 0 || (data != null && data.length > 0)) {
            return false; // a live node, not a leftover placeholder
        }

        try {
            zooKeeper.delete(path, stat.getVersion());
            LOGGER.info("Deleted stale empty node: " + path);
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            return false; // somebody else changed or removed it first
        }
    }

    /**
     * Report a failed watch operation at a level that matches what it means.
     *
     * <p>A session that has expired or a connection that has been lost is a transport
     * condition, and it is the one this class exists to ride out: it reconnects by itself
     * and tells the owner to put its registrations back. Logging it as a broker failure
     * paints a healthy broker's log red every time ZooKeeper blinks - and any line at
     * SEVERE is read as a failure by smoke.sh, so a blip could fail a run against a
     * cluster that did nothing wrong. Anything else here really is unexpected.
     */
    static void logWatchFailure(String what, Exception e) {
        if (e instanceof KeeperException.SessionExpiredException
                || e instanceof KeeperException.ConnectionLossException
                || e instanceof KeeperException.SessionMovedException
                || e instanceof KeeperException.OperationTimeoutException) {
            LOGGER.log(Level.WARNING, what + " - the ZooKeeper session is not usable: " + e.getMessage());
        } else {
            LOGGER.log(Level.SEVERE, what, e);
        }
    }

    /**
     * Delete a node and whatever it holds, if it is there.
     *
     * <p>Unlike {@link #deleteEmptyNode} this makes no assumption about the contents or
     * who owns it: it exists to take back nodes the caller has just written itself, when
     * the work that was going to justify them failed.
     *
     * @return true when this call removed the node
     */
    public boolean deleteNode(String path) throws KeeperException, InterruptedException {
        try {
            zooKeeper.delete(path, -1);
            LOGGER.info("Deleted node: " + path);
            return true;
        } catch (KeeperException.NoNodeException e) {
            return false;
        }
    }

    /**
     * Read a node's data, or null when the node is not there (any more).
     *
     * <p>Used where a missing node is an expected state - the controller can vanish
     * again at any moment - rather than an error worth a stack trace.
     */
    public String readDataIfPresent(String path) throws KeeperException, InterruptedException {
        try {
            byte[] data = zooKeeper.getData(path, false, null);
            return data == null ? "" : new String(data);
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }
    
    /**
     * Check if path exists
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        return stat != null;
    }
    
    /**
     * Get data from a node
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }
    
    /**
     * Set data for a node
     */
    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }
    
    /**
     * Get children of a path
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }
    
    /**
     * Create a path recursively
     */
    private void createPath(String path) {
        try {
            if (path.equals("/")) {
                return;
            }
            
            int lastSlashIndex = path.lastIndexOf('/');
            if (lastSlashIndex > 0) {
                // Create parent path recursively
                String parentPath = path.substring(0, lastSlashIndex);
                createPath(parentPath);
            }
            
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created ZooKeeper path: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }
    
    /**
     * Watch for children changes
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        logWatchFailure("Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            logWatchFailure("Failed to watch children for path: " + path, e);
        }
    }
    
    /**
     * Watch for node changes
     */
    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            logWatchFailure("Failed to watch node: " + path, e);
        }
    }
    
    // Note: there is deliberately no exists-then-delete helper any more. It was the
    // primitive that let two brokers remove each other's freshly elected controller.
    
    /**
     * Process ZooKeeper events
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            reconnectAfterExpiry();
        }
    }

    /**
     * Reconnect after the session ended, off ZooKeeper's event thread.
     *
     * <p>Everything below blocks - closing the old client joins its send thread, and the
     * new one is waited on - and this method is called from the event thread of the client
     * being closed. Blocking it stops it delivering the very replies that are being waited
     * for.
     */
    private void reconnectAfterExpiry() {
        Thread thread = new Thread(() -> {
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
                notifySessionExpired();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }, "zookeeper-reconnect");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Tell the owner its session ended, off ZooKeeper's event thread.
     *
     * <p>The reaction to this has to call back into ZooKeeper to re-register, and a
     * synchronous call made from the event thread waits for a reply that only the event
     * thread can deliver - so it has to happen on a thread of its own. This method is
     * called from {@code process()}, which is that thread.
     */
    private void notifySessionExpired() {
        SessionListener listener = sessionListener;
        if (listener == null) {
            return;
        }

        Thread thread = new Thread(listener::onSessionExpired, "zookeeper-session-expired");
        thread.setDaemon(true);
        thread.start();
    }
    
    /**
     * Callback interface for children changes
     */
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }
    
    /**
     * Callback interface for node changes
     */
    public interface NodeCallback {
        void onNodeChanged();
    }
}