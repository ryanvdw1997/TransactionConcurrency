package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (this.readonly) {
            throw new UnsupportedOperationException("Context is read only");
        } else if (parent == null || LockType.canBeParentLock(parentContext().lockman.getLockType(transaction, parent.name),
                lockType)) {
            lockman.acquire(transaction, name, lockType);
            if (parent != null) {
                if (parent.numChildLocks.containsKey(transaction.getTransNum())) {
                    int numChild = parent.numChildLocks.get(transaction.getTransNum());
                    parent.numChildLocks.replace(transaction.getTransNum(), numChild, numChild+1);
                } else {
                    parent.numChildLocks.put(transaction.getTransNum(), 1);
                }
            }
        } else {
            throw new InvalidLockException("Acquisition is not allowed");
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (this.readonly) {
            throw new UnsupportedOperationException("Context is read-only");
        } else if (numChildLocks != null && numChildLocks.containsKey(transaction.getTransNum())) {
            if (numChildLocks.get(transaction.getTransNum()) > 0) {
                throw new InvalidLockException("Must release from the bottom up");
            }
        }
        else {
            if (parent != null && parent.numChildLocks.containsKey(transaction.getTransNum())) {
                int target = parent.numChildLocks.get(transaction.getTransNum());
                parent.numChildLocks.replace(transaction.getTransNum(), target, target - 1);
            }
            lockman.release(transaction, name);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        LockType currLock = getExplicitLockType(transaction);
        if (readonly) {
            throw new UnsupportedOperationException("Context is read-only");
        } else if (currLock.equals(newLockType) || !LockType.substitutable(newLockType, currLock)
                || (parent != null && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), newLockType))) {
            throw new InvalidLockException("Cannot promote this lock");
        } else {
            lockman.promote(transaction, name, newLockType);
        }
            if (newLockType.equals(LockType.SIX) &&
                    (getExplicitLockType(transaction).equals(LockType.IS) || getExplicitLockType(transaction).equals(LockType.IX))) {
                for (ResourceName releaseName : sisDescendants(transaction)) {
                    lockman.release(transaction, releaseName);
                }
            }
        }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("This is context is read-only");
        } else if (getExplicitLockType(transaction).equals(LockType.S) ||
                getExplicitLockType(transaction).equals(LockType.X)) {
            return;
        } else if (getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("no lock held at this level.");
        } else {
            List<ResourceName> releases = new ArrayList<>();
            LockContext currCont = children.get(transaction.getTransNum());
            while (currCont != null) {
                for (LockContext child : currCont.children.values()) {
                    if (!child.lockman.getLockType(transaction, child.getResourceName()).equals(LockType.NL)) {
                        releases.add(child.getResourceName());
                    }
                }
                currCont = currCont.children.get(transaction.getTransNum());
            }
            if (getExplicitLockType(transaction).equals(LockType.IS)) {
                lockman.acquireAndRelease(transaction, name, LockType.S, releases);
            } else {
                lockman.acquireAndRelease(transaction, name, LockType.X, releases);
            }
        }
        if (numChildLocks.containsKey(transaction.getTransNum())) {
            int currCount = numChildLocks.get(transaction.getTransNum());
            numChildLocks.replace(transaction.getTransNum(), currCount, 0);
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        List<LockType> intents = new ArrayList<>();
        intents.add(LockType.IS);
        intents.add(LockType.IX);
        if (transaction == null) {
            return LockType.NL;
        }
        if (!getExplicitLockType(transaction).equals(LockType.NL)) {
            return getExplicitLockType(transaction);
        } else {
            LockContext parenText = parent;
            while (parenText != null) {
                if (parenText.getExplicitLockType(transaction).equals(LockType.NL)) {
                    parenText = parenText.parent;
                } else if (intents.contains(parenText.getExplicitLockType(transaction))) {
                    break;
                } else {
                    return parenText.getExplicitLockType(transaction);
                }
            }
        }
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> rNames = new ArrayList<>();
        LockContext currCont = this;
        while (currCont.getNumChildren(transaction) > 0) {
            LockContext child = currCont.children.get(transaction.getTransNum());
            LockType childLock = child.getExplicitLockType(transaction);
            if (childLock.equals(LockType.IS) || childLock.equals(LockType.S)) {
                rNames.add(child.name);
            } else {
                currCont = child;
            }
        }
        return rNames;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

