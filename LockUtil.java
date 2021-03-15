package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.HashMap;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (requestType.equals(LockType.NL)) {
            return;
        } else if (!explicitLockType.equals(LockType.NL)) {
            if (LockType.substitutable(LockType.X, explicitLockType)) {
                lockContext.escalate(transaction);
            } else {
                System.out.println("this happens");
                waterFallPromote(transaction, lockContext, LockType.IX);
                lockContext.promote(transaction, requestType);
            }
        } else if (parentContext.getEffectiveLockType(transaction).equals(LockType.NL)) {
                if (requestType.equals(LockType.S)) {
                    waterFallAcquire(transaction, lockContext.lockman.databaseContext(), LockType.IS);
                    lockContext.acquire(transaction, requestType);
                } else {
                    waterFallAcquire(transaction, lockContext.lockman.databaseContext(), LockType.IX);
                    lockContext.acquire(transaction, requestType);
                }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    // Get the current lockContexts of all levels
    private static void waterFallAcquire(TransactionContext transaction, LockContext lockContext, LockType aboveLocks) {
        Long nextTrans = lockContext.children.keySet().iterator().next();
        while (!lockContext.children.isEmpty()) {
            lockContext.acquire(transaction, aboveLocks);
            lockContext = lockContext.childContext(nextTrans);
        }
    }
    private static void waterFallPromote(TransactionContext transaction, LockContext lockContext, LockType aboveLocks) {
        LockContext databaseCont = lockContext.lockman.databaseContext();
        while (!databaseCont.children.isEmpty()) {
            databaseCont.lockman.promote(transaction, databaseCont.name, aboveLocks);
            databaseCont = databaseCont.childContext(transaction.getTransNum());
        }
    }
}
