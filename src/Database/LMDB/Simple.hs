
{-# LANGUAGE ConstraintKinds #-}

{-|
Module      : Database.LMDB.Simple
Description : Simple Haskell API for LMDB
Copyright   : © 2017–2018 Robert Leslie
License     : BSD3
Maintainer  : rob@mars.org
Stability   : experimental

This module provides a simple Haskell API for the
<https://symas.com/lightning-memory-mapped-database/ Lightning Memory-mapped Database>
(LMDB).

Example usage:

@
module Main where

import Database.LMDB.Simple
import Control.Monad (forM_)

main = do
  env <- openEnvironment "myenv" defaultLimits
  db <- readOnlyTransaction env $ getDatabase Nothing :: IO (Database String Int)

  transaction env $
    forM_ [("one",1),("two",2),("three",3)] $ \\(k,v) -> put db k (Just v)

  print =<< readOnlyTransaction env (get db "two")   -- Just 2
  print =<< readOnlyTransaction env (get db "nine")  -- Nothing
@

These additional APIs are available:

  * "Database.LMDB.Simple.Extra" provides additional functions for querying
    and modifying LMDB databases from within the 'Transaction' monad

  * "Database.LMDB.Simple.View" provides a read-only snapshot of an LMDB
    database that can be accessed from pure code

  * "Database.LMDB.Simple.DBRef" provides a mutable variable (accessed from
    'IO') that is tied to a particular key that persists in an LMDB database
-}

module Database.LMDB.Simple
  ( -- * Environments
    Environment
  , Limits (..)
  , defaultLimits
  , openEnvironment
  , openReadWriteEnvironment
  , openReadOnlyEnvironment
  , readOnlyEnvironment
  , clearStaleReaders
  , delaySync
  , delayMetaSync
  , isSyncDelayed
  , isMetaSyncDelayed

    -- * Transactions
  , Transaction
  , transaction
  , readWriteTransaction
  , readOnlyTransaction
  , nestTransaction
  , abort
  , AbortedTransaction

    -- * Databases
  , Database
  , getDatabase
  , get
  , put
  , clear

    -- * Access modes
  , ReadWrite
  , ReadOnly
  , Mode
  , SubMode
  ) where

import Control.Concurrent
  ( runInBoundThread
  )
import Control.Concurrent.MVar
  ( takeMVar
  , putMVar
  )
import Control.Exception
  ( Exception
  , throwIO
  , try
  , tryJust
  , bracketOnError
  )

import Control.Monad
  ( guard
  , void
  )

import Control.Monad.IO.Class
  ( MonadIO (liftIO)
  )

import Data.Coerce
  ( coerce
  )

import Database.LMDB.Raw
  ( LMDB_Error (LMDB_Error, e_code)
  , MDB_EnvFlag (MDB_NOSUBDIR, MDB_RDONLY, MDB_NOSYNC, MDB_NOMETASYNC)
  , MDB_DbFlag (MDB_CREATE)
  , mdb_env_create
  , mdb_env_open
  , mdb_env_get_flags
  , mdb_env_set_flags
  , mdb_env_set_mapsize
  , mdb_env_set_maxdbs
  , mdb_env_set_maxreaders
  , mdb_env_sync_flush
  , mdb_env_unset_flags
  , mdb_dbi_open'
  , mdb_txn_begin
  , mdb_txn_commit
  , mdb_txn_abort
  , mdb_txn_env
  , mdb_clear'
  , mdb_reader_check
  )

import Database.LMDB.Simple.Internal
  ( ReadWrite
  , ReadOnly
  , Mode
  , SubMode
  , Environment (Env)
  , Transaction (Txn)
  , Database (Db)
  , Serialise
  , isReadOnlyEnvironment
  , isReadOnlyTransaction
  , isReadWriteTransaction
  , mkNewEnvironment
  )
import qualified Database.LMDB.Simple.Internal as Internal

import Foreign.C
  ( Errno (Errno)
  , eNOTDIR
  )

-- | LMDB environments have various limits on the size and number of databases
-- and concurrent readers.
data Limits = Limits
  { mapSize      :: Int  -- ^ memory map size, in bytes (also the maximum size
                         -- of all databases)
  , maxDatabases :: Int  -- ^ maximum number of named databases
  , maxReaders   :: Int  -- ^ maximum number of concurrent 'ReadOnly'
                         -- transactions (also the number of slots in the lock
                         -- table)
  }

-- | The default limits are 1 MiB map size, 0 named databases, and 126
-- concurrent readers. These can be adjusted freely, and in particular the
-- 'mapSize' may be set very large (limited only by available address
-- space). However, LMDB is not optimized for a large number of named
-- databases so 'maxDatabases' should be kept to a minimum.
--
-- The default 'mapSize' is intentionally small, and should be changed to
-- something appropriate for your application. It ought to be a multiple of
-- the OS page size, and should be chosen as large as possible to accommodate
-- future growth of the database(s). Once set for an environment, this limit
-- cannot be reduced to a value smaller than the space already consumed by the
-- environment, however it can later be increased.
--
-- If you are going to use any named databases then you will need to change
-- 'maxDatabases' to the number of named databases you plan to use. However,
-- you do not need to change this field if you are only going to use the
-- single main (unnamed) database.
defaultLimits :: Limits
defaultLimits = Limits
  { mapSize      = 1024 * 1024  -- 1 MiB
  , maxDatabases = 0
  , maxReaders   = 126
  }

-- | Open an LMDB environment in either 'ReadWrite' or 'ReadOnly' mode. The
-- 'FilePath' argument may be either a directory or a regular file, but it
-- must already exist. If a regular file, an additional file with "-lock"
-- appended to the name is used for the reader lock table.
--
-- Note that an environment must have been opened in 'ReadWrite' mode at least
-- once before it can be opened in 'ReadOnly' mode.
--
-- An environment opened in 'ReadOnly' mode may still modify the reader lock
-- table (except when the filesystem is read-only, in which case no locks are
-- used).
openEnvironment :: Mode mode => FilePath -> Limits -> IO (Environment mode)
openEnvironment path limits = do
  env <- mdb_env_create

  mdb_env_set_mapsize    env (mapSize      limits)
  mdb_env_set_maxdbs     env (maxDatabases limits)
  mdb_env_set_maxreaders env (maxReaders   limits)

  environ <- mkNewEnvironment env :: Mode mode => IO (Environment mode)
  let flags   = [MDB_RDONLY | isReadOnlyEnvironment environ]

  r <- tryJust (guard . isNotDirectoryError) $ mdb_env_open env path flags
  case r of
    Left  _ -> mdb_env_open env path (MDB_NOSUBDIR : flags)
    Right _ -> return ()

  return environ

  where isNotDirectoryError :: LMDB_Error -> Bool
        isNotDirectoryError LMDB_Error { e_code = Left code }
          | Errno (fromIntegral code) == eNOTDIR = True
        isNotDirectoryError _                    = False

-- | Convenience function for opening an LMDB environment in 'ReadWrite'
-- mode; see 'openEnvironment'
openReadWriteEnvironment :: FilePath -> Limits -> IO (Environment ReadWrite)
openReadWriteEnvironment = openEnvironment

-- | Convenience function for opening an LMDB environment in 'ReadOnly'
-- mode; see 'openEnvironment'
openReadOnlyEnvironment :: FilePath -> Limits -> IO (Environment ReadOnly)
openReadOnlyEnvironment = openEnvironment

-- | For an LMDB environment opened in 'ReadWrite' mode, return the same
-- environment restricted to 'ReadOnly' mode. This can be used to prevent
-- modification of the environment for a subset of an application while
-- retaining the ability to perform modifications in another subset.
--
-- @since 0.4.0.0
readOnlyEnvironment :: Environment ReadWrite -> Environment ReadOnly
readOnlyEnvironment = coerce

-- | Check for stale entries in the reader lock table, and return the number
-- of entries cleared.
clearStaleReaders :: Environment mode -> IO Int
clearStaleReaders (Env env _ _) = mdb_reader_check env

-- | Perform a top-level transaction in either 'ReadWrite' or 'ReadOnly'
-- mode. A transaction may only be 'ReadWrite' if the environment is also
-- 'ReadWrite' (enforced by the 'SubMode' constraint).
--
-- Once completed, the transaction will be committed and the result
-- returned. An exception will cause the transaction to be implicitly aborted.
--
-- Note that there may be several concurrent 'ReadOnly' transactions (limited
-- only by the 'maxReaders' field of the 'Limits' argument given to
-- 'openEnvironment'), but there is at most one active 'ReadWrite'
-- transaction, which is forced to run in a bound thread, and is protected by
-- an internal mutex.
--
-- In general, long-lived transactions should be avoided. 'ReadOnly'
-- transactions prevent reuse of database pages freed by newer 'ReadWrite'
-- transactions, and thus the database can grow quickly. 'ReadWrite'
-- transactions prevent other 'ReadWrite' transactions, since writes are
-- serialized.
transaction :: (Mode tmode, SubMode emode tmode)
            => Environment emode -> Transaction tmode a -> IO a
transaction (Env env _ _) tx@(Txn tf)
  | isReadOnlyTransaction tx = run True
  | otherwise                = runInBoundThread (run False)
  where run readOnly =
          bracketOnError (mdb_txn_begin env Nothing readOnly) mdb_txn_abort $
          \txn -> tf txn >>= \result -> mdb_txn_commit txn >> return result

-- | The exception type thrown when a (top-level) transaction is explicitly
-- aborted
data AbortedTransaction = AbortedTransaction

instance Show AbortedTransaction where
  showsPrec _ AbortedTransaction = showString "aborted transaction"

instance Exception AbortedTransaction

-- | Convenience function for performing a top-level 'ReadWrite' transaction;
-- see 'transaction'
readWriteTransaction :: Environment ReadWrite
                     -> Transaction ReadWrite a -> IO a
readWriteTransaction = transaction

-- | Convenience function for performing a top-level 'ReadOnly' transaction;
-- see 'transaction'
readOnlyTransaction :: Environment mode
                    -> Transaction ReadOnly a -> IO a
readOnlyTransaction = transaction

-- | Nest a transaction within the current 'ReadWrite' transaction.
-- Transactions may be nested to any level.
--
-- If the nested transaction is aborted, 'Nothing' is returned. Otherwise, the
-- nested transaction is committed and the result is returned in a 'Just'
-- value. (The overall effect of a nested transaction depends on whether the
-- parent transaction is ultimately committed.)
--
-- An exception will cause the nested transaction to be implicitly aborted.
nestTransaction :: Transaction ReadWrite a -> Transaction ReadWrite (Maybe a)
nestTransaction (Txn tf) = Txn $ \ptxn ->
  let env = mdb_txn_env ptxn in maybeAborted $
  bracketOnError (mdb_txn_begin env (Just ptxn) False) mdb_txn_abort $
  \ctxn -> tf ctxn >>= \result -> mdb_txn_commit ctxn >> return result

  where maybeAborted :: IO a -> IO (Maybe a)
        maybeAborted io = either
          (\e -> let _ = e :: AbortedTransaction in Nothing) Just <$> try io

-- | Explicitly abort the current transaction, nullifying its effects on the
-- LMDB environment. No further actions will be performed within the
-- transaction.
--
-- In a nested transaction, this causes the child transaction to return
-- 'Nothing' to its parent. In a top-level transaction, this throws an
-- 'AbortedTransaction' exception, which can be caught.
abort :: Transaction mode a
abort = Txn $ \_ -> throwIO AbortedTransaction

-- | Retrieve a database handle from the LMDB environment. The database may be
-- specified by name, or 'Nothing' can be used to specify the main (unnamed)
-- database for the environment. If a named database is specified, it must
-- already exist, or it will be created if the transaction is 'ReadWrite'.
--
-- There are a limited number of named databases you may use in an
-- environment, set by the 'maxDatabases' field of the 'Limits' argument given
-- to 'openEnvironment'. By default ('defaultLimits') this number is zero, so
-- you must specify another limit in order to use any named databases.
--
-- You should not use both named and unnamed databases in the same
-- environment, because the unnamed database is used internally to store
-- entries for each named database.
--
-- You can (and should) retain the database handle returned by this action for
-- use in future transactions.
getDatabase :: Mode mode => Maybe String -> Transaction mode (Database k v)
getDatabase name = tx
  where tx = Txn $ \txn -> Db (mdb_txn_env txn) <$> mdb_dbi_open' txn name flags
        flags = [MDB_CREATE | isReadWriteTransaction tx]

-- | Lookup a key in a database and return the corresponding value, or return
-- 'Nothing' if the key does not exist in the database.
get :: (Serialise k, Serialise v)
    => Database k v -> k -> Transaction mode (Maybe v)
get = Internal.get

-- | Insert the given key/value pair into a database, or delete the key from
-- the database if 'Nothing' is given for the value.
put :: (Serialise k, Serialise v)
    => Database k v -> k -> Maybe v -> Transaction ReadWrite ()
put db key = maybe (void $ Internal.delete db key) (Internal.put db key)

-- | Delete all key/value pairs from a database, leaving the database empty.
clear :: Database k v -> Transaction ReadWrite ()
clear (Db _ dbi) = Txn $ \txn -> mdb_clear' txn dbi

-- | Delay flushing system buffer to disk when commiting transactions that are
-- using the given read-write 'Environment' until after`m` completes.
--
-- This optimization means a system crash can corrupt the database or lose the
-- last transactions if buffers are not yet flushed to disk. The risk is
-- governed by how often the system flushes dirty buffers to disk.
delaySync :: (MonadIO m) => Environment ReadWrite -> m a -> m a
delaySync (Env e c _) m = do
  c' <- liftIO $ takeMVar c
  liftIO $ mdb_env_set_flags e [MDB_NOSYNC]
  liftIO $ putMVar c (c' + 1)
  
  result <- m
  liftIO $ mdb_env_sync_flush e

  c'' <- liftIO $ takeMVar c
  if c'' <= 1
    then do liftIO $ mdb_env_unset_flags e [MDB_NOSYNC]
            liftIO $ putMVar c 0
    else liftIO $ putMVar c (c'' - 1)
  return result

-- | Flush system buffers to disk only once per transaction, omit the metadata flush, for the
-- given read-write 'Environment'.
-- Defer that until the system flushes files to disk, or next non readonly transaction commit
-- or after the mondadic action 'm' completes. This optimization maintains database integrity,
-- but a system crash may undo the last committed transaction. I.e. it preserves the ACI
-- (atomicity, consistency, isolation) but not D (durability) database property.
delayMetaSync :: (MonadIO m) => Environment ReadWrite -> m a -> m a
delayMetaSync (Env e _ c) m = do
  c' <- liftIO $ takeMVar c
  liftIO $ mdb_env_set_flags e [MDB_NOMETASYNC]
  liftIO $ putMVar c (c' + 1)
  
  result <- m
  liftIO $ mdb_env_sync_flush e

  c'' <- liftIO $ takeMVar c
  if c'' <= 1
    then do liftIO $ mdb_env_unset_flags e [MDB_NOMETASYNC]
            liftIO $ putMVar c 0
    else liftIO $ putMVar c (c'' - 1)
  return result

-- | Check if current 'Environment' delays flushing the system buffers to disk or not.
isSyncDelayed :: Environment ReadWrite -> IO Bool
isSyncDelayed e = isEnvFlagSet e MDB_NOSYNC

-- | Check if current 'Environment' delays flushing the meta data to disk or not.
isMetaSyncDelayed :: Environment ReadWrite -> IO Bool
isMetaSyncDelayed e = isEnvFlagSet e MDB_NOMETASYNC

-- | Checks if a flag is set on the current environment
isEnvFlagSet :: Environment mode -> MDB_EnvFlag -> IO Bool
isEnvFlagSet (Env e _ _) f = elem f <$> mdb_env_get_flags e
