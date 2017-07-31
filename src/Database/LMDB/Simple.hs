
{-# LANGUAGE ConstraintKinds #-}

{-|
Module      : Database.LMDB.Simple
Description : Simple Haskell API for LMDB
Copyright   : © 2017 Robert Leslie
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

Additional functions for querying and modifying LMDB databases are provided in
"Database.LMDB.Simple.Extra". For an option to access LMDB databases from pure
code, see "Database.LMDB.Simple.View".
-}

module Database.LMDB.Simple
  ( -- * Environments
    Environment
  , Limits (..)
  , defaultLimits
  , openEnvironment
  , openReadWriteEnvironment
  , openReadOnlyEnvironment
  , clearStaleReaders

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

import Prelude hiding
  ( lookup
  )

import Control.Concurrent
  ( runInBoundThread
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

import Data.Binary
  ( Binary
  )

import Database.LMDB.Raw
  ( LMDB_Error (LMDB_Error, e_code)
  , MDB_EnvFlag (MDB_NOSUBDIR, MDB_RDONLY)
  , MDB_DbFlag (MDB_CREATE)
  , mdb_env_create
  , mdb_env_open
  , mdb_env_set_mapsize
  , mdb_env_set_maxdbs
  , mdb_env_set_maxreaders
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
  , isReadOnlyEnvironment
  , isReadOnlyTransaction
  , isReadWriteTransaction
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

  let environ = Env env :: Mode mode => Environment mode
      flags   = [MDB_RDONLY | isReadOnlyEnvironment environ]

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

-- | Check for stale entries in the reader lock table, and return the number
-- of entries cleared.
clearStaleReaders :: Environment mode -> IO Int
clearStaleReaders (Env env) = mdb_reader_check env

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
transaction (Env env) tx@(Txn tf)
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
nestTransaction tx@(Txn tf) = Txn $ run (isReadOnlyTransaction tx)
  where run ro ptxn = let env = mdb_txn_env ptxn in maybeAborted $
          bracketOnError (mdb_txn_begin env (Just ptxn) ro) mdb_txn_abort $
          \ctxn -> tf ctxn >>= \result -> mdb_txn_commit ctxn >> return result

        maybeAborted :: IO a -> IO (Maybe a)
        maybeAborted io = either
          (\e -> let _ = e :: AbortedTransaction in Nothing) Just <$> try io

-- | Explicitly abort the current transaction, nullifying its effects on the
-- LMDB environment. No further actions will be performed within the current
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
get :: (Binary k, Binary v) => Database k v -> k -> Transaction mode (Maybe v)
get = Internal.get

-- | Insert the given key/value pair into a database, or delete the key from
-- the database if 'Nothing' is given for the value.
put :: (Binary k, Binary v)
    => Database k v -> k -> Maybe v -> Transaction ReadWrite ()
put db key = maybe (void $ Internal.delete db key) (Internal.put db key)

-- | Delete all key/value pairs from a database, leaving the database empty.
clear :: Database k v -> Transaction ReadWrite ()
clear (Db _ dbi) = Txn $ \txn -> mdb_clear' txn dbi
