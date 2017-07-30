
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}

module Database.LMDB.Simple.Internal
  ( AccessMode (..)
  , Mode
  , SubMode
  , Environment (..)
  , Transaction (..)
  , Database (..)
  , isReadOnlyEnvironment
  , isReadOnlyTransaction
  , isReadWriteTransaction
  , marshalOut
  , marshalIn
  , peekVal
  , forEachForward
  , forEachReverse
  , withCursor
  , withVal
  , defaultWriteFlags
  , overwriteFlags
  , get
  , get'
  , put
  , delete
  ) where

import Control.Exception
  ( bracket
  )

import Control.Monad
  ( (>=>)
  , void
  )

import Control.Monad.IO.Class
  ( MonadIO (liftIO)
  )

import Data.Binary
  ( Binary
  , encode
  , decode
  )

import Data.ByteString
  ( useAsCStringLen
  , packCStringLen
  )

import Data.ByteString.Lazy
  ( toStrict
  , fromStrict
  )

import Database.LMDB.Raw
  ( MDB_env
  , MDB_txn
  , MDB_dbi'
  , MDB_val (MDB_val)
  , MDB_cursor'
  , MDB_cursor_op (MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_PREV)
  , MDB_WriteFlag (MDB_CURRENT)
  , MDB_WriteFlags
  , mdb_cursor_open'
  , mdb_cursor_close'
  , mdb_cursor_get'
  , mdb_get'
  , mdb_put'
  , mdb_del'
  , compileWriteFlags
  )

import Foreign
  ( Ptr
  , alloca
  , castPtr
  , peek
  , poke
  )

import GHC.Exts (Constraint)

data AccessMode = ReadWrite | ReadOnly

class Mode (mode :: AccessMode) where
  isReadOnlyMode :: proxy mode -> Bool

instance Mode 'ReadWrite where
  isReadOnlyMode _ = False

instance Mode 'ReadOnly where
  isReadOnlyMode _ = True

type family SubMode (a :: AccessMode)
                    (b :: AccessMode) :: Constraint where
  SubMode a 'ReadWrite = a ~ 'ReadWrite
  SubMode a 'ReadOnly  = ()

-- | An LMDB environment is a directory or file on disk that contains one or
-- more databases, and has an associated (reader) lock table.
newtype Environment (mode :: AccessMode) = Env MDB_env

isReadOnlyEnvironment :: Mode mode => Environment mode -> Bool
isReadOnlyEnvironment = isReadOnlyMode

-- | An LMDB transaction is an atomic unit for reading and/or changing one or
-- more LMDB databases within an environment, during which the transaction has
-- a consistent view of the databases and is unaffected by any other
-- transaction. The effects of a transaction can either be committed to the
-- LMDB environment atomically, or they can be rolled back with no observable
-- effect on the environment if the transaction is aborted.
--
-- Transactions may be 'ReadWrite' or 'ReadOnly', however LMDB enforces a
-- strict single-writer policy so only one top-level 'ReadWrite' transaction
-- may be active at any time.
--
-- This API models transactions using a 'Transaction' monad. This monad has a
-- 'MonadIO' instance so it is possible to perform arbitrary I/O within a
-- transaction using 'liftIO'. However, such 'IO' actions are not atomic and
-- cannot be rolled back if the transaction is aborted, so use with care.
newtype Transaction (mode :: AccessMode) a = Txn (MDB_txn -> IO a)

isReadOnlyTransaction :: Mode mode => Transaction mode a -> Bool
isReadOnlyTransaction = isReadOnlyMode . mode
  where mode :: Transaction mode a -> proxy mode
        mode = undefined

isReadWriteTransaction :: Mode mode => Transaction mode a -> Bool
isReadWriteTransaction = not . isReadOnlyTransaction

instance Functor (Transaction mode) where
  fmap f (Txn tf) = Txn $ \txn -> fmap f (tf txn)

instance Applicative (Transaction mode) where
  pure x = Txn $ \_ -> pure x
  Txn tff <*> Txn tf = Txn $ \txn -> tff txn <*> tf txn

instance Monad (Transaction mode) where
  Txn tf >>= f = Txn $ \txn -> tf txn >>= \r -> let Txn tf' = f r in tf' txn

instance MonadIO (Transaction mode) where
  liftIO io = Txn $ const io

-- | A database maps arbitrary keys to values. This API uses the 'Binary'
-- class to serialize keys and values for LMDB to store on disk.
data Database k v = Db MDB_env MDB_dbi'

marshalIn :: Binary v => MDB_val -> IO v
marshalIn (MDB_val len ptr) =
  decode . fromStrict <$> packCStringLen (castPtr ptr, fromIntegral len)

marshalOut :: Binary v => v -> (MDB_val -> IO a) -> IO a
marshalOut value f = useAsCStringLen (toStrict $ encode value) $ \(ptr, len) ->
  f $ MDB_val (fromIntegral len) (castPtr ptr)

peekVal :: Binary v => Ptr MDB_val -> IO v
peekVal = peek >=> marshalIn

forEach :: MDB_cursor_op -> MDB_cursor_op
        -> MDB_txn -> MDB_dbi' -> Ptr MDB_val -> Ptr MDB_val
        -> a -> (IO a -> IO a) -> IO a
forEach first next txn dbi kptr vptr acc f =
  withCursor txn dbi $ cursorGet first acc

  where cursorGet op acc cursor = do
          found <- mdb_cursor_get' op cursor kptr vptr
          if found
            then f (cursorGet next acc cursor)
            else pure acc

forEachForward, forEachReverse :: MDB_txn -> MDB_dbi'
                               -> Ptr MDB_val -> Ptr MDB_val
                               -> a -> (IO a -> IO a) -> IO a
forEachForward = forEach MDB_FIRST MDB_NEXT
forEachReverse = forEach MDB_LAST  MDB_PREV

withCursor :: MDB_txn -> MDB_dbi' -> (MDB_cursor' -> IO a) -> IO a
withCursor txn dbi = bracket (mdb_cursor_open' txn dbi) mdb_cursor_close'

withVal :: MDB_val -> (Ptr MDB_val -> IO a) -> IO a
withVal val f = alloca $ \ptr -> poke ptr val >> f ptr

defaultWriteFlags, overwriteFlags :: MDB_WriteFlags
defaultWriteFlags = compileWriteFlags []
overwriteFlags    = compileWriteFlags [MDB_CURRENT]

get :: (Binary k, Binary v) => Database k v -> k -> Transaction mode (Maybe v)
get db key = get' db key >>=
  maybe (return Nothing) (liftIO . fmap Just . marshalIn)

get' :: Binary k => Database k v -> k -> Transaction mode (Maybe MDB_val)
get' (Db _ dbi) key = Txn $ \txn -> marshalOut key $ mdb_get' txn dbi

put :: (Binary k, Binary v)
    => Database k v -> k -> v -> Transaction 'ReadWrite ()
put (Db _ dbi) key value = Txn $ \txn ->
  marshalOut key $ \kval -> marshalOut value $ \vval ->
  void $ mdb_put' defaultWriteFlags txn dbi kval vval

delete :: Binary k => Database k v -> k -> Transaction 'ReadWrite Bool
delete (Db _ dbi) key = Txn $ \txn ->
  marshalOut key $ \kval -> mdb_del' txn dbi kval Nothing
