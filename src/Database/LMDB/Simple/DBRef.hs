
-- | This module provides a mutable variable 'DBRef' that is similar in
-- concept to 'Data.IORef.IORef' except that it is tied to a particular key
-- that persists in an LMDB database.

module Database.LMDB.Simple.DBRef
  ( DBRef
  , newDBRef
  , readDBRef
  , writeDBRef
  , modifyDBRef_
  , modifyDBRef
  ) where

import Control.Monad
  ( (>=>)
  , void
  )

import Control.Monad.IO.Class
  ( MonadIO (liftIO)
  )

import Data.ByteString
  ( ByteString
  )

import Data.ByteString.Lazy
  ( toStrict
  )

import Data.ByteString.Unsafe
  ( unsafeUseAsCStringLen
  )

import Database.LMDB.Raw
  ( MDB_dbi'
  , MDB_val (..)
  , mdb_get'
  , mdb_put'
  , mdb_del'
  )

import Database.LMDB.Simple
  ( transaction
  )

import Database.LMDB.Simple.Internal
  ( Environment (..)
  , Transaction (..)
  , Database (..)
  , ReadWrite
  , ReadOnly
  , Serialise
  , serialise
  , marshalIn
  , marshalOut
  , defaultWriteFlags
  )

import Foreign
  ( castPtr
  )

-- | A 'DBRef' is a reference to a particular key within an LMDB database. It
-- may be empty ('Nothing') if the key does not currently exist in the
-- database, or it may contain a 'Just' value corresponding to the key.
--
-- A 'DBRef' may be 'ReadWrite' or 'ReadOnly', depending on the environment
-- within which it is created. Note that 'ReadOnly' does not imply that the
-- contained value will not change, since the LMDB database could be modified
-- externally.
data DBRef mode a = Ref (Environment mode) MDB_dbi' ByteString

-- | Create a new 'DBRef' for the given key and database within the given
-- environment.
newDBRef :: Serialise k
         => Environment mode -> Database k a -> k -> IO (DBRef mode a)
newDBRef env (Db _ dbi) = return . Ref env dbi . toStrict . serialise

withVal :: ByteString -> (MDB_val -> IO a) -> IO a
withVal bs f = unsafeUseAsCStringLen bs $ \(ptr, len) ->
  f $ MDB_val (fromIntegral len) (castPtr ptr)

-- | Read the current value of a 'DBRef'.
readDBRef :: Serialise a => DBRef mode a -> IO (Maybe a)
readDBRef ref@(Ref env dbi key) = transaction env (tx ref)

  where tx :: Serialise a => DBRef mode a -> Transaction ReadOnly (Maybe a)
        tx _ = Txn $ \txn -> withVal key $ mdb_get' txn dbi >=>
          maybe (return Nothing) (liftIO . fmap Just . marshalIn)

-- | Write a new value into a 'DBRef'.
writeDBRef :: Serialise a => DBRef ReadWrite a -> Maybe a -> IO ()
writeDBRef (Ref env dbi key) = transaction env . maybe delKey putKey

  where delKey :: Transaction ReadWrite ()
        delKey = Txn $ \txn -> withVal key $ \kval ->
          void $ mdb_del' txn dbi kval Nothing

        putKey :: Serialise a => a -> Transaction ReadWrite ()
        putKey value = Txn $ \txn -> withVal key $ \kval ->
          marshalOut value $ \vval ->  -- FIXME: use mdb_reserve'
          void $ mdb_put' defaultWriteFlags txn dbi kval vval

-- | Atomically mutate the contents of a 'DBRef'.
modifyDBRef_ :: Serialise a => DBRef ReadWrite a -> (Maybe a -> Maybe a) -> IO ()
modifyDBRef_ ref f = modifyDBRef ref $ \x -> (f x, ())

-- | Atomically mutate the contents of a 'DBRef' and return a value.
modifyDBRef :: Serialise a
            => DBRef ReadWrite a -> (Maybe a -> (Maybe a, b)) -> IO b
modifyDBRef (Ref env dbi key) = transaction env . tx

  where tx :: Serialise a => (Maybe a -> (Maybe a, b)) -> Transaction ReadWrite b
        tx f = Txn $ \txn -> withVal key $ \kval -> mdb_get' txn dbi kval >>=
          maybe (return Nothing) (fmap Just . marshalIn) >>= \x ->
          let (x', r) = f x in maybe (mdb_del' txn dbi kval Nothing)
            (flip marshalOut $ mdb_put' defaultWriteFlags txn dbi kval) x' >>
          return r
