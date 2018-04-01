
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
  ( void
  )

import Data.ByteString
  ( ByteString
  )

import Database.LMDB.Raw
  ( MDB_dbi'
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
  , serialiseBS
  , getBS
  , putBS
  , deleteBS
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
newDBRef env (Db _ dbi) = return . Ref env dbi . serialiseBS

-- | Read the current value of a 'DBRef'.
readDBRef :: Serialise a => DBRef mode a -> IO (Maybe a)
readDBRef ref@(Ref env dbi key) = transaction env (tx env ref)

  where tx :: Serialise a
           => Environment mode -> DBRef mode a -> Transaction ReadOnly (Maybe a)
        tx (Env env _ _) _ = getBS (Db env dbi) key

-- | Write a new value into a 'DBRef'.
writeDBRef :: Serialise a => DBRef ReadWrite a -> Maybe a -> IO ()
writeDBRef (Ref env dbi key) = transaction env . maybe (delKey env) (putKey env)

  where delKey :: Environment ReadWrite -> Transaction ReadWrite ()
        delKey (Env env _ _) = void $ deleteBS (Db env dbi) key

        putKey :: Serialise a
               => Environment ReadWrite -> a -> Transaction ReadWrite ()
        putKey (Env env _ _) = putBS (Db env dbi) key

-- | Atomically mutate the contents of a 'DBRef'.
modifyDBRef_ :: Serialise a => DBRef ReadWrite a -> (Maybe a -> Maybe a) -> IO ()
modifyDBRef_ ref f = modifyDBRef ref $ \x -> (f x, ())

-- | Atomically mutate the contents of a 'DBRef' and return a value.
modifyDBRef :: Serialise a
            => DBRef ReadWrite a -> (Maybe a -> (Maybe a, b)) -> IO b
modifyDBRef (Ref env dbi key) = transaction env . tx env

  where tx :: Serialise a
           => Environment mode -> (Maybe a -> (Maybe a, b))
           -> Transaction ReadWrite b
        tx (Env env _ _) f = let db = Db env dbi in
          getBS db key >>= \x -> let (x', r) = f x in
          maybe (void $ deleteBS db key) (putBS db key) x' >>
          return r
