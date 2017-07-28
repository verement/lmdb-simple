
{-# LANGUAGE DataKinds #-}

module Database.LMDB.Simple.Transaction
  ( Transaction
  , null
  , size
  , lookup
  , findWithDefault
  , member
  , notMember
  , insert
  , insertWith
  , insertWithKey
  , insertLookupWithKey
  , elems
  , keys
  , toList
  , foldr
  , foldl
  , foldrWithKey
  , foldlWithKey
  , foldMapWithKey
  , delete
  , adjust
  , adjustWithKey
  , update
  , updateWithKey
  , updateLookupWithKey
  , alter
  ) where

import Prelude hiding
  ( foldl
  , foldr
  , lookup
  , null
  )

import Control.Monad
  ( void
  )

import Data.Binary
  ( Binary
  )

import Data.Maybe
  ( fromMaybe
  , isJust
  )

import Database.LMDB.Raw
  ( MDB_stat (ms_entries)
  , MDB_val
  , MDB_cursor_op (MDB_SET)
  , MDB_cursor'
  , MDB_WriteFlags
  , mdb_stat'
  , mdb_get'
  , mdb_put'
  , mdb_del'
  , mdb_cursor_get'
  , mdb_cursor_put'
  , mdb_cursor_del'
  )

import Database.LMDB.Simple.Internal
  ( AccessMode (ReadWrite)
  , Transaction (Txn)
  , Database (Db)
  , forEachForward
  , forEachReverse
  , marshalOut
  , marshalIn
  , peekVal
  , withCursor
  , withVal
  , defaultWriteFlags
  , overwriteFlags
  )

import Foreign
  ( alloca
  , nullPtr
  )

lookup :: (Binary k, Binary v) => k -> Database k v -> Transaction rw (Maybe v)
lookup key (Db dbi) = Txn $ \txn -> marshalOut key $ \kval -> do
  mvval <- mdb_get' txn dbi kval
  case mvval of
    Just vval -> marshalIn vval (return . Just)
    Nothing   -> return Nothing

findWithDefault :: (Binary k, Binary v)
                => v -> k -> Database k v -> Transaction rw v
findWithDefault def key db = fromMaybe def <$> lookup key db

null :: Database k v -> Transaction rw Bool
null (Db dbi) = Txn $ \txn -> do
  stat <- mdb_stat' txn dbi
  return (ms_entries stat == 0)

size :: Database k v -> Transaction rw Int
size (Db dbi) = Txn $ \txn -> do
  stat <- mdb_stat' txn dbi
  return (fromIntegral $ ms_entries stat)

member :: Binary k => k -> Database k v -> Transaction rw Bool
member key (Db dbi) = Txn $ \txn ->
  marshalOut key $ \kval -> isJust <$> mdb_get' txn dbi kval

notMember :: Binary k => k -> Database k v -> Transaction rw Bool
notMember key db = not <$> member key db

insert :: (Binary k, Binary v) => k -> v -> Database k v
       -> Transaction 'ReadWrite ()
insert key value (Db dbi) = Txn $ \txn ->
  marshalOut key $ \kval -> marshalOut value $ \vval ->
  void $ mdb_put' defaultWriteFlags txn dbi kval vval

insertWith :: (Binary k, Binary v) => (v -> v -> v) -> k -> v -> Database k v
           -> Transaction 'ReadWrite ()
insertWith f = insertWithKey (const f)

insertWithKey :: (Binary k, Binary v) => (k -> v -> v -> v) -> k -> v
              -> Database k v -> Transaction 'ReadWrite ()
insertWithKey f key value = void . insertLookupWithKey f key value

insertLookupWithKey :: (Binary k, Binary v) => (k -> v -> v -> v) -> k -> v
                    -> Database k v -> Transaction 'ReadWrite (Maybe v)
insertLookupWithKey f key value (Db dbi) = Txn $ \txn ->
  withCursor txn dbi $ \cursor -> marshalOut key $ \kval ->
  withVal kval $ \kptr -> alloca $ \vptr -> do
    found <- mdb_cursor_get' MDB_SET cursor kptr vptr
    if found
      then do oldValue <- peekVal vptr
              cursorPut cursor overwriteFlags kval (f key value oldValue)
              return (Just oldValue)
      else do cursorPut cursor defaultWriteFlags kval value
              return  Nothing

  where cursorPut :: Binary v => MDB_cursor' -> MDB_WriteFlags -> MDB_val -> v
                  -> IO Bool
        cursorPut cursor writeFlags kval value = marshalOut value $ \vval ->
          mdb_cursor_put' writeFlags cursor kval vval

elems :: Binary v => Database k v -> Transaction rw [v]
elems = foldr (:) []

keys :: Binary k => Database k v -> Transaction rw [k]
keys (Db dbi) = Txn $ \txn ->
  alloca $ \kptr ->
  forEachForward txn dbi kptr nullPtr [] $ \rest ->
  (:) <$> peekVal kptr <*> rest

toList :: (Binary k, Binary v) => Database k v -> Transaction rw [(k, v)]
toList = foldrWithKey (\k v -> ((k, v) :)) []

foldr :: Binary v => (v -> b -> b) -> b -> Database k v -> Transaction rw b
foldr f z (Db dbi) = Txn $ \txn ->
  alloca $ \vptr ->
  forEachForward txn dbi nullPtr vptr z $ \rest ->
  f <$> peekVal vptr <*> rest

foldrWithKey :: (Binary k, Binary v)
             => (k -> v -> b -> b) -> b -> Database k v -> Transaction rw b
foldrWithKey f z (Db dbi) = Txn $ \txn ->
  alloca $ \kptr ->
  alloca $ \vptr ->
  forEachForward txn dbi kptr vptr z $ \rest ->
  f <$> peekVal kptr <*> peekVal vptr <*> rest

foldl :: Binary v => (a -> v -> a) -> a -> Database k v -> Transaction rw a
foldl f z (Db dbi) = Txn $ \txn ->
  alloca $ \vptr ->
  forEachReverse txn dbi nullPtr vptr z $ \rest ->
  flip f <$> peekVal vptr <*> rest

foldlWithKey :: (Binary k, Binary v)
             => (a -> k -> v -> a) -> a -> Database k v -> Transaction rw a
foldlWithKey f z (Db dbi) = Txn $ \txn ->
  alloca $ \kptr ->
  alloca $ \vptr ->
  forEachReverse txn dbi kptr vptr z $ \rest ->
  (\k v a -> f a k v) <$> peekVal kptr <*> peekVal vptr <*> rest

foldMapWithKey :: (Monoid m, Binary k, Binary v)
               => (k -> v -> m) -> Database k v -> Transaction rw m
foldMapWithKey f = foldrWithKey (\k v a -> f k v `mappend` a) mempty

delete :: Binary k => k -> Database k v -> Transaction 'ReadWrite Bool
delete key (Db dbi) = Txn $ \txn ->
  marshalOut key $ \kval -> mdb_del' txn dbi kval Nothing

adjust :: (Binary k, Binary v) => (v -> v) -> k
       -> Database k v -> Transaction 'ReadWrite Bool
adjust f = adjustWithKey (const f)

adjustWithKey :: (Binary k, Binary v) => (k -> v -> v) -> k
              -> Database k v -> Transaction 'ReadWrite Bool
adjustWithKey f = updateWithKey (\k v -> Just $ f k v)

update :: (Binary k, Binary v) => (v -> Maybe v) -> k
       -> Database k v -> Transaction 'ReadWrite Bool
update f = updateWithKey (const f)

updateWithKey :: (Binary k, Binary v) => (k -> v -> Maybe v) -> k
              -> Database k v -> Transaction 'ReadWrite Bool
updateWithKey f key db = isJust <$> updateLookupWithKey f key db

updateLookupWithKey :: (Binary k, Binary v) => (k -> v -> Maybe v) -> k
                    -> Database k v -> Transaction 'ReadWrite (Maybe v)
updateLookupWithKey f = alterWithKey (maybe Nothing . f)

alter :: (Binary k, Binary v) => (Maybe v -> Maybe v) -> k
      -> Database k v -> Transaction 'ReadWrite (Maybe v)
alter f = alterWithKey (const f)

alterWithKey :: (Binary k, Binary v) => (k -> Maybe v -> Maybe v) -> k
             -> Database k v -> Transaction 'ReadWrite (Maybe v)
alterWithKey f key (Db dbi) = Txn $ \txn ->
  withCursor txn dbi $ \cursor -> marshalOut key $ \kval ->
  withVal kval $ \kptr -> alloca $ \vptr -> do
    found <- mdb_cursor_get' MDB_SET cursor kptr vptr
    if found
      then peekVal vptr >>= \oldValue -> do
        let old = Just oldValue
        case f key old of
          new@(Just newValue) -> marshalOut newValue $ \vval ->
            mdb_cursor_put' overwriteFlags cursor kval vval >>
            return new
          Nothing -> mdb_cursor_del' defaultWriteFlags cursor >>
            return old
      else case f key Nothing of
             new@(Just newValue) -> marshalOut newValue $ \vval ->
               mdb_cursor_put' defaultWriteFlags cursor kval vval >>
               return new
             Nothing -> return Nothing
