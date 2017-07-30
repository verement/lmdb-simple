
{-# LANGUAGE DataKinds #-}

-- | This module exports many functions for querying and modifying LMDB
-- databases using common idioms (albeit in monadic form).

module Database.LMDB.Simple.Extra
  ( -- * Query
    null
  , size
  , member
  , notMember
  , lookup
  , findWithDefault

    -- * Modification

    -- ** Insertion
  , insert
  , insertWith
  , insertWithKey
  , insertLookupWithKey

    -- ** Delete/Update
  , delete
  , adjust
  , adjustWithKey
  , update
  , updateWithKey
  , updateLookupWithKey
  , alter

    -- * Folds
  , foldr
  , foldl
  , foldrWithKey
  , foldlWithKey
  , foldDatabaseWithKey

    -- * Conversion
  , elems
  , keys
  , toList
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
  , peekVal
  , withCursor
  , defaultWriteFlags
  , overwriteFlags
  )
import qualified Database.LMDB.Simple.Internal as Internal

import Foreign
  ( alloca
  , nullPtr
  , with
  )

-- | Lookup the value at a key in the database.
--
-- The function will return the corresponding value as @('Just' value)@, or
-- 'Nothing' if the key isn't in the database.
lookup :: (Binary k, Binary v) => k -> Database k v -> Transaction mode (Maybe v)
lookup = flip Internal.get

-- | The expression @('findWithDefault' def k db)@ returns the value at key
-- @k@ or returns default value @def@ when the key is not in the database.
findWithDefault :: (Binary k, Binary v)
                => v -> k -> Database k v -> Transaction mode v
findWithDefault def key db = fromMaybe def <$> lookup key db

-- | Is the database empty?
null :: Database k v -> Transaction mode Bool
null (Db _ dbi) = Txn $ \txn -> do
  stat <- mdb_stat' txn dbi
  return (ms_entries stat == 0)

-- | The number of entries in the database.
size :: Database k v -> Transaction mode Int
size (Db _ dbi) = Txn $ \txn -> do
  stat <- mdb_stat' txn dbi
  return (fromIntegral $ ms_entries stat)

-- | Is the key a member of the database? See also 'notMember'.
member :: Binary k => k -> Database k v -> Transaction mode Bool
member key db = isJust <$> Internal.get' db key

-- | Is the key not a member of the database? See also 'member'.
notMember :: Binary k => k -> Database k v -> Transaction mode Bool
notMember key db = not <$> member key db

-- | Insert a new key and value in the database. If the key is already present
-- in the database, the associated value is replaced with the supplied
-- value. 'insert' is equivalent to @'insertWith' 'const'@.
insert :: (Binary k, Binary v) => k -> v -> Database k v
       -> Transaction 'ReadWrite ()
insert key value db = Internal.put db key value

-- | Insert with a function, combining new value and old value. @'insertWith'
-- f key value db@ will insert the pair @(key, value)@ into @db@ if key does
-- not exist in the database. If the key does exist, the function will insert
-- the pair @(key, f new_value old_value)@.
insertWith :: (Binary k, Binary v) => (v -> v -> v) -> k -> v -> Database k v
           -> Transaction 'ReadWrite ()
insertWith f = insertWithKey (const f)

-- | Insert with a function, combining key, new value and old
-- value. @'insertWithKey' f key value db@ will insert the pair @(key, value)@
-- into @db@ if key does not exist in the database. If the key does exist, the
-- function will insert the pair @(key, f key new_value old_value)@. Note that
-- the key passed to @f@ is the same key passed to 'insertWithKey'.
insertWithKey :: (Binary k, Binary v) => (k -> v -> v -> v) -> k -> v
              -> Database k v -> Transaction 'ReadWrite ()
insertWithKey f key value = void . insertLookupWithKey f key value

-- | Combines insert operation with old value retrieval. The monadic action
-- @('insertLookupWithKey' f k x db)@ has the same effect as @('insertWithKey'
-- f k x db)@ but returns the same value as @('lookup' k db)@.
insertLookupWithKey :: (Binary k, Binary v) => (k -> v -> v -> v) -> k -> v
                    -> Database k v -> Transaction 'ReadWrite (Maybe v)
insertLookupWithKey f key value (Db _ dbi) = Txn $ \txn ->
  withCursor txn dbi $ \cursor -> marshalOut key $ \kval ->
  with kval $ \kptr -> alloca $ \vptr -> do
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

-- | Return all elements of the database in the order of their keys.
elems :: Binary v => Database k v -> Transaction mode [v]
elems = foldr (:) []

-- | Return all keys of the database in the order they are stored on disk.
keys :: Binary k => Database k v -> Transaction mode [k]
keys (Db _ dbi) = Txn $ \txn ->
  alloca $ \kptr ->
  forEachForward txn dbi kptr nullPtr [] $ \rest ->
  (:) <$> peekVal kptr <*> rest

-- | Convert the database to a list of key/value pairs. Note that this will
-- make a copy of the entire database in memory.
toList :: (Binary k, Binary v) => Database k v -> Transaction mode [(k, v)]
toList = foldrWithKey (\k v -> ((k, v) :)) []

-- | Fold the values in the database using the given right-associative binary
-- operator.
foldr :: Binary v => (v -> b -> b) -> b -> Database k v -> Transaction mode b
foldr f z (Db _ dbi) = Txn $ \txn ->
  alloca $ \vptr ->
  forEachForward txn dbi nullPtr vptr z $ \rest ->
  f <$> peekVal vptr <*> rest

-- | Fold the keys and values in the database using the given
-- right-associative binary operator.
foldrWithKey :: (Binary k, Binary v)
             => (k -> v -> b -> b) -> b -> Database k v -> Transaction mode b
foldrWithKey f z (Db _ dbi) = Txn $ \txn ->
  alloca $ \kptr ->
  alloca $ \vptr ->
  forEachForward txn dbi kptr vptr z $ \rest ->
  f <$> peekVal kptr <*> peekVal vptr <*> rest

-- | Fold the values in the database using the given left-associative binary
-- operator.
foldl :: Binary v => (a -> v -> a) -> a -> Database k v -> Transaction mode a
foldl f z (Db _ dbi) = Txn $ \txn ->
  alloca $ \vptr ->
  forEachReverse txn dbi nullPtr vptr z $ \rest ->
  flip f <$> peekVal vptr <*> rest

-- | Fold the keys and values in the database using the given left-associative
-- binary operator.
foldlWithKey :: (Binary k, Binary v)
             => (a -> k -> v -> a) -> a -> Database k v -> Transaction mode a
foldlWithKey f z (Db _ dbi) = Txn $ \txn ->
  alloca $ \kptr ->
  alloca $ \vptr ->
  forEachReverse txn dbi kptr vptr z $ \rest ->
  (\k v a -> f a k v) <$> peekVal kptr <*> peekVal vptr <*> rest

-- | Fold the keys and values in the database using the given monoid.
foldDatabaseWithKey :: (Monoid m, Binary k, Binary v)
                    => (k -> v -> m) -> Database k v -> Transaction mode m
foldDatabaseWithKey f = foldrWithKey (\k v a -> f k v `mappend` a) mempty

-- | Delete a key and its value from the database. If the key was not present
-- in the database, this returns 'False'; otherwise it returns 'True'.
delete :: Binary k => k -> Database k v -> Transaction 'ReadWrite Bool
delete = flip Internal.delete

-- | Update a value at a specific key with the result of the provided
-- function. When the key is not a member of the database, this returns
-- 'False'; otherwise it returns 'True'.
adjust :: (Binary k, Binary v) => (v -> v) -> k
       -> Database k v -> Transaction 'ReadWrite Bool
adjust f = adjustWithKey (const f)

-- | Adjust a value at a specific key. When the key is not a member of the
-- database, this returns 'False'; otherwise it returns 'True'.
adjustWithKey :: (Binary k, Binary v) => (k -> v -> v) -> k
              -> Database k v -> Transaction 'ReadWrite Bool
adjustWithKey f = updateWithKey (\k v -> Just $ f k v)

-- | The monadic action @('update' f k db)@ updates the value @x@ at @k@ (if
-- it is in the database). If @(f x)@ is 'Nothing', the element is deleted. If
-- it is @('Just' y)@, the key @k@ is bound to the new value @y@.
update :: (Binary k, Binary v) => (v -> Maybe v) -> k
       -> Database k v -> Transaction 'ReadWrite Bool
update f = updateWithKey (const f)

-- | The monadic action @('updateWithKey' f k db)@ updates the value @x@ at
-- @k@ (if it is in the database). If @(f k x)@ is 'Nothing', the element is
-- deleted. If it is @('Just' y)@, the key @k@ is bound to the new value @y@.
updateWithKey :: (Binary k, Binary v) => (k -> v -> Maybe v) -> k
              -> Database k v -> Transaction 'ReadWrite Bool
updateWithKey f key db = isJust <$> updateLookupWithKey f key db

-- | Lookup and update. See also 'updateWithKey'. The function returns changed
-- value, if it is updated. Returns the original key value if the database
-- entry is deleted.
updateLookupWithKey :: (Binary k, Binary v) => (k -> v -> Maybe v) -> k
                    -> Database k v -> Transaction 'ReadWrite (Maybe v)
updateLookupWithKey f = alterWithKey (maybe Nothing . f)

-- | The monadic action @('alter' f k db)@ alters the value @x@ at @k@, or
-- absence thereof. 'alter' can be used to insert, delete, or update a value
-- in a database.
alter :: (Binary k, Binary v) => (Maybe v -> Maybe v) -> k
      -> Database k v -> Transaction 'ReadWrite ()
alter f key db = void $ alterWithKey (const f) key db

alterWithKey :: (Binary k, Binary v) => (k -> Maybe v -> Maybe v) -> k
             -> Database k v -> Transaction 'ReadWrite (Maybe v)
alterWithKey f key (Db _ dbi) = Txn $ \txn ->
  withCursor txn dbi $ \cursor -> marshalOut key $ \kval ->
  with kval $ \kptr -> alloca $ \vptr -> do
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
