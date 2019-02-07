
module Harness
  ( setup
  , tearDown
  ) where

import Database.LMDB.Simple

setup :: IO (Environment ReadWrite, Database Int String)
setup = do
  env <- openEnvironment "test/env" defaultLimits
         { mapSize      = 1024 * 1024 * 1024
         , maxDatabases = 4
         }
  db <- transaction env $ do
    db <- getDatabase Nothing
    clear db
    return db

  return (env, db)

tearDown :: (Environment ReadWrite, Database Int String) -> IO ()
tearDown (env, _db) = closeEnvironment env
