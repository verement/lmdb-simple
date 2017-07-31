
module Main where

import Database.LMDB.Simple
import Control.Monad (forM_)

main :: IO ()
main = do
  env <- openEnvironment "test/env" defaultLimits
  db <- readOnlyTransaction env $ getDatabase Nothing :: IO (Database String Int)

  transaction env $ do
    clear db
    forM_ [("one",1),("two",2),("three",3)] $ \(k,v) -> put db k (Just v)

  print =<< readOnlyTransaction env (get db "two")   -- Just 2
  print =<< readOnlyTransaction env (get db "nine")  -- Nothing
