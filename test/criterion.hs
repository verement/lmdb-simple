
{-# LANGUAGE DataKinds #-}

module Main where

import Criterion.Main
import Database.LMDB.Simple
import Harness

import Control.Monad (forM, forM_)

main :: IO ()
main = do
  (env, db) <- setup
  defaultMain
    [ bench ("insertion of " ++ elements) $ whnfIO (insertion env db)
    , bench ("retrieval of " ++ elements) $   nfIO (retrieval env db)
    ]

n :: Int
n = 10000

elements :: String
elements = show n ++ " elements"

insertion :: Environment 'ReadWrite -> Database Int String -> IO ()
insertion env db = transaction env $ do
  clear db
  forM_ [1..n] $ \i -> put db i (Just $ show i)

retrieval :: Environment mode -> Database Int String -> IO [Maybe String]
retrieval env db = readOnlyTransaction env $ forM [1..n] $ \i -> get db i
