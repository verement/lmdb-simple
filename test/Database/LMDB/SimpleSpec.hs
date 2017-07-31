
module Database.LMDB.SimpleSpec
  ( spec
  ) where

import Control.Monad (forM, forM_)
import Database.LMDB.Simple
import Database.LMDB.Simple.Extra
import Harness
import Test.Hspec

spec :: Spec
spec = beforeAll setup $ do

  describe "basic operations" $ do
    it "inserts and counts entries" $ \(env, db) ->
      transaction env
      ( do forM_ [1..100] $ \i -> put db i (Just $ show i)
           size db
      ) `shouldReturn` 100

    it "retrieves entries" $ \(env, db) ->
      readOnlyTransaction env (forM [1..100] $ \i -> get db i)
      `shouldReturn` map (Just . show) [1 :: Int .. 100]

    it "deletes entries" $ \(env, db) ->
      transaction env (put db 50 Nothing >> (,) <$> get db 50 <*> size db)
      `shouldReturn` (Nothing, 99)

  describe "transactions" $ do
    it "aborts" $ \(env, db) ->
      transaction env (put db 0 (Just "zero") >> abort)
      `shouldThrow` (const True :: Selector AbortedTransaction)

    it "rolls back" $ \(env, db) ->
      readOnlyTransaction env (get db 0)
      `shouldReturn` Nothing

    it "aborts nested transactions" $ \(env, db) ->
      transaction env
      ( do put db 1 (Just "one")
           nestTransaction $ put db 2 (Just "two") >> abort
      ) `shouldReturn` (Nothing :: Maybe ())

    it "rolls back nested transactions" $ \(env, db) ->
      readOnlyTransaction env ((,) <$> get db 1 <*> get db 2)
      `shouldReturn` (Just "one", Just "2")

    it "commits nested transactions" $ \(env, db) ->
      transaction env
      ( do nestTransaction (put db 3 $ Just "three")
           get db 3
      ) `shouldReturn` Just "three"
