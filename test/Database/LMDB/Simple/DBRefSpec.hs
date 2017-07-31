
module Database.LMDB.Simple.DBRefSpec
  ( spec
  ) where

import Database.LMDB.Simple.DBRef
import Harness
import Test.Hspec

spec :: Spec
spec = beforeAll (setup >>= \(env, db) -> newDBRef env db 0) $ do

  it "starts empty" $ \ref ->
    readDBRef ref
    `shouldReturn` Nothing

  it "reads what is written" $ \ref ->
    (writeDBRef ref (Just "foo") >> readDBRef ref)
    `shouldReturn` Just "foo"

  it "reads what is modified" $ \ref ->
    (modifyDBRef_ ref (fmap (++ "bar")) >> readDBRef ref)
    `shouldReturn` Just "foobar"

  it "can be made empty again" $ \ref ->
    (writeDBRef ref Nothing >> readDBRef ref)
    `shouldReturn` Nothing
