# Copyright (C) 2009-2014 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require_relative 'spec_helper'
require 'merge_1'

describe "#fetch_ary" do

  it("should fetch via array of keys") {
    hash = {a: 1, b: {c: 2, d: {e: 3, f: 4}, g: 5}, h: 6}
    expect(hash.fetch_ary([:b, :d, :e])).to eq(3)
    expect(hash.fetch_ary([:b, :d])).to eq({e: 3, f: 4})
    expect(hash.fetch_ary([:b])).to eq({c: 2, d: {e: 3, f: 4}, g: 5})
    expect(hash.fetch_ary([:a])).to eq(1)
    expect(hash.fetch_ary([])).to eq(nil)
    expect(hash.fetch_ary([:i])).to eq(nil)
    expect(hash.fetch_ary([:i], [])).to eq([])
  }

  it("should fetch via symbol") {
    hash = {a: 1, b: {c: 2, d: {e: 3, f: 4}, g: 5}, h: 6}
    expect(hash.fetch(:a)).to eq(1)
    expect(hash.fetch(:b)).to eq({c: 2, d: {e: 3, f: 4}, g: 5})
    expect(hash.fetch(:h)).to eq(6)
  }

  it("should fetch via string") {
    hash = {"a" => 1, "b" => {"c" => 2, "d" => {"e" => 3, "f" => 4}, "g" => 5}, "h" => 6}
    expect(hash.fetch("a")).to eq(1)
    expect(hash.fetch("b")).to eq({"c" => 2, "d" => {"e" => 3, "f" => 4}, "g" => 5})
    expect(hash.fetch("h")).to eq(6)
  }

end

describe Mongo::Combinator1 do

  context "combinator" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_merge_1'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
      @db_name = @mongo_uri.db_name

      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client[@db_name]
      @combinator = Mongo::Combinator1.new(@db, 'people', 'gender', 'gender', '_id')
      @data = {
          :before => {
              :people => [
                  {"_id" => 11, "name" => "Joe", "gender" => 1},
                  {"_id" => 22, "name" => "Jane", "gender" => 2},
                  {"_id" => 33, "name" => "Other"}
              ],
              :gender => [
                  {"_id" => 1, "name" => "Male"},
                  {"_id" => 2, "name" => "Female"},
                  {"_id" => 3, "name" => "Other"}
              ]
          },
          :after => {
              :people => [
                  {"_id"=>11, "name"=>"Joe", "gender"=>{"_id"=>1, "name"=>"Male"}},
                  {"_id"=>22, "name"=>"Jane", "gender"=>{"_id"=>2, "name"=>"Female"}},
                  {"_id"=>33, "name"=>"Other"}
              ]
          }
      }
      load_fixture(@db, @data[:before])
    end

    after(:each) do
      @mongo_client.drop_database(@db_name)
    end

    it("should merge child into parent") {
      @combinator.merge_1 # initial merge_1
      match_fixture(@db, @data[:after])
    }

    it("should re-merge child into parent") {
      @combinator.merge_1 # initial merge_1
      @combinator.merge_1 # re-run merge_1
      match_fixture(@db, @data[:after])
    }

    #pending test for nested re-merge
  end

end

