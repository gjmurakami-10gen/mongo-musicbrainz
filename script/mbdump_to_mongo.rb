#!/usr/bin/env ruby
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

require 'time'
require 'json'
require 'pp'
require 'mongo'
require 'benchmark'
require 'ruby-prof'
require 'trollop'

class Fixnum
  def to_s_with_comma
    self.to_s.gsub(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1,")
  end
end

def profile
  result = RubyProf.profile { yield }
  RubyProf::FlatPrinter.new(result).print(STDOUT)
  RubyProf::GraphPrinter.new(result).print(STDOUT)
end

def file_to_s(file)
  IO.read(file).chomp
end

BASE_DIR = File.expand_path('../..', __FILE__)
FULLEXPORT_DIR = "#{BASE_DIR}/ftp.musicbrainz.org/pub/musicbrainz/data/fullexport"
LATEST = "#{FULLEXPORT_DIR}/LATEST"
MBDUMP_DIR = "#{BASE_DIR}/data/fullexport/#{file_to_s(LATEST)}/mbdump"
SCHEMA_FILE = "#{BASE_DIR}/schema/create_tables.json"
MONGO_DBNAME = "musicbrainz"

$create_tables = JSON.parse(IO.read(SCHEMA_FILE))

$transform = {
    'BOOLEAN' => Proc.new {|s| if s == 't'; true; elsif s == 'f'; false; else raise 'BOOLEAN'; end },
    'CHAR(2)' => nil,
    'CHAR(3)' => nil,
    'CHAR(4)' => nil,
    'CHAR(8)' => nil,
    'CHAR(11)' => nil,
    'CHAR(12)' => nil,
    'CHAR(16)' => nil,
    'CHAR(28)' => nil,
    'CHARACTER(15)' => nil,
    'INT' => Proc.new {|s| s.to_i },
    'INTEGER' => Proc.new {|s| s.to_i },
    'SERIAL' => Proc.new {|s| s.to_i },
    'SMALLINT' => Proc.new {|s| s.to_i },
    'TEXT' => nil,
    'TIMESTAMP' => Proc.new {|s| Time.parse(s).gmtime },
    'UUID' => nil,
    'uuid' => nil,
    'VARCHAR' => nil,
    'VARCHAR(10)' => nil,
    'VARCHAR(50)' => nil,
    'VARCHAR(100)' => nil,
    'VARCHAR(255)' => nil,
    'INTEGER[]' => Proc.new {|s| s[/\{([^}]*)\}/,1].split(',').collect{|si| si.to_i} },
    'POINT' => Proc.new {|s| s[/\(([^)]*)\)/,1].split(',').collect{|si| si.to_f} }
}

def get_columns(table_name)
  statement = $create_tables.find{|sql| sql.has_key?('create_table') && sql['create_table']['table_name'] == table_name}
  create_table = statement['create_table']
  create_table['columns']
end

def merge_transforms(columns)
  columns.collect{|column|
    transform = $transform.fetch(column['data_type']){|key| raise "$transform[#{key.inspect}] unimplemented for column #{column['column_name'].inspect}"}
    column.merge('transform' => transform)
  }
end

def load_table(db, name, options)
  collection = db[name]
  collection.remove
  columns = get_columns(name)
  columns = merge_transforms(columns)
  columns = columns.collect{|column| [column['column_name'], column['transform']]}
  file_name = "#{MBDUMP_DIR}/#{name}"
  slice_size = options[:profile] ? 10_000 : 100_000
  count = 0
  real = 0.0
  file = File.open(file_name)
  file.each_slice(slice_size) do |lines|
    count += lines.count
    tms = Benchmark.measure do
      docs = lines.collect do |line|
        values = line.chomp.split(/\t/, -1)
        zip = columns.zip(values).select{|e| e[1] != "\\N" }
        doc = zip.collect do |column, value|
          key = column.first #column['column_name']
          key = '_id' if key == 'id'
          transform = column.last #column['transform']
          value = transform.call(value) if transform
          [key, value]
        end
        Hash[*doc.flatten(1)]
      end
      collection.insert(docs)
    end
    real += tms.real
    puts "collection:#{name} pos:#{(100.0*file.pos/file.size).round}% real:#{real.round} count:#{count.to_s_with_comma} docs_per_sec:#{(lines.size.to_f/tms.real).round}"
    STDOUT.flush
    break if options[:profile]
  end
end

if $0 == __FILE__
  banner = "usage: #{$0} [options] table_names"

  options = Trollop::options do
    banner banner
    opt :profile, "Profile", :short => 'p', :default => false
  end

  abort banner if ARGV.size < 1

  client = Mongo::MongoClient.from_uri
  db = client[MONGO_DBNAME]

  ARGV.each_with_index do |arg, index|
    puts "[#{index+1} of #{ARGV.size}] #{arg}"
    if options[:profile]
      profile { load_table(db, arg, options) }
    else
      load_table(db, arg, options)
    end
  end

  client.close
end