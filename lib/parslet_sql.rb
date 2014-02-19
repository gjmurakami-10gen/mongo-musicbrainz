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

require 'parslet'

class CreateTablesParser < Parslet::Parser
  rule(:space) { match('\s').repeat(1) }
  rule(:space?) { space.maybe }
  rule(:ident) { match('\w').repeat(1) }
  rule(:int) { match('-').maybe >> match('\d').repeat(1) }
  rule(:string) { str("'") >> match("[^']").repeat >> str("'") }
  rule(:size) { str('(') >> match('\d').repeat(1) >> str(')') }
  rule(:array) { str('[]') }
  rule(:comment) { (str('--') >> space? >> match('[^\n]').repeat >> match('\n') >> space?).repeat(1).as(:comment) >> space? }

  rule(:sql) { (space | directive.as(:directive) | create_table.as(:create_table) | other_sql.as(:other_sql) | comment.as(:comment)).repeat }
  rule(:directive) { match('\\\\') >> match('[^\n]').repeat >> space? }
  rule(:create_table) { str('CREATE TABLE') >> space >> table_name >> columns.as(:columns) >> terminator >> space? }
  rule(:table_name) { ident.as(:table_name) >> space? }
  rule(:columns) { str("(") >> space? >> column_defs >> str(")") >> space? }
  rule(:column_defs) { column_line.repeat >> (constraint | last_column_line).repeat(1) >> space? }
  rule(:column_line) { (constraint | column_def) >> str(',') >> space? >> comment.maybe >> space? }
  rule(:last_column_line) { column_def >> space? >> comment.maybe >> space? }
  rule(:column_def) { column_name >> data_type >> column_constraint.maybe >> space? }
  rule(:column_name) { ident.as(:column_name) >> space? }
  rule(:data_type) { (ident >> size.maybe >> array.maybe).as(:data_type) >> space? }
  rule(:column_constraint) {
    (
      (
        constraint |
        check |
        str("NOT NULL") |
        str("NULL") |
        str("DEFAULT") >> space >> default_value |
        str("WITH") >> space >> storage_parameter
      ) >> space?
    ).repeat(1).as(:column_constraint)
  }
  rule(:default_value) { (ident >> call_args.maybe | int | string) >> space? }
  rule(:constraint) { (str("CONSTRAINT") >> space >> ident >> space).maybe >> check >> space? }
  rule(:check) { str("CHECK") >> space >> call_args >> space? }
  rule(:call_args) { str("(") >> (not_parens | call_args).repeat >> str(")") >> space? }
  rule(:not_parens) { match('[^()]').repeat(1) }
  rule(:storage_parameter) { str("TIME ZONE") >> space? }
  rule(:terminator) { match(';') >> space? }
  rule(:other_sql) { match('[^;]').repeat(1) >> terminator >> space? }
  root(:sql)
end
