// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/cdc/cdc_row_filter.h"

#include "yb/util/test_util.h"

namespace yb {
namespace cdc {

class CDCRowFilterTest : public YBTest {
 protected:
  // Helper to create a test RowMessage with a single int64 column
  RowMessage CreateTestRow(RowMessage::Op op, const std::string& column_name, int64_t value) {
    RowMessage row;
    row.set_op(op);

    auto* datum = row.add_new_tuple();
    datum->set_column_name(column_name);
    datum->set_datum_int64(value);

    return row;
  }

  // Helper to create a filter
  CDCRowFilterPB CreateFilter(const std::string& column_name, const std::vector<int64_t>& values) {
    CDCRowFilterPB filter;
    filter.set_column_name(column_name);
    for (auto value : values) {
      filter.add_values(value);
    }
    return filter;
  }
};

TEST_F(CDCRowFilterTest, EmptyFilterMatchesAll) {
  CDCRowFilterPB filter_pb;  // Empty filter
  CDCRowFilter filter(filter_pb);

  ASSERT_TRUE(filter.IsEmpty());

  auto row = CreateTestRow(RowMessage::INSERT, "shop_id", 10);
  auto result = filter.Matches(row);
  ASSERT_OK(result);
  ASSERT_TRUE(*result);
}

TEST_F(CDCRowFilterTest, SingleValueFilter) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  ASSERT_FALSE(filter.IsEmpty());

  // Matching value
  auto row1 = CreateTestRow(RowMessage::INSERT, "shop_id", 10);
  auto result1 = filter.Matches(row1);
  ASSERT_OK(result1);
  ASSERT_TRUE(*result1);

  // Non-matching value
  auto row2 = CreateTestRow(RowMessage::INSERT, "shop_id", 20);
  auto result2 = filter.Matches(row2);
  ASSERT_OK(result2);
  ASSERT_FALSE(*result2);
}

TEST_F(CDCRowFilterTest, MultipleValuesFilter) {
  auto filter_pb = CreateFilter("shop_id", {10, 20, 30});
  CDCRowFilter filter(filter_pb);

  // Matching values
  auto row1 = CreateTestRow(RowMessage::INSERT, "shop_id", 10);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row1)));

  auto row2 = CreateTestRow(RowMessage::INSERT, "shop_id", 20);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row2)));

  auto row3 = CreateTestRow(RowMessage::INSERT, "shop_id", 30);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row3)));

  // Non-matching value
  auto row4 = CreateTestRow(RowMessage::INSERT, "shop_id", 40);
  ASSERT_FALSE(*ASSERT_RESULT(filter.Matches(row4)));
}

TEST_F(CDCRowFilterTest, TransactionMarkersAlwaysIncluded) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  // BEGIN record (no tuple data)
  RowMessage begin_row;
  begin_row.set_op(RowMessage::BEGIN);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(begin_row)));

  // COMMIT record (no tuple data)
  RowMessage commit_row;
  commit_row.set_op(RowMessage::COMMIT);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(commit_row)));

  // SAFEPOINT record
  RowMessage safepoint_row;
  safepoint_row.set_op(RowMessage::SAFEPOINT);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(safepoint_row)));
}

TEST_F(CDCRowFilterTest, DDLRecordsAlwaysIncluded) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  // DDL record
  RowMessage ddl_row;
  ddl_row.set_op(RowMessage::DDL);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(ddl_row)));

  // TRUNCATE record
  RowMessage truncate_row;
  truncate_row.set_op(RowMessage::TRUNCATE);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(truncate_row)));
}

TEST_F(CDCRowFilterTest, ColumnNotFoundReturnsError) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  // Row with different column name
  auto row = CreateTestRow(RowMessage::INSERT, "user_id", 10);
  auto result = filter.Matches(row);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsNotFound());
}

TEST_F(CDCRowFilterTest, UpdateOperationFiltered) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  // UPDATE with matching value
  auto row1 = CreateTestRow(RowMessage::UPDATE, "shop_id", 10);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row1)));

  // UPDATE with non-matching value
  auto row2 = CreateTestRow(RowMessage::UPDATE, "shop_id", 20);
  ASSERT_FALSE(*ASSERT_RESULT(filter.Matches(row2)));
}

TEST_F(CDCRowFilterTest, DeleteOperationFiltered) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  // DELETE with matching value
  auto row1 = CreateTestRow(RowMessage::DELETE, "shop_id", 10);
  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row1)));

  // DELETE with non-matching value
  auto row2 = CreateTestRow(RowMessage::DELETE, "shop_id", 20);
  ASSERT_FALSE(*ASSERT_RESULT(filter.Matches(row2)));
}

TEST_F(CDCRowFilterTest, Int32ColumnConvertedToInt64) {
  auto filter_pb = CreateFilter("shop_id", {10});
  CDCRowFilter filter(filter_pb);

  RowMessage row;
  row.set_op(RowMessage::INSERT);
  auto* datum = row.add_new_tuple();
  datum->set_column_name("shop_id");
  datum->set_datum_int32(10);  // int32 instead of int64

  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row)));
}

TEST_F(CDCRowFilterTest, BoolColumnConvertedToInt64) {
  auto filter_pb = CreateFilter("is_active", {1});
  CDCRowFilter filter(filter_pb);

  RowMessage row;
  row.set_op(RowMessage::INSERT);
  auto* datum = row.add_new_tuple();
  datum->set_column_name("is_active");
  datum->set_datum_bool(true);  // bool converts to 1

  ASSERT_TRUE(*ASSERT_RESULT(filter.Matches(row)));

  // Test false converts to 0
  filter_pb = CreateFilter("is_active", {0});
  CDCRowFilter filter2(filter_pb);

  RowMessage row2;
  row2.set_op(RowMessage::INSERT);
  auto* datum2 = row2.add_new_tuple();
  datum2->set_column_name("is_active");
  datum2->set_datum_bool(false);

  ASSERT_TRUE(*ASSERT_RESULT(filter2.Matches(row2)));
}

}  // namespace cdc
}  // namespace yb
