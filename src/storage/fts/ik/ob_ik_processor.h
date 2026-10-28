/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_PROCESSOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
// chunked deque for the IK result tokens: constant-time push/pop with slot
// recycling on reset(), removing the per-token node allocations of ObList.
// blocks are allocated from the (per-parse) allocator and reclaimed with it.
class ObFastTokenDeque
{
public:
  static constexpr int64_t BLOCK_BITS = 8;
  static constexpr int64_t BLOCK_SIZE = 1 << BLOCK_BITS;
  static constexpr int64_t MAX_BLOCK_CNT = 64;

  explicit ObFastTokenDeque(ObIAllocator &allocator)
      : allocator_(allocator), blocks_(), alloc_cnt_(0), head_(0), size_(0)
  {}
  ~ObFastTokenDeque() = default;

  int push_back(const ObIKToken &token)
  {
    int ret = OB_SUCCESS;
    const int64_t pos = head_ + size_;
    const int64_t block = pos >> BLOCK_BITS;
    if (OB_UNLIKELY(block >= MAX_BLOCK_CNT)) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_ISNULL(blocks_[block])) {
      if (OB_ISNULL(blocks_[block] = static_cast<ObIKToken *>(
                        allocator_.alloc(sizeof(ObIKToken) * BLOCK_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ++alloc_cnt_;
      }
    }
    if (OB_SUCC(ret)) {
      blocks_[block][pos & (BLOCK_SIZE - 1)] = token;
      ++size_;
    }
    return ret;
  }

  ObIKToken &get_first() { return blocks_[head_ >> BLOCK_BITS][head_ & (BLOCK_SIZE - 1)]; }
  void pop_front() { --size_; ++head_; }
  bool empty() const { return 0 == size_; }
  // recycles slots in place; allocated blocks stay for reuse within this parse
  void reset() { head_ = 0; size_ = 0; }

private:
  ObIAllocator &allocator_;
  ObIKToken *blocks_[MAX_BLOCK_CNT] = {};
  int64_t alloc_cnt_;
  int64_t head_;
  int64_t size_;
};

class TokenizeContext
{
public:
  TokenizeContext(ObCollationType coll_type,
                  ObIAllocator &allocator,
                  const char *fulltext,
                  int64_t fulltext_len,
                  bool is_smart);

  ~TokenizeContext();

  int init();
  int reset_resource();

  int get_next_token(const char *&word, int64_t &word_len, int64_t &offset, int64_t &char_cnt);

  int compound(ObIKToken &result);

  int current_char(const char *&ch, uint8_t &char_len);
  int current_char_type(ObFTCharUtil::CharType &type);

  int step_next();

  ObCollationType collation() const;
  int64_t get_end_cursor() const;
  const char *fulltext() const;
  int64_t fulltext_len() const;
  int64_t get_cursor() const;

  bool is_last() const;
  bool iter_end() const;
  bool is_smart() const;

  int add_chain(ObIKTokenChain *chain);
  int add_token(const char *fulltext,
                int64_t offset,
                int64_t length,
                int64_t char_cnt,
                ObIKTokenType type);

  ObFTSortList &token_list() { return token_list_; }

  ObFastTokenDeque &result_list() { return result_list_; }

  int32_t handle_size() const { return handle_size_; }

private:
  int prepare_next_char();

  ObCollationType coll_type_;
  const ObCharsetInfo *cs_;  // cached for the per-character well-formedness check
  const char *fulltext_;
  int64_t fulltext_len_;

  int64_t cursor_;
  int64_t next_char_len_;
  ObFTCharUtil::CharType next_char_type_;

  uint32_t handle_size_;
  bool is_smart_;

  ObFTSortList token_list_;
  ObFastTokenDeque result_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(TokenizeContext);
};

class ObIIKProcessor
{
public:
  ObIIKProcessor() {}

  virtual ~ObIIKProcessor() {}

  int process(TokenizeContext &ctx);

  // dispatch with the caller-fetched char info, avoiding repeated context reads
  OB_INLINE int process(TokenizeContext &ctx,
                        const char *ch,
                        const uint8_t char_len,
                        const ObFTCharUtil::CharType type)
  {
    return do_process(ctx, ch, char_len, type);
  }

  // Reset per-parse mutable state so the processor can be reused across parses.
  virtual void reset_state() {}

  virtual int do_process(TokenizeContext &ctx,
                         const char *ch,
                         const uint8_t char_len,
                         const ObFTCharUtil::CharType type)
      = 0;
};

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_PROCESSOR_H_
