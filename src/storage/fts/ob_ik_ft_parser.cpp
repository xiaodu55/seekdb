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

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/ob_ik_ft_parser.h"

#include "lib/charset/ob_charset.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_vector.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "data_plane/fts/ob_fts_struct.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ik/ob_ik_cjk_processor.h"
#include "storage/fts/ik/ob_ik_letter_processor.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_quantifier_processor.h"
#include "storage/fts/ik/ob_ik_surrogate_processor.h"

namespace oceanbase
{
namespace storage
{
int ObIKFTParser::compute_coll_type(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.cs_) || OB_ISNULL(param.cs_->name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (CS_TYPE_INVALID == (coll_type_ = ObCharset::collation_type(param.cs_->name))) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObIKFTParser::init_core(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    coll_type_ = ObCollationType::CS_TYPE_INVALID;
    if (OB_FAIL(init_dict(param))) {
    } else if (OB_FAIL(init_segmenter(param))) {
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObIKFTParser::prepare_for_call(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(param.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // defensive reset: processors are clean after a full parse, but a previous
    // parse may have bailed out early on error
    for (ObIIKProcessor *segmenter : segmenters_) {
      if (OB_NOT_NULL(segmenter)) {
        segmenter->reset_state();
      }
    }
    ctx_alloc_ = param.allocator_;
    if (OB_FAIL(compute_coll_type(param))) {
    } else if (OB_ISNULL(ctx_ = OB_NEWx(TokenizeContext,
                                      param.allocator_,
                                      coll_type_,
                                      *(param.allocator_),
                                      param.fulltext_,
                                      param.ft_length_,
                                      param.ik_param_.mode_ == ObFTIKParam::Mode::SMART))) {
      ctx_alloc_ = nullptr;
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ctx_->init())) {
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
      ctx_->~TokenizeContext();
      ctx_alloc_->free(ctx_);
      ctx_ = nullptr;
      ctx_alloc_ = nullptr;
    }
  }

  return ret;
}

void ObIKFTParser::release_call()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~TokenizeContext();
    if (OB_NOT_NULL(ctx_alloc_)) {
      ctx_alloc_->free(ctx_);
    } else {
      allocator_.free(ctx_);
    }
    ctx_ = nullptr;
    ctx_alloc_ = nullptr;
  }
}

int ObIKFTParser::get_next_token(const char *&word,
                                 int64_t &word_len,
                                 int64_t &char_cnt,
                                 int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  const char *output_word;
  int64_t len;
  int64_t offset;
  int64_t cnt;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool accept_token = false;
    while (OB_SUCC(ret) && !accept_token) {
      if (OB_FAIL(produce())) {
      } else if (OB_FAIL(ctx_->get_next_token(output_word, len, offset, cnt))) {
        if (OB_ITER_END == ret) {
          // ok, end this iter
        } else {
        }
      } else {
        bool is_stop = false;
        // if (!OB_ISNULL(dict_stop_)
        //     && OB_FAIL(dict_stop_->match(ObString(len, output_word + offset), is_stop))) {
        //   LOG_WARN("Failed to match stopwords", K(ret));
        // } else
        if (!is_stop) {
          word = output_word + offset;
          word_len = len;
          char_cnt = cnt;
          word_freq = 1;
          accept_token = true;
        } else {
        }
      }
    }
  }

  return ret;
}

int ObIKFTParser::produce()
{
  int ret = OB_SUCCESS;
  // Loop until end or has data to output
  while (OB_SUCC(ret) && ctx_->result_list().empty() && !ctx_->iter_end()) {
    if (OB_FAIL(process_next_batch())) {
      if (OB_ITER_END == ret) {
        // ok
      } else {
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIKFTParser::process_one_char(TokenizeContext &ctx,
                                   const char *ch,
                                   const uint8_t char_len,
                                   const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  // process by char with all segmenters; pass the already-fetched char info through
  // and iterate the flat array to avoid per-char list traversal
  for (int64_t i = 0; OB_SUCC(ret) && i < segmenter_cnt_; ++i) {
    if (OB_FAIL(segmenter_arr_[i]->process(ctx, ch, char_len, type))) {
    }
  }
  return ret;
}

int ObIKFTParser::process_next_batch()
{
  int ret = OB_SUCCESS;
  ctx_->reset_resource();

  // handle next segmenter
  bool do_seg = false;

  if (ctx_->iter_end()) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !do_seg && !ctx_->iter_end()) {
      const char *ch;
      uint8_t char_len = 0;
      ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
      if (OB_FAIL(ctx_->current_char(ch, char_len))) {
      } else if (OB_FAIL(ctx_->current_char_type(type))) {
      } else if (OB_FAIL(process_one_char(*ctx_, ch, char_len, type))) {
      } else {
        // 1. check segmention
        if (ctx_->handle_size() > SEGMENT_LIMIT && type == ObFTCharUtil::CharType::USELESS) {
          do_seg = true;
        }

        // 2. move to next;
        if (OB_FAIL(ctx_->step_next())) {
          if (OB_ITER_END == ret) {
          } else {
          }
        }
      } // end of one batch
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      // persistent arbitrator: reclaim last batch's chains and reuse the hashmap
      arbitrator_.reuse();
      if (OB_FAIL(arbitrator_.process(*ctx_))) {
      } else if (OB_FAIL(arbitrator_.output_result(*ctx_))) {
      }
    } else {
      // Already logged.
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// reusable parser core pool
namespace
{
constexpr const char *IK_PARSER_POOL_LABEL = "IkParserPool";

struct ObIKParserPool
{
  ObSpinLock lock_;
  ObArenaAllocator alloc_;
  ObVector<ObIKFTParser *> idle_;

  explicit ObIKParserPool() : alloc_(ObMemAttr(IK_PARSER_POOL_LABEL))
  {
  }
};

ObIKParserPool &get_ik_parser_pool()
{
  static ObIKParserPool pool;
  return pool;
}
}  // anonymous namespace

int acquire_pooled_ik_parser(const ObFTParserParam &param, ObFTDictHub *hub, ObIKFTParser *&parser)
{
  int ret = OB_SUCCESS;
  parser = nullptr;
  ObIKParserPool &pool = get_ik_parser_pool();
  ObIKFTParser *core = nullptr;
  // pool.alloc_ is not thread-safe: core creation and pool teardown must hold the lock
  ObSpinLockGuard guard(pool.lock_);
  if (pool.idle_.size() > 0) {
    core = pool.idle_.at(pool.idle_.size() - 1);
    pool.idle_.remove(pool.idle_.size() - 1);
  } else if (OB_ISNULL(core = OB_NEWx(ObIKFTParser, &pool.alloc_, pool.alloc_, hub))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(core->init_core(param))) {
    OB_DELETEx(ObIKFTParser, &pool.alloc_, core);
    core = nullptr;
  }
  if (OB_SUCC(ret)) {
    parser = core;
  }
  return ret;
}

void release_pooled_ik_parser(ObIKFTParser *parser)
{
  if (OB_NOT_NULL(parser)) {
    parser->release_call();
    ObIKParserPool &pool = get_ik_parser_pool();
    ObSpinLockGuard guard(pool.lock_);
    if (OB_SUCCESS != pool.idle_.push_back(parser)) {
      // pool push failed (OOM): destruct the core; the pool allocator keeps its blocks
      parser->~ObIKFTParser();
    }
  }
}

void destroy_ik_parser_pool()
{
  ObIKParserPool &pool = get_ik_parser_pool();
  ObSpinLockGuard guard(pool.lock_);
  for (int64_t i = 0; i < pool.idle_.size(); ++i) {
    if (OB_NOT_NULL(pool.idle_.at(i))) {
      pool.idle_.at(i)->~ObIKFTParser();
    }
  }
  pool.idle_.reset();
  pool.alloc_.reset();
}

int ObIKFTParserDesc::segment(ObFTParserParam *param, ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObIKFTParser *parser = nullptr;
  ObFTDictHub *hub = nullptr;
  if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObFTParseData::instance().get_dict_hub(hub))) {
  } else if (OB_FAIL(acquire_pooled_ik_parser(*param, hub, parser))) {
  } else if (OB_FAIL(parser->prepare_for_call(*param))) {
    // the core itself is still healthy, return it to the pool for the next call
    release_pooled_ik_parser(parser);
    parser = nullptr;
  } else {
    iter = parser;
  }

  return ret;
}

void ObIKFTParserDesc::free_token_iter(ObFTParserParam *param,
                                       ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    ObIKFTParser *parser = static_cast<ObIKFTParser *>(iter);
    parser->release_call();
    release_pooled_ik_parser(parser);
  }
  iter = nullptr;
}


int ObIKFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

int ObIKFTParser::init_dict(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  ObIFTDict *tmp_dict = nullptr;

  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
  }

  ObFTRangeDict *dict = nullptr;
  ObFTDictDesc main_dict_desc("main_dict",
                              ObFTDictType::DICT_IK_MAIN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc quan_dict_desc("quan_dict",
                              ObFTDictType::DICT_IK_QUAN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc stopword_dict_desc("stopword",
                                  ObFTDictType::DICT_IK_STOP,
                                  ObCharsetType::CHARSET_UTF8MB4,
                                  ObCollationType::CS_TYPE_UTF8MB4_BIN);

  if (should_read_newest_table()) {
    // clear dict cache, always false now
  } else {
    if (OB_FAIL(init_single_dict(main_dict_desc, cache_main_))) {
    } else if (OB_FAIL(init_single_dict(quan_dict_desc, cache_quan_))) {
    } else if (OB_FAIL(init_single_dict(stopword_dict_desc, cache_stop_))) {
    }
  }

  if (OB_FAIL(ret)) {
    // already logged.
  } else if (OB_FAIL(build_dict_from_cache(main_dict_desc, cache_main_, dict_main_))) {
  } else if (OB_FAIL(build_dict_from_cache(quan_dict_desc, cache_quan_, dict_quan_))) {
  } else if (OB_FAIL(build_dict_from_cache(stopword_dict_desc, cache_stop_, dict_stop_))) {
  }

  return ret;
}

int ObIKFTParser::init_single_dict(ObFTDictDesc desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hub_->load_cache(desc, container))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(hub_->build_cache(desc, container))) {
      }
    } else {
    }
  }
  return ret;
}

int ObIKFTParser::init_segmenter(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  // do have an order
  ObIKLetterProcessor *letter_seg = nullptr;
  ObIKQuantifierProcessor *cnqsg = nullptr;
  ObIKCJKProcessor *cjksg = nullptr;
  ObIKSurrogateProcessor *surrogate_seg = nullptr;
  if (OB_ISNULL(letter_seg = OB_NEWx(ObIKLetterProcessor, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(dict_quan_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cnqsg = OB_NEWx(ObIKQuantifierProcessor, &allocator_, *dict_quan_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(dict_main_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cjksg = OB_NEWx(ObIKCJKProcessor, &allocator_, *dict_main_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(surrogate_seg = OB_NEWx(ObIKSurrogateProcessor, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(segmenters_.push_back(letter_seg))) {
  } else if (FALSE_IT(letter_seg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cnqsg))) {
  } else if (FALSE_IT(cnqsg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cjksg))) {
  } else if (FALSE_IT(cjksg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(surrogate_seg))) {
  } else if (OB_FALSE_IT(surrogate_seg = nullptr)) {
  }
  // push back by order, quantifier is before cjk

  if (OB_SUCC(ret)) {
    segmenter_cnt_ = 0;
    for (ObIIKProcessor *segmenter : segmenters_) {
      segmenter_arr_[segmenter_cnt_++] = segmenter;
    }
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKLetterProcessor, &allocator_, letter_seg);
    OB_DELETEx(ObIKQuantifierProcessor, &allocator_, cnqsg);
    OB_DELETEx(ObIKCJKProcessor, &allocator_, cjksg);
    OB_DELETEx(ObIKSurrogateProcessor, &allocator_, surrogate_seg);
  }
  return ret;
}

void ObIKFTParser::reset()
{
  if (!OB_ISNULL(ctx_)) {
    ctx_->~TokenizeContext();
    if (OB_NOT_NULL(ctx_alloc_)) {
      ctx_alloc_->free(ctx_);
    } else {
      allocator_.free(ctx_);
    }
    ctx_ = nullptr;
    ctx_alloc_ = nullptr;
  }

  for (ObIIKProcessor *segmenter : segmenters_) {
    if (!OB_ISNULL(segmenter)) {
      segmenter->~ObIIKProcessor();
      allocator_.free(segmenter);
    }
  }
  segmenters_.clear();

  cache_main_.reset();
  cache_quan_.reset();
  cache_stop_.reset();

  if (!OB_ISNULL(dict_main_)) {
    dict_main_->~ObIFTDict();
    allocator_.free(dict_main_);
  }
  if (!OB_ISNULL(dict_quan_)) {
    dict_quan_->~ObIFTDict();
    allocator_.free(dict_quan_);
  }
  if (!OB_ISNULL(dict_stop_)) {
    dict_stop_->~ObIFTDict();
    allocator_.free(dict_stop_);
  }

  is_inited_ = false;
}

bool ObIKFTParser::should_read_newest_table() const { return false; }
int ObIKFTParser::build_dict_from_cache(const ObFTDictDesc &desc,
                                        ObFTCacheRangeContainer &container,
                                        ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(dict = OB_NEWx(ObFTRangeDict, &allocator_, allocator_, &container, desc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(dict->init())) {
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIFTDict, &allocator_, dict);
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
