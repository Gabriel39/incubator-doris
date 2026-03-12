// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <bthread/types.h>
#include <stdint.h>

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "concurrentqueue.h"
#include "core/block/block.h"
#include "exec/scan/scanner.h"
#include "exec/scan/task_executor/split_runner.h"
#include "runtime/runtime_profile.h"

namespace doris {

class RuntimeState;
class TupleDescriptor;
class WorkloadGroup;

namespace pipeline {
class ScanLocalStateBase;
class Dependency;
} // namespace pipeline

namespace vectorized {

class Scanner;
class ScannerDelegate;
class ScannerScheduler;
class ScannerScheduler;
class TaskExecutor;
class TaskHandle;
struct ScannerMemLimiter;

// Query-level memory arbitrator that distributes memory fairly across all scan contexts
struct ScannerMemShareArbitrator {
    ENABLE_FACTORY_CREATOR(ScannerMemShareArbitrator)
    TUniqueId query_id;
    int64_t query_mem_limit = 0;
    int64_t scan_mem_limit = 0;
    std::atomic<int64_t> total_scanner_mem_bytes = 0;

    ScannerMemShareArbitrator(const TUniqueId& qid, int64_t query_mem_limit, double max_scan_ratio);

    // Update memory allocation when scanner memory usage changes
    // Returns new scan memory limit for this context
    int64_t update_scanner_mem_bytes(int64_t old_value, int64_t new_value);
    void register_scan_node();
    std::string debug_string() const {
        return fmt::format("query_id: {}, query_mem_limit: {}, scan_mem_limit: {}",
                           print_id(query_id), query_mem_limit, scan_mem_limit);
    }
};

// Scan-context-level memory limiter that controls scanner concurrency based on memory
struct ScannerMemLimiter {
private:
    TUniqueId query_id;
    mutable std::mutex lock;
    // Parallelism of the scan operator
    const int64_t parallelism = 0;
    const bool serial_scan = false;
    const int64_t query_scan_mem_limit;
    std::atomic<int64_t> running_scanner_count = 0;

    std::atomic<int64_t> estimated_block_mem_bytes = 0;
    int64_t estimated_block_mem_bytes_update_count = 0;
    int64_t arb_scanner_mem_bytes = 0;
    std::atomic<int64_t> open_scanner_context_count = 0;

    // Memory limit for this scan node (shared by all instances), updated by memory share arbitrator
    std::atomic<int64_t> scan_mem_limit = 0;

public:
    ENABLE_FACTORY_CREATOR(ScannerMemLimiter)
    ScannerMemLimiter(const TUniqueId& qid, int64_t parallelism, bool serial_scan,
                      int64_t mem_limit)
            : query_id(qid),
              parallelism(parallelism),
              serial_scan(serial_scan),
              query_scan_mem_limit(mem_limit) {}

    // Calculate available scanner count based on memory limit
    int available_scanner_count(int ins_idx) const;

    int64_t update_running_scanner_count(int delta) { return running_scanner_count += delta; }

    // Re-estimated the average memory usage of a block, and update the estimated_block_mem_bytes accordingly.
    void reestimated_block_mem_bytes(int64_t value);
    void update_scan_mem_limit(int64_t value) { scan_mem_limit = value; }
    void update_arb_scanner_mem_bytes(int64_t value) {
        value = std::min(value, query_scan_mem_limit);
        arb_scanner_mem_bytes = value;
    }
    int64_t get_arb_scanner_mem_bytes() const { return arb_scanner_mem_bytes; }

    int64_t get_estimated_block_mem_bytes() const { return estimated_block_mem_bytes; }

    int64_t update_open_scanner_context_count(int delta) {
        return open_scanner_context_count.fetch_add(delta);
    }
    std::string debug_string() const {
        return fmt::format(
                "query_id: {}, parallelism: {}, serial_scan: {}, query_scan_mem_limit: {}, "
                "running_scanner_count: {}, estimated_block_mem_bytes: {}, "
                "estimated_block_mem_bytes_update_count: {}, arb_scanner_mem_bytes: {}, "
                "open_scanner_context_count: {}, scan_mem_limit: {}",
                print_id(query_id), parallelism, serial_scan, query_scan_mem_limit,
                running_scanner_count.load(), estimated_block_mem_bytes.load(),
                estimated_block_mem_bytes_update_count, arb_scanner_mem_bytes,
                open_scanner_context_count, scan_mem_limit);
    }
};

// Adaptive processor for dynamic scanner concurrency adjustment
struct ScannerAdaptiveProcessor {
    ENABLE_FACTORY_CREATOR(ScannerAdaptiveProcessor)
    ScannerAdaptiveProcessor() = default;
    ~ScannerAdaptiveProcessor() = default;
    // Expected scanners in this cycle

    int expected_scanners = 0;
    // Timing metrics
    // int64_t context_start_time = 0;
    // int64_t scanner_total_halt_time = 0;
    // int64_t scanner_gen_blocks_time = 0;
    // std::atomic_int64_t scanner_total_io_time = 0;
    // std::atomic_int64_t scanner_total_running_time = 0;
    // std::atomic_int64_t scanner_total_scan_bytes = 0;

    // Timestamps
    // std::atomic_int64_t last_scanner_finish_timestamp = 0;
    // int64_t check_all_scanners_last_timestamp = 0;
    // int64_t last_driver_output_full_timestamp = 0;
    int64_t adjust_scanners_last_timestamp = 0;

    // Adjustment strategy fields
    // bool try_add_scanners = false;
    // double expected_speedup_ratio = 0;
    // double last_scanner_scan_speed = 0;
    // int64_t last_scanner_total_scan_bytes = 0;
    // int try_add_scanners_fail_count = 0;
    // int check_slow_io = 0;
    // int32_t slow_io_latency_ms = 100; // Default from config
};

class ScanTask {
public:
    ScanTask(std::weak_ptr<ScannerDelegate> delegate_scanner) : scanner(delegate_scanner) {
        _resource_ctx = thread_context()->resource_ctx();
        DorisMetrics::instance()->scanner_task_cnt->increment(1);
    }

    ~ScanTask() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_resource_ctx->memory_context()->mem_tracker());
        cached_blocks.clear();
        DorisMetrics::instance()->scanner_task_cnt->increment(-1);
    }

private:
    // whether current scanner is finished
    bool eos = false;
    Status status = Status::OK();
    std::shared_ptr<ResourceContext> _resource_ctx;

public:
    std::weak_ptr<ScannerDelegate> scanner;
    std::list<std::pair<vectorized::BlockUPtr, size_t>> cached_blocks;
    bool is_first_schedule = true;
    // Use weak_ptr to avoid circular references and potential memory leaks with SplitRunner.
    // ScannerContext only needs to observe the lifetime of SplitRunner without owning it.
    // When SplitRunner is destroyed, split_runner.lock() will return nullptr, ensuring safe access.
    std::weak_ptr<SplitRunner> split_runner;

    void set_status(Status _status) {
        if (_status.is<ErrorCode::END_OF_FILE>()) {
            // set `eos` if `END_OF_FILE`, don't take `END_OF_FILE` as error
            eos = true;
        }
        status = _status;
    }
    Status get_status() const { return status; }
    bool status_ok() { return status.ok() || status.is<ErrorCode::END_OF_FILE>(); }
    bool is_eos() const { return eos; }
    void set_eos(bool _eos) { eos = _eos; }
};

// ScannerContext is responsible for recording the execution status
// of a group of Scanners corresponding to a ScanNode.
// Including how many scanners are being scheduled, and maintaining
// a producer-consumer blocks queue between scanners and scan nodes.
//
// ScannerContext is also the scheduling unit of ScannerScheduler.
// ScannerScheduler schedules a ScannerContext at a time,
// and submits the Scanners to the scanner thread pool for data scanning.
class ScannerContext : public std::enable_shared_from_this<ScannerContext>,
                       public HasTaskExecutionCtx {
    ENABLE_FACTORY_CREATOR(ScannerContext);
    friend class ScannerScheduler;

public:
    ScannerContext(RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
                   const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners,
                   int64_t limit_, std::shared_ptr<pipeline::Dependency> dependency,
                   std::shared_ptr<ScannerMemShareArbitrator> arb,
                   std::shared_ptr<ScannerMemLimiter> limiter, int ins_idx,
                   bool enable_adaptive_scan
#ifdef BE_TEST
                   ,
                   int num_parallel_instances
#endif
    );

    ~ScannerContext() override;
    Status init();

    vectorized::BlockUPtr get_free_block(bool force);
    void return_free_block(vectorized::BlockUPtr block);
    void clear_free_blocks();
    inline void inc_block_usage(size_t usage) { _block_memory_usage += usage; }

    int64_t block_memory_usage() { return _block_memory_usage; }

    // Caller should make sure the pipeline task is still running when calling this function
    void update_peak_running_scanner(int num);
    void reestimated_block_mem_bytes(int64_t num);

    // Get next block from blocks queue. Called by ScanNode/ScanOperator
    // Set eos to true if there is no more data to read.
    Status get_block_from_queue(RuntimeState* state, vectorized::Block* block, bool* eos, int id);

    [[nodiscard]] Status validate_block_schema(Block* block);

    // submit the running scanner to thread pool in `ScannerScheduler`
    // set the next scanned block to `ScanTask::current_block`
    // set the error state to `ScanTask::status`
    // set the `eos` to `ScanTask::eos` if there is no more data in current scanner
    Status submit_scan_task(std::shared_ptr<ScanTask> scan_task, std::unique_lock<std::mutex>&);

    // Push back a scan task.
    void push_back_scan_task(std::shared_ptr<ScanTask> scan_task);

    // Return true if this ScannerContext need no more process
    bool done() const { return _is_finished || _should_stop; }

    std::string debug_string();

    std::shared_ptr<TaskHandle> task_handle() const { return _task_handle; }

    std::shared_ptr<ResourceContext> resource_ctx() const { return _resource_ctx; }

    RuntimeState* state() { return _state; }

    void stop_scanners(RuntimeState* state);

    int batch_size() const { return _batch_size; }

    // During low memory mode, there will be at most 4 scanners running and every scanner will
    // cache at most 1MB data. So that every instance will keep 8MB buffer.
    bool low_memory_mode() const;

    // TODO(yiguolei) add this as session variable
    int32_t low_memory_mode_scan_bytes_per_scanner() const {
        return 1 * 1024 * 1024; // 1MB
    }

    int32_t low_memory_mode_scanners() const { return 4; }

    pipeline::ScanLocalStateBase* local_state() const { return _local_state; }

    // the unique id of this context
    std::string ctx_id;
    TUniqueId _query_id;

    bool _should_reset_thread_name = true;

    int32_t num_scheduled_scanners() {
        std::lock_guard<std::mutex> l(_transfer_lock);
        return _num_scheduled_scanners;
    }

    Status schedule_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                              std::unique_lock<std::mutex>& transfer_lock,
                              std::unique_lock<std::shared_mutex>& scheduler_lock);

protected:
    /// Four criteria to determine whether to increase the parallelism of the scanners
    /// 1. It ran for at least `SCALE_UP_DURATION` ms after last scale up
    /// 2. Half(`WAIT_BLOCK_DURATION_RATIO`) of the duration is waiting to get blocks
    /// 3. `_free_blocks_memory_usage` < `_max_bytes_in_queue`, remains enough memory to scale up
    /// 4. At most scale up `MAX_SCALE_UP_RATIO` times to `_max_thread_num`
    void _set_scanner_done();

    RuntimeState* _state = nullptr;
    pipeline::ScanLocalStateBase* _local_state = nullptr;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _output_tuple_desc = nullptr;
    const RowDescriptor* _output_row_descriptor = nullptr;

    std::mutex _transfer_lock;
    std::list<std::shared_ptr<ScanTask>> _tasks_queue;

    Status _process_status = Status::OK();
    std::atomic_bool _should_stop = false;
    std::atomic_bool _is_finished = false;

    // Lazy-allocated blocks for all scanners to share, for memory reuse.
    moodycamel::ConcurrentQueue<vectorized::BlockUPtr> _free_blocks;

    int _batch_size;
    // The limit from SQL's limit clause
    int64_t limit;

    int64_t _max_bytes_in_queue = 0;
    // Using stack so that we can resubmit scanner in a LIFO order, maybe more cache friendly
    std::stack<std::shared_ptr<ScanTask>> _pending_scanners;
    // Scanner that is submitted to the scheduler.
    std::atomic_int _num_scheduled_scanners = 0;
    // Scanner that is eos or error.
    int32_t _num_finished_scanners = 0;
    // weak pointer for _scanners, used in stop function
    std::vector<std::weak_ptr<ScannerDelegate>> _all_scanners;
    std::shared_ptr<RuntimeProfile> _scanner_profile;
    // This counter refers to scan operator's local state
    RuntimeProfile::Counter* _scanner_memory_used_counter = nullptr;
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    RuntimeProfile::Counter* _scale_up_scanners_counter = nullptr;
    std::shared_ptr<ResourceContext> _resource_ctx;
    std::shared_ptr<pipeline::Dependency> _dependency = nullptr;
    std::shared_ptr<doris::vectorized::TaskHandle> _task_handle;

    std::atomic<int64_t> _block_memory_usage = 0;

    // adaptive scan concurrency related

    ScannerScheduler* _scanner_scheduler = nullptr;
    MOCK_REMOVE(const) int32_t _min_scan_concurrency_of_scan_scheduler = 0;
    // The overall target of our system is to make full utilization of the resources.
    // At the same time, we dont want too many tasks are queued by scheduler, that is not necessary.
    // Each scan operator can submit _max_scan_concurrency scanner to scheduelr if scheduler has enough resource.
    // So that for a single query, we can make sure it could make full utilization of the resource.
    int32_t _max_scan_concurrency = 0;
    MOCK_REMOVE(const) int32_t _min_scan_concurrency = 1;

    std::shared_ptr<ScanTask> _pull_next_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                                                   int32_t current_concurrency);

    int32_t _get_margin(std::unique_lock<std::mutex>& transfer_lock,
                        std::unique_lock<std::shared_mutex>& scheduler_lock);

    // Memory-aware adaptive scheduling
    std::shared_ptr<ScannerMemLimiter> _scanner_mem_limiter = nullptr;
    std::shared_ptr<ScannerMemShareArbitrator> _mem_share_arb = nullptr;
    std::shared_ptr<ScannerAdaptiveProcessor> _adaptive_processor = nullptr;
    const int _ins_idx;
    const bool _enable_adaptive_scanners = false;

    // Adjust scan memory limit based on arbitrator feedback
    void _adjust_scan_mem_limit(int64_t old_scanner_mem_bytes, int64_t new_scanner_mem_bytes);

    // Calculate available scanner count for adaptive scheduling
    int _available_pickup_scanner_count();

    // TODO: Add implementation of runtime_info_feed_back
    // adaptive scan concurrency related end
};
} // namespace vectorized
} // namespace doris
