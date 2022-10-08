
#include <iostream>
#include <vector>
#include <memory>
#include <atomic>
#include <optional>
#include <thread>

#include <unistd.h>

#include <disruptorplus/ring_buffer.hpp>
#include <disruptorplus/single_threaded_claim_strategy.hpp>
#include <disruptorplus/multi_threaded_claim_strategy.hpp>
#include <disruptorplus/spin_wait_strategy.hpp>
#include <disruptorplus/sequence_barrier.hpp>

using namespace disruptorplus;


using Milliseconds = std::uint64_t;

class Job {
public:
    virtual void poll(std::uint64_t threadId, Milliseconds deadline) = 0;
};

class Scheduler {
    public:
        void addJob(std::unique_ptr<Job> job) {
            _jobs.push_back(std::move(job));
        }

        void run(std::uint64_t numThreads) {
            for (auto i = 0ull; i < numThreads; ++i) {
                _workers.push_back(std::thread([&, id = i]{
                    while (!_isShutdown.load()) {
                        for (auto &&j : _jobs) {
                            j->poll(id, 0 /* TODO */);
                        }
                    }
                }));
            }
        }

        void shutdown() {
            _isShutdown.store(true);
        }

        void join() {
            for (auto && t: _workers) t.join();
        }

    private:
        std::atomic_bool _isShutdown{false};
        std::vector<std::thread> _workers;
        std::vector<std::unique_ptr<Job>> _jobs;
};

class SequentialJob final : public Job {
    public:
        SequentialJob(std::unique_ptr<Job> job) : _job(std::move(job)) {}

        void poll(std::uint64_t threadId, Milliseconds deadline) override {
            if (!_inProgress.exchange(true)) {
                _job->poll(threadId, deadline);

                _inProgress.store(false);
            }
        }

    private: 
        std::atomic_bool _inProgress{false};
        std::unique_ptr<Job> _job;
};

class ExampleJob final : public Job {
    public:
        explicit ExampleJob(std::string msg) : _msg(std::move(msg)) {}
        void poll(std::uint64_t threadId, Milliseconds) override {
            std::cout << threadId << ": " << _msg << std::endl;
        } 

    private:
        std::string _msg;
};
template <typename T, template <typename S> typename ClaimStrategy>
struct WorkQueue {
    explicit WorkQueue(size_t bufferSize)
        : waitStrategy(),
          claimStrategy(bufferSize, waitStrategy),
          consumed(waitStrategy),
          buffer(bufferSize) {
        claimStrategy.add_claim_barrier(consumed);
    }

    void enqueue(T&& t) {
        sequence_t seq = claimStrategy.claim_one();
        buffer[seq] = std::move(t);
        claimStrategy.publish(seq);
    }

    T& peek() {
        if (nextToRead >= lastAvailable) {
            lastAvailable = claimStrategy.wait_until_published(nextToRead);
        }

        auto& res = buffer[nextToRead];
        return res;
    }

    void pop() {
        if (nextToRead == lastAvailable) {
            consumed.publish(lastAvailable);
        }
        ++nextToRead;
    }

    sequence_t nextToRead{0};
    sequence_t lastAvailable{0};
    spin_wait_strategy waitStrategy;
    ClaimStrategy<spin_wait_strategy> claimStrategy;
    sequence_barrier<spin_wait_strategy> consumed;
    ring_buffer<T> buffer;
};

template <typename Input, typename Output, typename Callable >
class MapJob final : public Job {
    public:
        const std::uint64_t kBatchSize = 1024;
        MapJob(std::uint64_t numThreads, 
                std::shared_ptr<WorkQueue<Input, single_threaded_claim_strategy>> input, 
                std::unique_ptr<WorkQueue<Output, multi_threaded_claim_strategy>> output, 
                Callable mapper) : 
            _threadState(numThreads),
            _input(std::move(input)), 
            _output(std::move(output)), 
            _mapper(std::forward<Callable>(mapper)),
            _nextReadIdx(-kBatchSize), // TODO This seems bad
            _lastReadIdx(kBatchSize) {}
        struct ThreadState {
            std::optional<sequence_t> nextReadIdx{std::nullopt};
        };

        void poll(std::uint64_t threadId, Milliseconds deadline) override {
            auto &myThreadState = _threadState[threadId];

            auto nextReadAt = [&] {
                if (myThreadState.nextReadIdx) {
                    std::cout << threadId << ": using cached value" << std::endl;
                    return *myThreadState.nextReadIdx; 
                } else  {
                    // Atomically get next sequence number to read from
                    sequence_t prevReadAt = _nextReadIdx.load(std::memory_order_relaxed); 
                    sequence_t nextReadAt = prevReadAt + kBatchSize;
                    while (!_nextReadIdx.compare_exchange_strong(prevReadAt, nextReadAt)){
                        nextReadAt = prevReadAt + kBatchSize;
                    }
                    std::cout << "threadId: " << threadId 
                          << ", prevReadAt: " << prevReadAt
                          << ", nextReadAt: " << nextReadAt << std::endl;

                    return nextReadAt;
                }
            }();

            auto publish = [&] {
                std::optional<sequence_t> min;

                for (auto i =0ull; i < _threadState.size(); ++i) {
                    // TODO This has some probably thread unsafe shenanigans
                    auto n = _threadState[i].nextReadIdx; 
                    if (n && (!min || difference(*n, *min) < 0))
                        min = *n;
                }
                if (min) {
                    std::cout << "Publishing: " << *min << std::endl;
                    _input->consumed.publish(*min);
                }
            };
            sequence_t lastToRead = nextReadAt + kBatchSize;

            while (difference(nextReadAt, lastToRead) < 0) {
                std::cout << threadId << ": going to wait  for " << nextReadAt << std::endl;
                auto available = _input->claimStrategy.wait_until_published(nextReadAt, std::chrono::milliseconds(1));
                std::cout << threadId << ": done wait:  " << available << std::endl;
                // Timeout
                if (difference(available, nextReadAt) < 0) {

                    if (myThreadState.nextReadIdx && nextReadAt > myThreadState.nextReadIdx) {
                        publish();
                    }
                    myThreadState.nextReadIdx = nextReadAt;
                    std::cout << threadId << ": returning early" << std::endl;
                    return;
                }
                    std::cout << threadId << ": not returning early" << std::endl;

                auto end = std::min(lastToRead, available);
                do {
                    auto x = _input->buffer[nextReadAt];
                    auto out = _mapper(x);
                    // TODO: Put in output
                    std::cout << threadId << ": x: " << x << std::endl;

                    ++nextReadAt;
                } while (difference(nextReadAt, end) < 0);
            }
            myThreadState.nextReadIdx = std::nullopt;

            publish();
        }

    private:
        // TODO Cache align items
        std::vector<ThreadState> _threadState;
        std::shared_ptr<WorkQueue<Input, single_threaded_claim_strategy>> _input;
        std::unique_ptr<WorkQueue<Output, multi_threaded_claim_strategy>> _output;
        Callable _mapper;
        std::atomic<sequence_t> _nextReadIdx;
        std::atomic<sequence_t> _lastReadIdx;
};


int main() {
    std::cout << "hello world" << std::endl;
    const std::uint64_t kNumThreads{8};
    Scheduler scheduler;
    scheduler.addJob(std::make_unique<ExampleJob>("not synchronized"));
    scheduler.addJob(std::make_unique<SequentialJob>(std::make_unique<ExampleJob>("sync")));
    using Input = WorkQueue<double, single_threaded_claim_strategy>;
    using Output = WorkQueue<double, multi_threaded_claim_strategy>;
    auto input = std::make_shared<Input>(1024);
    
    auto output = std::make_unique<Output>(1024);
    auto f = [](double x) { return x * 2; };
    scheduler.addJob(std::make_unique<MapJob<double, double, decltype(f)>>(kNumThreads, 
                                              input, 
                                              std::move(output), 
                                              std::move(f)));
    scheduler.run(kNumThreads);
    for (auto i = 0ull; i < 1024*1024; ++i) {
        input->enqueue(i);
    }
    sleep(1);
    scheduler.shutdown();
    scheduler.join();
}
