#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    currentTask = 0;
    activeTasks = 0;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    {
        std::unique_lock<std::mutex> lock(queueMutex);
        stop = true;
        while (!readyQueue.empty()) {
            readyQueue.pop();  // Clear all remaining tasks
        }
    }
    condition.notify_all();
    for (std::thread &thread : threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    
    std::queue<std::function<void()>> taskQueue;

    for (int i = 0;i < num_total_tasks; i++){
        taskQueue.push([runnable, i, num_total_tasks](){
            runnable->runTask(i, num_total_tasks);
        });
    }

    std::lock_guard<std::mutex> lock(queueMutex);
    waitingQueue.push_back(WaitingTaskStruct {
        currentTask,
        deps,
        taskQueue
    });

    if (deps.empty()){
        ConvertToReady();
    }
    return currentTask++;
}   

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(queueMutex);
    condition.wait(lock, [this] { return waitingQueue.empty() && readyQueue.empty(); });
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        std::function<void()> task;
        TaskID taskID;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            condition.wait(lock, [this] { return stop || !readyQueue.empty(); });

            if (stop && readyQueue.empty()) return;

            // Extract and remove a task
            taskID = readyQueue.front().taskID;
            task = std::move(readyQueue.front().task); // Take the task
            readyQueue.pop(); // Remove from readyQueue
            activeTasks++;
        }

        // Execute the task outside the lock
        task();

        // Check if all tasks for this taskID are done
        bool taskGotReady = false;
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            activeTasks--;
            
            bool isAnyOtherTaskForThisTaskID = false;

            std::queue<ReadyTaskStruct> tempQueue = readyQueue;

            while (!tempQueue.empty()) {
                if (tempQueue.front().taskID == taskID) {
                    isAnyOtherTaskForThisTaskID = true; // Found the taskID
                }
                tempQueue.pop(); // Move to next element
            }

            if (!isAnyOtherTaskForThisTaskID) {  // No more tasks with this TaskID
                for (auto& taskStruct : waitingQueue) {
                    auto& deps = taskStruct.deps;
                    size_t beforeSize = deps.size();
                    
                    // Remove taskID from dependencies
                    deps.erase(std::remove(deps.begin(), deps.end(), taskID), deps.end());

                    if (beforeSize > 0 && deps.empty()) {  // If it was dependent and now isn't
                        taskGotReady = true;
                    }
                }
            }

            if (taskGotReady) {
                ConvertToReady();
            }


            if (readyQueue.empty() && waitingQueue.empty() && activeTasks == 0){
                condition.notify_all();
            }
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::ConvertToReady() {
    for (auto it = waitingQueue.begin(); it != waitingQueue.end(); ) {
        if (it->deps.empty()) {
            // Move each subtask individually to the map under the correct TaskID
            while (!it->taskArray.empty()) {
                readyQueue.push(ReadyTaskStruct{it->taskID, std::move(it->taskArray.front())});
                it->taskArray.pop();
                condition.notify_one();  // Wake up a worker thread
            }            
            it = waitingQueue.erase(it);  // Remove from waitingQueue after moving tasks
        } else {
            ++it;
        }
    }    
}

