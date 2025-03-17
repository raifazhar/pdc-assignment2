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

    std::lock_guard<std::mutex> lock(queueMutex);
    waitingQueue.push_back(WaitingTaskStruct {
        currentTask,
        deps,
        num_total_tasks,
        runnable
    });

    if (deps.empty()){
        ConvertToReady();
    }
    return currentTask++;
}   

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(queueMutex);
    syncCondition.wait(lock, [this] { return waitingQueue.empty() && readyQueue.empty() && activeTasks == 0; });
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        std::function<void()> task;
        TaskID taskID;
        bool taskGotReady = false;
        bool taskGotCompleted = false;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            condition.wait(lock, [this] { return stop || !readyQueue.empty(); });

            if (stop && readyQueue.empty()) return;

            ReadyTaskStruct& readyTaskStruct = readyQueue.front(); // Get a reference to the front element
            taskID = readyTaskStruct.taskID; // Get the taskID

            // Create the task as a lambda
            task = [taskID, runnable = readyTaskStruct.runnable, numTotalTasks = readyTaskStruct.numTotalTasks, remainingTasks = readyTaskStruct.remainingTasks]() {
                int i = numTotalTasks - remainingTasks;
                runnable->runTask(i, numTotalTasks);
            };
            // Decrement remainingTasks in the queue
            readyTaskStruct.remainingTasks--;

            // If remainingTasks reaches 0, pop the element from the queue
            if (readyTaskStruct.remainingTasks == 0) {
                readyQueue.pop();
                taskGotCompleted = true;
            }
            
            // Remove from readyQueue
            activeTasks++;
        }

        // Execute the task outside the lock
        task();

        // Check if all tasks for this taskID are done
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            activeTasks--;
            

            if (taskGotCompleted) {  // No more tasks with this TaskID
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
                syncCondition.notify_all();
            }
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::ConvertToReady() {
    for (auto it = waitingQueue.begin(); it != waitingQueue.end(); ) {
        if (it->deps.empty()) {
            readyQueue.push(ReadyTaskStruct{
                it->taskID,
                it->numTotalTasks,
                it->numTotalTasks,
                it->runnable
            });
            condition.notify_all();
            it = waitingQueue.erase(it);  // Remove from waitingQueue after moving tasks
        } else {
            ++it;
        }
    }
}

