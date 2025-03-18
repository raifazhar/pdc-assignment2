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
    stop = false;
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
    stop = true;
    readyCondition.notify_all();
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
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        waitingTasks.push_back(WaitingTaskStruct {
            currentTask,
            deps,
            num_total_tasks,
            runnable
        });
    }

    AdjustDeps();

    return currentTask++;
}   

void TaskSystemParallelThreadPoolSleeping::sync() {
    if (readyTasks.size() == 0 && waitingTasks.size() == 0 && runningTasks.size() == 0){
        return;
    }
    std::unique_lock<std::mutex> lock(queueMutex);

    syncCondition.wait(lock, [this]{
        return waitingTasks.empty() && readyTasks.empty() && runningTasks.empty();
    });
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        ReadyTaskStruct readyTaskStruct;
        TaskID taskID;
        int subTaskIndex;
        IRunnable *runnable;
        int numTotalTasks;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            readyCondition.wait(lock, [this] { return stop || !readyTasks.empty(); });

            if (stop) return;
            readyTaskStruct = readyTasks[0];

            taskID = readyTasks[0].taskID;
            subTaskIndex = readyTasks[0].subTaskIndex;
            runnable = readyTasks[0].runnable;
            numTotalTasks = readyTasks[0].numTotalTasks;

            readyTasks.pop_front();
        
            //add to running vector
            runningTasks.push_back(readyTaskStruct);
        }

        // Execute the task outside the lock
        runnable->runTask(subTaskIndex, numTotalTasks);

        bool earlyBreak = false;
        {
            // Delete it from the running Tasks and Check if all tasks for this taskID are done
            std::lock_guard<std::mutex> lock(queueMutex);
            runningTasks.erase(std::remove(runningTasks.begin(), runningTasks.end(), readyTaskStruct), runningTasks.end());
            

            for (int i = 0; i < runningTasks.size(); i++){
                if (runningTasks[i].taskID == taskID){
                    //some function of this task id is still running
                    earlyBreak = true;
                    break;
                }
            }
        
            if(!earlyBreak){
                //the taskID was not in running tasks

                for(int i = 0; i<readyTasks.size(); i++){
                    if (readyTasks[i].taskID == taskID){
                        earlyBreak = true;
                        break;
                    }
                }
            }

            if (!earlyBreak){
                // the task ID was in neither the running nor the ready tasks
                //so it completed
                completedTasks.push_back(taskID);
            }

            if (waitingTasks.empty() && readyTasks.empty() && runningTasks.empty()){
                syncCondition.notify_all();

                break;
            }
        }
        if (!earlyBreak){
            AdjustDeps();
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::ConvertToReady() {
    {
        std::lock_guard<std::mutex> lock(queueMutex); 
        
        for (int i = 0; i < waitingTasks.size(); i++){
            if (waitingTasks[i].deps.empty()){
                //switch to running

                for (int j = 0; j < waitingTasks[i].numTotalTasks; j++){
                    readyTasks.push_back(ReadyTaskStruct{
                        waitingTasks[i].taskID,
                        waitingTasks[i].numTotalTasks,
                        j,
                        waitingTasks[i].runnable
                    });
                }

                waitingTasks.erase(waitingTasks.begin() + i);
                i--;
            }
        }
    }
    readyCondition.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::AdjustDeps() {
    bool taskDepsEmpty = false;
    {
        std::lock_guard<std::mutex> lock(queueMutex);

        for (int j = 0; j < waitingTasks.size(); j++){
            if (waitingTasks[j].deps.empty()){
                taskDepsEmpty = true;
                continue;
            }
            for (int i = 0; i < completedTasks.size(); i++){
                for (int k = 0; k < waitingTasks[j].deps.size(); k++){
                    if (completedTasks[i] == waitingTasks[j].deps[k]){
                        //the task has been complted to remove the dependency
                        waitingTasks[j].deps.erase(waitingTasks[j].deps.begin() + k);
                        k--;

                        if (waitingTasks[j].deps.empty()){
                            taskDepsEmpty = true;
                        }
                    }
                }
            }
        }
    }

    if (taskDepsEmpty){
        ConvertToReady();
    }
}

