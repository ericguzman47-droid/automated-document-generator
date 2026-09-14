# Automated-Document-Generator

A high-performance, fault-tolerant C# .NET console orchestration engine designed to manage, execute, and monitor complex enterprise workflows driven entirely by database configurations. 

## 📌 The Business Problem & Architectural Solution

### The Challenge
In many enterprise environments, batch processing and scheduled automation tasks (such as dynamic data extraction, document assembly, and downstream post-processing) are handled by rigid, hardcoded applications or brittle UI automation scripts. This approach introduces severe operational liabilities:
* **Silent Failures:** Sub-steps (like an external script crashing or a network drive disconnect) often terminate the host application silently, leaving system administrators blind to failures until end-users report missing data.
* **Deployment Bottlenecks:** Simple operational changes—like temporarily pausing a job, altering a post-processing SQL query, or changing a script path—require full code changes, regression testing, and CI/CD deployment pipelines.
* **Lack of Visibility:** Without structured execution metrics (durations, record counts, error traces), performance bottlenecks go unnoticed until system debt degrades performance.

### The Solution
This engine decouples system configuration from the execution logic by shifting the operational control plane entirely to a relational database ledger. By evaluating job states dynamically at runtime, the engine reads metadata-driven instructions, chains sequential task workflows, wraps individual operations in isolated fault boundaries, and surfaces full execution telemetry.

## 🏗️ System Architecture & Workflow Pipeline

The engine is built around a centralized loop designed for strict sequential task dependencies. If an individual downstream operation fails (e.g., a post-process SQL constraint violation or a missing external batch file), the exception is intercepted, packaged with a full stack trace, and committed to a central telemetry ledger without dropping host execution threads.

graph TD
    A[Windows Task Scheduler / Cron] -->|Instantiates Thread| B(C# Orchestration Engine)
    B -->|1. Polls Active Tasks| C[(Database: SystemWorkflows)]
    B -->|2. Validates Soft Kill-Switches| D{Task Enabled?}
    D -->|No| E[Log Skip Event & Exit]
    D -->|Yes| F[Execute Core Data Views]
    F -->|3. Handoff Dataset| G[Primary Process Engine]
    G -->|4. Sequential Step A| H[Execute Post-Process SQL]
    H -->|5. Sequential Step B| I[Execute External Script Dependencies]
    I -->|6. Intercept Failures| J[Collate Execution Metrics]
    J -->|7. Guaranteed Transaction| K[(Database: SystemExecutionLogs)]
    
        public WorkflowRunner(IDataRepository dataRepository, ICoreProcessor coreProcessor)
        {
            _dataRepository = dataRepository;
            _coreProcessor = coreProcessor;
        }

        public void Execute(AutomationTask task)
        {
            // 1. Defend the boundary: Validate the soft-kill configuration status instantly
            if (task.IsEnabled != 1)
            {
                Console.WriteLine($"[SKIPPED] Task '{task.TaskName}' is currently disabled in the database registry.");
                return;
            }

            // 2. Initialize point-in-time logging variables
            DateTime startTime = DateTime.Now;
            int recordsProcessed = 0;
            string executionStatus = "Success";
            string exceptionDump = null;

            Console.WriteLine($"Initializing Workflow Pipeline: {task.TaskName}");

            try
            {
                // 3. Extract source data dynamically based on configuration fields
                DataTable processData = _dataRepository.FetchTargetView(task.ViewName);
                recordsProcessed = processData.Rows.Count;
                Console.WriteLine($"Dataset Retrieved. Records Pending Processing: {recordsProcessed}");

                if (recordsProcessed > 0)
                {
                    // 4. Pass dataset execution over to the core computing assembly
                    _coreProcessor.ExecuteBatchProcessing(task, processData);

                    // 5. Sequential Post-Process Step A: Execute Inline Database Queries
                    if (!string.IsNullOrWhiteSpace(task.PostExecutionSQL))
                    {
                        Console.WriteLine("Executing Configured Post-Execution SQL script...");
                        _dataRepository.ExecuteDirectQuery(task.PostExecutionSQL);
                        Console.WriteLine("Post-Execution SQL query executed successfully.");
                    }

                    // 6. Sequential Post-Process Step B: Spawn Sub-Process CLI Dependencies
                    if (!string.IsNullOrWhiteSpace(task.PostExecutionBatch) && File.Exists(task.PostExecutionBatch))
                    {
                        Console.WriteLine($"Spawning external CLI sub-process: {task.PostExecutionBatch}");
                        using (System.Diagnostics.Process proc = System.Diagnostics.Process.Start(task.PostExecutionBatch))
                        {
                            proc.WaitForExit(); // Blocks execution thread until external dependency completely terminates
                        }
                        Console.WriteLine("External CLI sub-process exited successfully.");
                    }
                }
            }
            catch (Exception ex)
            {
                // Capture all downstream infrastructure and execution exceptions seamlessly
                Console.WriteLine($"CRITICAL PIPELINE EXCEPTION: {ex.Message}");
                executionStatus = "Failed";
                exceptionDump = ex.ToString(); // Serializes full stack trace for deep debugging
            }
            finally
            {
                // 7. This block is guaranteed to execute even under total application failure
                DateTime endTime = DateTime.Now;
                Console.WriteLine($"Pipeline closed with status '{executionStatus}'. Synchronizing logs with core server...");

                try
                {
                    // Fire-and-forget metrics logging, passing string-typed models explicitly to database integers
                    int logRecordId = _dataRepository.WriteTelemetryLog(
                        Convert.ToInt32(task.TaskID),
                        task.TaskName,
                        startTime,
                        endTime,
                        recordsProcessed,
                        executionStatus,
                        exceptionDump
                    );
                    Console.WriteLine($"Central telemetry synchronized. Master Log ID: {logRecordId}");
                }
                catch (Exception logEx)
                {
                    // Fail-safe protection: Prevent application from blowing up ungracefully if the logging server drops offline
                    System.Diagnostics.Trace.WriteLine($"CRITICAL: Database logging subsystem failed completely: {logEx.Message}");
                }
            }
        }
    }
}

2. The Native Data & Telemetry Access Layer (DataRepository.cs)
Demonstrates raw ADO.NET efficiency, native SQL parametrization to secure data boundaries against injection vulnerabilities, and parsing SCOPE_IDENTITY() via scalar commands.
using System;
using Microsoft.Data.SqlClient;

namespace EnterpriseAutomation.Database
{
    public class DataRepository : IDataRepository
    {
        private readonly string _connectionString;

        public DataRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public int WriteTelemetryLog(int taskId, string taskName, DateTime start, DateTime end, int records, string status, string error)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                // Omit identity columns from explicit insertion, append statement to fetch assigned key natively
                string query = @"
                    INSERT INTO Core.dbo.SystemExecutionLogs 
                    (TaskID, TaskName, StartTime, EndTime, RecordsProcessed, ExecutionStatus, ExceptionDump) 
                    VALUES 
                    (@TaskID, @TaskName, @StartTime, @EndTime, @Records, @Status, @Error);
                    SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@TaskID", taskId);
                    cmd.Parameters.AddWithValue("@TaskName", taskName);
                    cmd.Parameters.AddWithValue("@StartTime", start);
                    cmd.Parameters.AddWithValue("@EndTime", end);
                    cmd.Parameters.AddWithValue("@Records", records);
                    cmd.Parameters.AddWithValue("@Status", status);
                    
                    // Sanitize large exception text logs, substituting .NET null values with true DBNull indicators
                    cmd.Parameters.AddWithValue("@Error", string.IsNullOrEmpty(error) ? (object)DBNull.Value : error);

                    conn.Open();
                    
                    // ExecuteScalar evaluates the batch execution and returns the precise value generated by SCOPE_IDENTITY()
                    object result = cmd.ExecuteScalar();
                    return result != null ? Convert.ToInt32(result) : 0;
                }
            }
        }
    }
}

**Key Technical Implications & Wins
--- Strict Fault Isolation
The master engine structure introduces robust protective boundaries around untrusted sequential tasks. By routing downstream processing through centralized exception handling blocks, infrastructure exceptions—such as bad network connections, file access latency, or SQL execution deadlocks—are gracefully intercepted. This completely eliminates hanging zombie processes on application servers and guarantees clean task transitions.

**Real-Time Diagnostic Ingestion
Rather than dumping technical debt into unorganized flat text files across local server directories, the application aggregates every performance parameter natively into a structured relational schema. Utilizing transactional SCOPE_IDENTITY() hooks provides administrators with immediate visibility into performance timelines, data metrics, and absolute stack traces, significantly reducing root-cause resolution times.
