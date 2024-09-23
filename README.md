Code Overview:
Database Models (SQLAlchemy): Customer and Campaign models represent customer and marketing campaign data.
FastAPI Endpoints:
POST /webhook: For receiving webhook data.
GET /data: Retrieve data with pagination using offset and limit.
GET /sync/{source}: Trigger synchronization from the specified external API (CRM or Marketing).
GET /tasks: List all running background tasks.
POST /tasks/cancel: Cancel a running task.
Background Tasks: Background sync tasks are handled using FastAPI’s BackgroundTasks class.
Asynchronous HTTP Requests: httpx is used for asynchronous HTTP requests to external APIs.
Task Management: An in-memory dictionary (tasks) is used to track the status of background tasks.
Logging: Errors and events are logged using Python’s built-in logging module.
Running the Project:
Run the application:
uvicorn filename:app --reload
Testing Endpoints:
Use tools like Postman or cURL to test endpoints like /sync/{source} or /data.
Make sure to adjust the file names and paths accordingly when using Docker or SQLite

