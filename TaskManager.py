# FileName: TaskManager.py
import asyncio
import functools
import inspect
import enum
from dataclasses import dataclass, field
from typing import Optional, Callable, Any, List, Dict

import logging

# --- Define SHARED Components ---

class Priority(enum.IntEnum):
    """Priority for tasks. Lower value means higher priority."""
    HIGH = 1
    NORMAL = 5
    LOW = 10

@dataclass(order=True)
class _ProcessingTask:
    """A task to be placed in the priority queue."""
    priority: int
    future: asyncio.Future = field(compare=False)
    func: Callable = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: dict = field(compare=False)

# --- The Generic Worker Pool Class (Building Block) ---

class ProcessingManager:
    """
    A generic, self-contained scheduler for a specific type of background task.
    This acts as a single, independent "worker pool".
    """
    def __init__(
        self,
        name: str,
        max_background_workers: int,
        max_concurrent_streams: int = 0
    ):
        self.name = name
        self.logger = logging.getLogger(f"{name}Scheduler")
        self.logger.setLevel(logging.DEBUG) # <-- FIX: Was using 'logger' instead of 'self.logger'
        self.logger.info(
            f"Initializing manager '{name}': {max_background_workers} workers, "
            f"{max_concurrent_streams} streaming slots."
        )
        self.max_workers = max_background_workers
        self.queue = asyncio.PriorityQueue()

        if max_concurrent_streams > 0:
            self.stream_semaphore = asyncio.Semaphore(max_concurrent_streams)
        else:
            self.stream_semaphore = None

        self._workers: List[asyncio.Task] = []
        self._worker_prefix = f"{name.capitalize()}Worker"

    async def _worker(self, worker_name: str):
        self.logger.info(f"Worker '{worker_name}' started.")
        while True:
            try:
                task: _ProcessingTask = await self.queue.get()
                self.logger.info(f"Worker '{worker_name}' picked up task (Prio: {task.priority}) for: {task.func.__name__}")
                try:
                    if not asyncio.iscoroutinefunction(task.func):
                         raise TypeError(f"Task function {task.func.__name__} for manager {self.name} must be a coroutine.")

                    result = await task.func(*task.args, **task.kwargs)
                    task.future.set_result(result)

                except Exception as e:
                    self.logger.exception(f"Worker '{worker_name}' failed task: {task.func.__name__}")
                    task.future.set_exception(e)
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                self.logger.info(f"Worker '{worker_name}' shutting down.")
                break
            except Exception:
                self.logger.exception(f"Critical error in worker '{worker_name}'.")


    def start(self):
        if not self._workers and self.max_workers > 0:
            self.logger.info(f"Starting {self.max_workers} background workers...")
            for i in range(self.max_workers):
                worker_task = asyncio.create_task(self._worker(f"{self._worker_prefix}-{i+1}"))
                self._workers.append(worker_task)

    async def stop(self):
        if not self._workers: return
        self.logger.info(f"Stopping all workers for manager '{self.name}'...")
        for worker in self._workers: worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        self.logger.info(f"All workers for manager '{self.name}' have shut down.")

    async def schedule_task(self, func: Callable, args: tuple, kwargs: dict, priority: Priority) -> Any:
        if self.max_workers <= 0:
            msg = f"Cannot schedule task for '{self.name}', no workers are configured."
            self.logger.error(msg)
            raise RuntimeError(msg)
        future = asyncio.get_running_loop().create_future()
        task = _ProcessingTask(priority=priority.value, future=future, func=func, args=args, kwargs=kwargs)
        await self.queue.put(task)
        self.logger.debug(f"Queued task (Prio: {priority.name}) for: {func.__name__}")
        return await future

    async def join(self):
        """Waits until the manager's queue is empty and all tasks are processed."""
        self.logger.info(f"Waiting for all tasks in manager '{self.name}' to complete...")
        await self.queue.join()
        self.logger.info(f"All tasks for manager '{self.name}' are complete.")


# --- The System Controller Class ---
class TaskManagerRegistry:
    """A singleton to create, manage, and access all named worker pools."""
    def __init__(self):
        self._managers: Dict[str, ProcessingManager] = {}
        self.logger = logging.getLogger("TaskManagerRegistry")
        self.logger.setLevel(logging.DEBUG) # <-- FIX: Was using 'logger' instead of 'self.logger'


    def clear(self):
        """Removes all registered manager instances."""
        if self._managers:
            self.logger.info("Clearing all existing task manager instances.")
            self._managers.clear()
            
    def configure_from_dict(self, config: Dict[str, Dict]):
        self.logger.info("Configuring task managers from dictionary...")
        for name, settings in config.items():
            if name in self._managers:
                self.logger.warning(f"Manager '{name}' already configured. Skipping.")
                continue

            self._managers[name] = ProcessingManager(
                name=name,
                max_background_workers=settings.get("workers", 0),
                max_concurrent_streams=settings.get("streams", 0)
            )

    def get_manager(self, name: str) -> ProcessingManager:
        manager = self._managers.get(name)
        if manager is None:
            raise RuntimeError(f"No manager named '{name}' is registered. Available: {list(self._managers.keys())}")
        return manager

    def start_all(self):
        self.logger.info("Starting all registered task managers...")
        for manager in self._managers.values():
            manager.start()

    async def stop_all(self):
        self.logger.info("Stopping all registered task managers...")
        stop_tasks = [manager.stop() for manager in self._managers.values()]
        await asyncio.gather(*stop_tasks)
        self.logger.info("All task managers have been shut down.")

    async def join_all(self):
        """Waits for all registered managers to finish their work."""
        self.logger.info("Waiting for all task managers to become idle...")
        join_tasks = [manager.join() for manager in self._managers.values()]
        await asyncio.gather(*join_tasks)
        self.logger.info("All task managers are now idle.")

    # --- DECORATOR METHODS ---
    def queue_task(self, manager_name: str, priority: Priority = Priority.NORMAL):
        """
        Decorator to queue a function on a specific named manager.
        Usage: @registry.queue_task("image", priority=Priority.HIGH)
        """
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                manager = self.get_manager(manager_name)
                # The task function itself should be a coroutine now
                return await manager.schedule_task(func, args, kwargs, priority)
            return wrapper
        return decorator

    def limit_concurrency(self, manager_name: str):
        """
        Decorator to limit concurrency using a specific manager's semaphore.
        Correctly handles both regular async functions and async generators.
        """
        def decorator(func):
            if inspect.isasyncgenfunction(func):
                @functools.wraps(func)
                async def generator_wrapper(*args, **kwargs):
                    manager = self.get_manager(manager_name)
                    if manager.stream_semaphore is None:
                        raise TypeError(f"Manager '{manager_name}' has no concurrency limit configured (streams=0).")
                    async with manager.stream_semaphore:
                        async for item in func(*args, **kwargs):
                            yield item
                return generator_wrapper
            else:
                @functools.wraps(func)
                async def regular_wrapper(*args, **kwargs):
                    manager = self.get_manager(manager_name)
                    if manager.stream_semaphore is None:
                        raise TypeError(f"Manager '{manager_name}' has no concurrency limit configured (streams=0).")
                    async with manager.stream_semaphore:
                        return await func(*args, **kwargs)
                return regular_wrapper
        return decorator

task_registry = TaskManagerRegistry()