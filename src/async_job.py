from maap.maap import MAAP
maap = MAAP()
import asyncio

class AsyncJob:
    
    def __init__(self, job_id):
        self.job_id = job_id
        self.status = "..."
        self.status_changed = asyncio.Event()
    
    async def check_status(self):
        job_status = maap.getJobStatus(self.job_id)
    
        if self.status != job_status:
            print(f"Status changed to {job_status}")
            self.status = job_status
            self.status_changed.set()
    
        if self.status not in {"Accepted", "Running"}:
            self.status = "job completed"

    async def wait_for_status_change(self):
        print("Waiting for status to change...")
        await self.status_changed.wait()
        self.status_changed.clear()
        if self.status != "job completed":
            await self.wait_for_status_change()
    
    async def get_job_status(self):
        print(f"job id: {self.job_id}")    
        wait_task = asyncio.create_task(self.wait_for_status_change())
    
        while self.status != "job completed":
            await asyncio.sleep(10)
            await self.check_status()
