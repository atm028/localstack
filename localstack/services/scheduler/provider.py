import logging
import threading

from localstack.aws.api.scheduler import (
    SchedulerApi,
    ListSchedulesResult,
    CreateScheduleResult,
    SchedulerResult,
    String,
    Boolean,
    CreateScheduleRequest
)
from localstack.services.scheduler.models import (
    schedulers_store,
    SchedulerStore,
    Scheduler
)
from localstack.services.plugins import ServiceLifecycleHook
from localstack.aws.accounts import get_aws_account_id
from localstack.utils.run import FuncThread
from localstack.aws.api import RequestContext, handler
from localstack.utils.aws import aws_stack
from localstack.services.moto import call_moto

from typing import Dict, List, Optional, Tuple, Any

LOG = logging.getLogger(__name__)

class SchedulerProvider(SchedulerApi, ServiceLifecycleHook):
    def __init__(self):
        super().__init__()
        self._mutex = threading.RLock()
        self.thread: Optional[FuncThread] = None

    @handler("ListSchedules", expand=False)
    def list_schedules(self,
                       context: RequestContext,
                       group_name: String = None
                       ) -> ListSchedulesResult:
        store = self.get_store(context.account_id, context.region)
        res = [SchedulerResult(
            Arn=s.arn,
            CreationDate=s.creation_date,
            Name=s.name,
            State=s.state
        ) for s in store.schedulers.values()]
        return ListSchedulesResult(SchedulerList=res)

    @handler("CreateSchedule", expand=False)
    def create_schedule(self,
                        context: RequestContext,
                        request: CreateScheduleRequest
                        ) -> CreateScheduleResult:
        name = request['Name']

        store = self.get_store(context.account_id, context.region)
        with self._mutex:
            if name not in store.schedulers:
                scheduler = Scheduler(name, context.region, context.account_id, request)
                LOG.debug("Create scheduler with name: " + name)
                store.schedulers[name] = scheduler
        response: CreateScheduleResult = call_moto(context)
        return response

    @staticmethod
    def get_store(account_id: String = None, region: String = None) -> SchedulerStore:
        return schedulers_store[account_id or get_aws_account_id()][region or aws_stack.get_region()]