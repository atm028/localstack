import logging

from datetime import datetime

from localstack.services.stores import AccountRegionBundle, BaseStore, LocalAttribute
from localstack.aws.api.scheduler import String
from localstack.aws.connect import connect_to

from typing import Dict
import uuid
import collections.abc

LOG = logging.getLogger(__name__)

class Scheduler:
    def __init__(self,
                name: String,
                region: String,
                account_id: String,
                attributes=None,
                tags=None) -> None:
        self.name = name
        self.arn = None
        self.creation_date = None
        self.group_name = None
        self.last_modification_date = None
        self.state = None
        self.target = {}
        self.region = region
        self.account_id = account_id
        self.tags = tags
        self.attributes = self.default_attributes()
        if attributes:
            self.attributes.update(attributes)

        events = connect_to(aws_session_token=account_id, region_name=region).events
        scheduleExpression = attributes['ScheduleExpression']
        if "rate" in scheduleExpression or "cron" in scheduleExpression:
            scheduleExpression = scheduleExpression
        else:
            scheduleExpression = self.at2cron(scheduleExpression)
        target = attributes['Target']

        queue_target_id = f"target-{self.short_uuid()}"
        if isinstance(target, collections.abc.Sequence):
            targets = [{"Id": queue_target_id, "Arn": s['Arn'], "Input":s['Input']} for s in target]
        else:
            targets = [{"Id": queue_target_id, "Arn": target['Arn'], "Input": target['Input']}]

        LOG.debug("=======================")
        LOG.debug(scheduleExpression)
        LOG.debug(targets)
        LOG.debug("=======================")

        events.put_rule(Name=name, ScheduleExpression=scheduleExpression)
        events.put_targets(Rule=name, Targets=targets)
    def default_attributes(self):
        return {};

    def short_uuid(self) -> str:
        return str(uuid.uuid4())[:8]

    def at2cron(self, at: String):
        at = datetime.strptime(at.replace("at", "").replace("(", "").replace(")", ""), "%Y-%m-%dT%H:%M:%S")
        return f"cron({at.minute} {at.hour} {at.day} {at.month} ? {at.year})"


class SchedulerStore(BaseStore):
    schedulers: Dict[str, Scheduler] = LocalAttribute(default=dict)
    deleted: Dict[str, float] = LocalAttribute(default=dict)

    def expire_deleted(self):
        pass

schedulers_store = AccountRegionBundle("scheduler", SchedulerStore)