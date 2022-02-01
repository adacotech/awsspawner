import datetime

import boto3
import traitlets
from jupyterhub.spawner import Spawner
from tornado import gen
from tornado.concurrent import Future
from traitlets import Bool, Instance, List, TraitType, Type, Unicode, default
from traitlets.config.configurable import Configurable


class Datetime(TraitType):
    klass = datetime.datetime
    default_value = datetime.datetime(1900, 1, 1)


class AWSSpawnerAuthentication(Configurable):
    def get_session(self, region) -> boto3.Session:
        return boto3.Session(region_name=region)


class AWSSpawnerSecretAccessKeyAuthentication(AWSSpawnerAuthentication):

    aws_access_key_id = Unicode(config=True)
    aws_secret_access_key = Unicode(config=True)

    def get_session(self, region) -> boto3.Session:
        return boto3.Session(region_name=region, aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)


class AWSSpawner(Spawner):

    aws_region = Unicode(config=True)
    launch_type = Unicode(config=True)
    assign_public_ip = Bool(False, config=True)
    task_role_arn = Unicode(config=True)
    task_cluster_name = Unicode(config=True)
    task_container_name = Unicode(config=True)
    task_definition_arn = Unicode(config=True)
    task_security_groups = List(trait=Unicode, config=True)
    task_subnets = List(trait=Unicode, config=True)
    notebook_scheme = Unicode(config=True)
    notebook_args = List(trait=Unicode, config=True)
    args_join = Unicode(config=True)

    authentication_class = Type(AWSSpawnerAuthentication, config=True)
    authentication = Instance(AWSSpawnerAuthentication)

    @default("authentication")
    def _default_authentication(self):
        return self.authentication_class(parent=self)

    task_arn = Unicode("")

    # We mostly are able to call the AWS API to determine status. However, when we yield the
    # event loop to create the task, if there is a poll before the creation is complete,
    # we must behave as though we are running/starting, but we have no IDs to use with which
    # to check the task.
    calling_run_task = Bool(False)

    progress_buffer = None

    def load_state(self, state):
        """Misleading name: this "loads" the state onto self, to be used by other methods"""

        super().load_state(state)

        # Called when first created: we might have no state from a previous invocation
        self.task_arn = state.get("task_arn", "")

    def get_state(self):
        """Misleading name: the return value of get_state is saved to the database in order
        to be able to restore after the hub went down"""

        state = super().get_state()
        state["task_arn"] = self.task_arn

        return state

    def poll(self):
        # Return values, as dictacted by the Jupyterhub framework:
        # 0                   == not running, or not starting up, i.e. we need to call start
        # None                == running, or not finished starting
        # 1, or anything else == error

        return (
            None
            if self.calling_run_task
            else 0
            if self.task_arn == ""
            else None
            if (_get_task_status(self.log, self._aws_endpoint(), self.task_cluster_name, self.task_arn)) in ALLOWED_STATUSES
            else 1
        )

    async def start(self):
        self.log.debug("Starting spawner")

        task_port = self.port
        session = self.authentication.get_session(self.aws_region)

        self.progress_buffer.write({"progress": 0.5, "message": "Starting server..."})
        try:
            self.calling_run_task = True
            args = self.get_args() + self.notebook_args
            run_response = _run_task(
                self.log,
                session,
                self.launch_type,
                self.assign_public_ip,
                self.task_role_arn,
                self.task_cluster_name,
                self.task_container_name,
                self.task_definition_arn,
                self.task_security_groups,
                self.task_subnets,
                self.cmd + args,
                self.get_env(),
                self.args_join,
            )
            task_arn = run_response["tasks"][0]["taskArn"]
            self.progress_buffer.write({"progress": 1})
        finally:
            self.calling_run_task = False

        self.task_arn = task_arn

        max_polls = 50
        num_polls = 0
        task_ip = ""
        while task_ip == "":
            num_polls += 1
            if num_polls >= max_polls:
                raise Exception("Task {} took too long to find IP address".format(self.task_arn))

            task_ip = _get_task_ip(self.log, session, self.task_cluster_name, task_arn)
            await gen.sleep(1)
            self.progress_buffer.write({"progress": 1 + num_polls / max_polls})

        self.progress_buffer.write({"progress": 2})

        max_polls = self.start_timeout
        num_polls = 0
        status = ""
        while status != "RUNNING":
            num_polls += 1
            if num_polls >= max_polls:
                raise Exception("Task {} took too long to become running".format(self.task_arn))

            status = _get_task_status(self.log, session, self.task_cluster_name, task_arn)
            if status not in ALLOWED_STATUSES:
                raise Exception("Task {} is {}".format(self.task_arn, status))

            await gen.sleep(1)
            self.progress_buffer.write({"progress": 2 + num_polls / max_polls * 98})

        self.progress_buffer.write({"progress": 100, "message": "Server started"})
        await gen.sleep(1)

        self.progress_buffer.close()

        return f"{self.notebook_scheme}://{task_ip}:{task_port}"

    async def stop(self, now=False):
        if self.task_arn == "":
            return
        session = self.authentication.get_session(self.aws_region)

        self.log.debug("Stopping task (%s)...", self.task_arn)
        _ensure_stopped_task(self.log, session, self.task_cluster_name, self.task_arn)
        self.log.debug("Stopped task (%s)... (done)", self.task_arn)

    def clear_state(self):
        super().clear_state()
        self.log.debug("Clearing state: (%s)", self.task_arn)
        self.task_arn = ""
        self.progress_buffer = AsyncIteratorBuffer()

    async def progress(self):
        async for progress_message in self.progress_buffer:
            yield progress_message


ALLOWED_STATUSES = ("", "PROVISIONING", "PENDING", "RUNNING")


def _ensure_stopped_task(_, session, task_cluster_name, task_arn):
    client = session.client("ecs")
    client.stop_task(cluster=task_cluster_name, task=task_arn)


def _get_task_ip(logger, session, task_cluster_name, task_arn):
    described_task = _describe_task(logger, session, task_cluster_name, task_arn)

    ip_address_attachements = (
        [attachment["value"] for attachment in described_task["attachments"][0]["details"] if attachment["name"] == "privateIPv4Address"]
        if described_task and "attachments" in described_task and described_task["attachments"]
        else []
    )
    ip_address = ip_address_attachements[0] if ip_address_attachements else ""
    return ip_address


def _get_task_status(logger, session, task_cluster_name, task_arn):
    described_task = _describe_task(logger, session, task_cluster_name, task_arn)
    status = described_task["lastStatus"] if described_task else ""
    return status


def _describe_task(_, session, task_cluster_name, task_arn):
    client = session.client("ecs")

    described_tasks = client.describe_tasks(cluster=task_cluster_name, tasks=[task_arn])

    # Very strangely, sometimes 'tasks' is returned, sometimes 'task'
    # Also, creating a task seems to be eventually consistent, so it might
    # not be present at all
    task = (
        described_tasks["tasks"][0]
        if "tasks" in described_tasks and described_tasks["tasks"]
        else described_tasks["task"]
        if "task" in described_tasks
        else None
    )
    return task


def _run_task(
    _,
    session,
    launch_type,
    assign_public_ip,
    task_role_arn,
    task_cluster_name,
    task_container_name,
    task_definition_arn,
    task_security_groups,
    task_subnets,
    task_command_and_args,
    task_env,
    args_join="",
):
    if args_join != "":
        task_command_and_args = [args_join.join(task_command_and_args)]

    client = session.client("ecs")

    dict_data = {
        "cluster": task_cluster_name,
        "taskDefinition": task_definition_arn,
        "overrides": {
            "taskRoleArn": task_role_arn,
            "containerOverrides": [
                {
                    "command": task_command_and_args,
                    "environment": [
                        {
                            "name": name,
                            "value": value,
                        }
                        for name, value in task_env.items()
                    ],
                    "name": task_container_name,
                }
            ],
        },
        "count": 1,
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "assignPublicIp": "ENABLED" if assign_public_ip else "DISABLED",
                "securityGroups": task_security_groups,
                "subnets": task_subnets,
            },
        },
    }

    if launch_type != traitlets.Undefined:
        if launch_type == "FARGATE_SPOT":
            dict_data["capacityProviderStrategy"] = [{"base": 1, "capacityProvider": "FARGATE_SPOT", "weight": 1}]
        else:
            dict_data["launchType"] = launch_type
    return client.run_task(**dict_data)


class AsyncIteratorBuffer:
    # The progress streaming endpoint may be requested multiple times, so each
    # call to `__aiter__` must return an iterator that starts from the first message

    class _Iterator:
        def __init__(self, parent):
            self.parent = parent
            self.cursor = 0

        async def __anext__(self):
            future = self.parent.futures[self.cursor]
            self.cursor += 1
            return await future

    def __init__(self):
        self.futures = [Future()]

    def __aiter__(self):
        return self._Iterator(self)

    def close(self):
        self.futures[-1].set_exception(StopAsyncIteration())

    def write(self, item):
        self.futures[-1].set_result(item)
        self.futures.append(Future())
