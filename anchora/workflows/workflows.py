from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from anchora.agents import FULFILLMENT_AGENT_ID, agent_for_step
    from anchora.models import OrderRequest, WorkflowState
    from anchora.workflows.compensation import SagaCompensator


ACTIVITY_TIMEOUT = timedelta(seconds=15)
ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=5),
    maximum_attempts=3,
)


@workflow.defn
class FulfillmentWorkflow:
    def __init__(self) -> None:
        self.state = WorkflowState(
            order_id="",
            agent_id=FULFILLMENT_AGENT_ID,
            status="pending",
        )

    @workflow.query
    def get_status(self) -> WorkflowState:
        return self.state

    @workflow.run
    async def run(self, order_id: str, order: OrderRequest) -> WorkflowState:
        self.state = WorkflowState(
            order_id=order_id,
            agent_id=FULFILLMENT_AGENT_ID,
            status="running",
        )
        compensator = SagaCompensator()
        amount = order.quantity * 1000

        try:
            await self._run_step(
                "check_inventory",
                order.product_id,
                order.quantity,
            )
            compensator.add(
                "release_inventory",
                order.product_id,
                order.quantity,
                order_id,
            )
            await self._run_step(
                "reserve_inventory",
                order.product_id,
                order.quantity,
                order_id,
            )
            compensator.add("refund_payment_by_idempotency_key", order_id)
            await self._run_step(
                "process_payment",
                order.customer_id,
                amount,
                order.payment_method,
                order_id,
            )
            compensator.add("revert_warehouse", order_id)
            await self._run_step(
                "update_warehouse",
                order_id,
                order.product_id,
                order.quantity,
            )
            self.state.status = "completed"
            self.state.record_event(
                "workflow",
                "completed",
                "Agent workflow completed",
                FULFILLMENT_AGENT_ID,
            )
            return self.state
        except Exception as exc:
            self.state.status = "compensating"
            self.state.compensation_triggered = True
            self.state.failure_reason = str(exc)
            self.state.record_event(
                self.state.current_step or "workflow",
                "failed",
                f"Workflow failed: {exc}",
                agent_for_step(self.state.current_step or "workflow"),
            )
            await compensator.compensate(self.state)
            self.state.status = "failed"
            return self.state

    async def _activity(self, name: str, *args: object):
        return await workflow.execute_activity(
            name,
            args=list(args),
            schedule_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )

    async def _run_step(
        self,
        step_name: str,
        *args: object,
    ):
        agent_id = agent_for_step(step_name)
        self.state.record_event(
            step_name, "started", f"{step_name} started", agent_id
        )
        result = await self._activity(step_name, *args)
        self.state.steps_completed.append(step_name)
        self.state.record_event(
            step_name, "completed", f"{step_name} completed", agent_id
        )
        return result
