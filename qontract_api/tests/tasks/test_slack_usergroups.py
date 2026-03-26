"""Unit tests for Slack usergroups Celery task."""

from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from qontract_api.integrations.slack_usergroups.schemas import (
    SlackUsergroupActionCreate,
    SlackUsergroupsTaskResult,
)
from qontract_api.integrations.slack_usergroups.tasks import (
    generate_lock_key,
    reconcile_slack_usergroups_task,
)
from qontract_api.models import Secret, TaskStatus
from qontract_api.slack.domain import (
    SlackUsergroup,
    SlackUsergroupConfig,
    SlackWorkspace,
)


@pytest.fixture
def sample_workspaces() -> list[SlackWorkspace]:
    """Create sample workspace list."""
    return [
        SlackWorkspace(
            name="workspace-1",
            managed_usergroups=["oncall"],
            usergroups=[
                SlackUsergroup(
                    handle="oncall",
                    config=SlackUsergroupConfig(
                        users=["alice@example.com"],
                        channels=[],
                        description="",
                    ),
                )
            ],
            token=Secret(
                secret_manager_url="https://vault.example.com",
                path="secret/slack/workspace-1",
            ),
        )
    ]


def test_generate_lock_key_single_workspace(
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """Test lock key generation for single workspace."""
    mock_self = MagicMock()
    lock_key = generate_lock_key(mock_self, sample_workspaces)

    assert lock_key == "workspace-1"


def test_generate_lock_key_multiple_workspaces() -> None:
    """Test lock key generation for multiple workspaces (sorted)."""
    workspaces = [
        SlackWorkspace(
            name="workspace-b",
            managed_usergroups=[],
            usergroups=[],
            token=Secret(
                secret_manager_url="https://vault.example.com",
                path="secret/slack/workspace-b",
            ),
        ),
        SlackWorkspace(
            name="workspace-a",
            managed_usergroups=[],
            usergroups=[],
            token=Secret(
                secret_manager_url="https://vault.example.com",
                path="secret/slack/workspace-a",
            ),
        ),
    ]
    mock_self = MagicMock()
    lock_key = generate_lock_key(mock_self, workspaces)

    # Should be sorted alphabetically
    assert lock_key == "workspace-a,workspace-b"


@patch("qontract_api.integrations.slack_usergroups.tasks.get_cache")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_secret_manager")
@patch(
    "qontract_api.integrations.slack_usergroups.service.create_slack_workspace_client"
)
def test_reconcile_task_dry_run_success(
    mock_factory_function: MagicMock,
    mock_get_secret_manager: MagicMock,
    mock_get_cache: MagicMock,
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """Test task execution in dry-run mode."""
    # Setup mocks
    mock_cache = MagicMock()
    mock_get_cache.return_value = mock_cache

    mock_secret_backend = MagicMock()
    mock_secret_backend.read.return_value = "xoxb-test-token"
    mock_get_secret_manager.return_value = mock_secret_backend

    mock_slack_client = MagicMock()
    mock_slack_client.get_slack_usergroups.return_value = []
    mock_slack_client.clean_slack_usergroups.return_value = []

    # Mock the factory function to return the client directly
    mock_factory_function.return_value = mock_slack_client

    # Create mock task instance
    mock_self = MagicMock()
    mock_self.request.id = "test-task-123"

    # Access the underlying function bypassing the decorator
    task_func = reconcile_slack_usergroups_task.__wrapped__.__wrapped__

    # Execute task (dry-run)
    result = task_func(mock_self, sample_workspaces, dry_run=True)

    # Verify result
    assert isinstance(result, SlackUsergroupsTaskResult)
    assert result.status == TaskStatus.SUCCESS
    assert result.applied_count == 0  # dry-run
    assert result.errors == []

    # Verify factory function was called with correct arguments
    assert mock_factory_function.called


# ---------------------------------------------------------------------------
# Event publishing — helpers for service-level mocking
# ---------------------------------------------------------------------------


def _task_func() -> Callable:
    """Return the unwrapped task function (bypasses Celery + deduplication decorators)."""
    return reconcile_slack_usergroups_task.__wrapped__.__wrapped__


def _make_action(
    workspace: str = "workspace-1",
    usergroup: str = "oncall",
) -> SlackUsergroupActionCreate:
    return SlackUsergroupActionCreate(
        workspace=workspace,
        usergroup=usergroup,
        users=["alice@example.com"],
        description="",
    )


def _make_result(
    actions: list[SlackUsergroupActionCreate] | None = None,
    errors: list[str] | None = None,
) -> SlackUsergroupsTaskResult:
    acts = actions or []
    errs = errors or []
    return SlackUsergroupsTaskResult(
        status=TaskStatus.FAILED if errs else TaskStatus.SUCCESS,
        actions=acts,
        applied_count=len(acts),
        errors=errs,
    )


# ---------------------------------------------------------------------------
# Event publishing — success events
# ---------------------------------------------------------------------------


@patch("qontract_api.integrations.slack_usergroups.tasks.get_event_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_secret_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_cache")
@patch("qontract_api.integrations.slack_usergroups.tasks.SlackUsergroupsService")
def test_publishes_success_event_for_applied_action(
    mock_service_cls: MagicMock,
    mock_get_cache: MagicMock,
    mock_get_secret_manager: MagicMock,
    mock_get_event_manager: MagicMock,
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """A success event is published for each successfully applied action."""
    action = _make_action()
    mock_service_cls.return_value.reconcile.return_value = _make_result(
        actions=[action]
    )
    mock_event_manager = MagicMock()
    mock_get_event_manager.return_value = mock_event_manager

    mock_self = MagicMock()
    mock_self.request.id = "test-task-ok"

    _task_func()(mock_self, sample_workspaces, dry_run=False)

    mock_event_manager.publish_event.assert_called_once()
    published = mock_event_manager.publish_event.call_args[0][0]
    assert published.type == "qontract-api.slack-usergroups.create"


# ---------------------------------------------------------------------------
# Event publishing — error events
# ---------------------------------------------------------------------------


@patch("qontract_api.integrations.slack_usergroups.tasks.get_event_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_secret_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_cache")
@patch("qontract_api.integrations.slack_usergroups.tasks.SlackUsergroupsService")
def test_publishes_error_event_for_each_error(
    mock_service_cls: MagicMock,
    mock_get_cache: MagicMock,
    mock_get_secret_manager: MagicMock,
    mock_get_event_manager: MagicMock,
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """An error event is published for each reconciliation error."""
    mock_service_cls.return_value.reconcile.return_value = _make_result(
        errors=["workspace-1/oncall: Failed to execute action create: Slack API error"]
    )
    mock_event_manager = MagicMock()
    mock_get_event_manager.return_value = mock_event_manager

    mock_self = MagicMock()
    mock_self.request.id = "test-task-err"

    _task_func()(mock_self, sample_workspaces, dry_run=False)

    mock_event_manager.publish_event.assert_called_once()
    published = mock_event_manager.publish_event.call_args[0][0]
    assert published.type == "qontract-api.slack-usergroups.error"
    assert "Failed to execute" in published.data["error"]


@patch("qontract_api.integrations.slack_usergroups.tasks.get_event_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_secret_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_cache")
@patch("qontract_api.integrations.slack_usergroups.tasks.SlackUsergroupsService")
def test_publishes_both_event_types_on_partial_failure(
    mock_service_cls: MagicMock,
    mock_get_cache: MagicMock,
    mock_get_secret_manager: MagicMock,
    mock_get_event_manager: MagicMock,
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """Both action and error events are published when there are actions and errors."""
    action = _make_action()
    mock_service_cls.return_value.reconcile.return_value = _make_result(
        actions=[action],
        errors=["workspace-1/team-b: Failed to execute action create: Slack API error"],
    )
    mock_event_manager = MagicMock()
    mock_get_event_manager.return_value = mock_event_manager

    mock_self = MagicMock()
    mock_self.request.id = "test-task-partial"

    _task_func()(mock_self, sample_workspaces, dry_run=False)

    assert mock_event_manager.publish_event.call_count == 2
    event_types = {
        c[0][0].type for c in mock_event_manager.publish_event.call_args_list
    }
    assert event_types == {
        "qontract-api.slack-usergroups.create",
        "qontract-api.slack-usergroups.error",
    }


@patch("qontract_api.integrations.slack_usergroups.tasks.get_event_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_secret_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_cache")
@patch("qontract_api.integrations.slack_usergroups.tasks.SlackUsergroupsService")
def test_no_events_published_in_dry_run(
    mock_service_cls: MagicMock,
    mock_get_cache: MagicMock,
    mock_get_secret_manager: MagicMock,
    mock_get_event_manager: MagicMock,
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """No events are published in dry-run mode."""
    mock_service_cls.return_value.reconcile.return_value = _make_result(
        actions=[_make_action()],
        errors=["some error"],
    )
    mock_event_manager = MagicMock()
    mock_get_event_manager.return_value = mock_event_manager

    mock_self = MagicMock()
    mock_self.request.id = "test-task-dry"

    _task_func()(mock_self, sample_workspaces, dry_run=True)

    mock_event_manager.publish_event.assert_not_called()


@patch("qontract_api.integrations.slack_usergroups.tasks.get_event_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_secret_manager")
@patch("qontract_api.integrations.slack_usergroups.tasks.get_cache")
@patch("qontract_api.integrations.slack_usergroups.tasks.SlackUsergroupsService")
def test_no_events_published_when_event_manager_disabled(
    mock_service_cls: MagicMock,
    mock_get_cache: MagicMock,
    mock_get_secret_manager: MagicMock,
    mock_get_event_manager: MagicMock,
    sample_workspaces: list[SlackWorkspace],
) -> None:
    """No events are published when the event manager is not configured (returns None)."""
    mock_service_cls.return_value.reconcile.return_value = _make_result(
        actions=[_make_action()],
        errors=["some error"],
    )
    mock_get_event_manager.return_value = None

    mock_self = MagicMock()
    mock_self.request.id = "test-task-no-em"

    result = _task_func()(mock_self, sample_workspaces, dry_run=False)
    assert (
        result.status == TaskStatus.FAILED
    )  # errors present → failed, but no exception raised
