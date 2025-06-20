import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from reconcile import queries
from reconcile.slack_base import slackapi_from_slack_workspace
from reconcile.utils.defer import defer
from reconcile.utils.jira_client import JiraClient
from reconcile.utils.secret_reader import SecretReader
from reconcile.utils.sharding import is_in_shard_round_robin
from reconcile.utils.slack_api import SlackApi
from reconcile.utils.state import (
    State,
    init_state,
)

QONTRACT_INTEGRATION = "jira-watcher"


def fetch_current_state(
    jira_board: Mapping, settings: Mapping
) -> tuple[JiraClient, dict[str, dict[str, str]]]:
    jira = JiraClient(jira_board, settings=settings)
    issues = jira.get_issues(fields=["key", "status", "summary"])
    return jira, {
        issue.key: {"status": issue.fields.status.name, "summary": issue.fields.summary}
        for issue in issues
    }


def fetch_previous_state(state: State, project: str) -> dict:
    return state.get(project, {})


def format_message(
    server: str,
    key: str,
    data: Mapping,
    event: str,
    previous_state: Mapping | None = None,
    current_state: Mapping | None = None,
) -> str:
    summary = data["summary"]
    info = (
        ": {} -> {}".format(previous_state["status"], current_state["status"])
        if previous_state and current_state
        else ""
    )
    url = f"{server}/browse/{key}"
    return f"{url} ({summary}) {event}{info}"


def calculate_diff(
    server: str, current_state: Mapping, previous_state: Mapping
) -> list[str]:
    messages = []
    new_issues = [
        format_message(server, key, data, "created")
        for key, data in current_state.items()
        if key not in previous_state
    ]
    messages.extend(new_issues)

    deleted_issues = [
        format_message(server, key, data, "deleted")
        for key, data in previous_state.items()
        if key not in current_state
    ]
    messages.extend(deleted_issues)

    updated_issues = [
        format_message(server, key, data, "status change", previous_state[key], data)
        for key, data in current_state.items()
        if key in previous_state and data["status"] != previous_state[key]["status"]
    ]
    messages.extend(updated_issues)

    return messages


def init_slack(jira_board: Mapping[str, Any]) -> SlackApi:
    secret_reader = SecretReader(queries.get_secret_reader_settings())
    slack_info = jira_board["slack"]

    return slackapi_from_slack_workspace(
        slack_info,
        secret_reader,
        QONTRACT_INTEGRATION,
        channel=slack_info.get("channel"),
        init_usergroups=False,
    )


def act(dry_run: bool, jira_board: Mapping[str, str], diffs: Sequence[str]) -> None:
    if not dry_run and diffs:
        slack = init_slack(jira_board)

    for diff in reversed(diffs):
        logging.info(diff)
        if not dry_run:
            slack.chat_post_message(diff)


def write_state(state: State, project: str, state_to_write: Mapping) -> None:
    state.add(project, value=state_to_write, force=True)


@defer
def run(dry_run: bool, defer: Callable | None = None) -> None:
    jira_boards = [j for j in queries.get_jira_boards() if j.get("slack")]
    settings = queries.get_app_interface_settings()
    state = init_state(integration=QONTRACT_INTEGRATION)
    if defer:
        defer(state.cleanup)
    for index, jira_board in enumerate(jira_boards):
        if not is_in_shard_round_robin(jira_board["name"], index):
            continue
        jira, current_state = fetch_current_state(jira_board, settings)
        if not current_state:
            logging.warning(
                "not acting on empty Jira boards. "
                + "please create a ticket to get started."
            )
            continue
        previous_state = fetch_previous_state(state, jira.project)
        if previous_state:
            assert jira.server
            diffs = calculate_diff(jira.server, current_state, previous_state)
            act(dry_run, jira_board, diffs)
        if not dry_run:
            write_state(state, jira.project, current_state)
