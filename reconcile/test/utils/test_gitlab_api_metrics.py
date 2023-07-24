from pytest_mock import MockerFixture

from reconcile.utils.secret_reader import SecretReaderBase
from reconcile.utils.gitlab_api import GitLabApi, MRState
from gitlab.v4.objects import (
    Project,
)
from unittest.mock import (
    create_autospec,
)


def test(mocker: MockerFixture, secret_reader: SecretReaderBase) -> None:
    mocked_gitlab = mocker.patch("gitlab.Gitlab")
    instance = {
        "token": "test",
        "sslVerify": None,
        "url": "test",
    }
    project = create_autospec(spec=Project)
    mocked_gitlab.projects = {"1": project}
    api = GitLabApi(
        instance=instance,
        secret_reader=secret_reader,
        project_id="1",
    )
    
    api.get_merge_requests(state=MRState.OPENED)
    assert api.project == project
