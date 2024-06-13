from reconcile.dynatrace_token_provider.meta import QONTRACT_INTEGRATION
from reconcile.typed_queries.dynatrace_environments import get_dynatrace_environments
from reconcile.typed_queries.ocm import get_ocm_environments
from reconcile.utils.runtime.integration import (
    PydanticRunParams,
    QontractReconcileIntegration,
)


class DynatraceTokenProviderIntegrationParamsV2(PydanticRunParams):
    ocm_organization_ids: set[str] | None = None


class DynatraceTokenProviderIntegrationV2(
    QontractReconcileIntegration[DynatraceTokenProviderIntegrationParamsV2]
):
    @property
    def name(self) -> str:
        return QONTRACT_INTEGRATION

    def run(self, dry_run: bool) -> None:
        ocm_environments = get_ocm_environments()
        dynatrace_environments = get_dynatrace_environments()

        # TODO: select clusters
        # TODO: fetch existing tokens from clusters
        # TODO: update/create/delete tokens in clusters
