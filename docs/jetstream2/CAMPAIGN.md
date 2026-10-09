# Finite campaign authorization

`pilot campaign-preview --grant PRIVATE_JSON` is an offline preview. `pilot
campaign-apply --grant PRIVATE_JSON` records an explicit finite allowance only
after the existing controller, private locks and fresh provider offload checks
pass. Neither command activates a VM. Application and exact replay return an
auditable receipt; a reused identity with changed contents is refused.

The request has exactly these fields:

```json
{
  "schema": "flowdc-campaign-authorization-v1",
  "campaign_id": "11111111-1111-4111-8111-111111111111",
  "registration_id": "22222222-2222-4222-8222-222222222222",
  "expected_binding_sha256": "<64 lowercase hexadecimal characters>",
  "expected_accounts_sha256": "<64 lowercase hexadecimal characters>",
  "selected_ids": ["<three to six sorted registered UUIDs>"],
  "cumulative_limits_seconds": {"<each selected UUID>": 36000},
  "expires_at": "2026-10-16T00:00:00Z",
  "max_window_seconds": 1800,
  "protocol_sha256": "<frozen protocol file digest>",
  "budget_sha256": "<costed campaign file digest>",
  "budget_su": 500
}
```

The example is schematic and must be populated with observed registration,
binding and account digests from the private journal and the approved protocol
and budget. Digests are expectations, not proof of human authorization or fresh
readiness. The operator must retain the exact protocol and budget. The CLI does
not infer allocation balance, expiration or grant authority from an SU number.

Each selected account retains its original pilot `limit` (at most 7,200 seconds),
all consumption, uncertainty, events and obligations. A separate `campaign_limit`
sets the effective cumulative ceiling, at most 604,800 seconds. It must increase
the previous effective ceiling and leave enough time for a declared window.
Campaign expiry must be later than a full window and within seven days of the
application clock. Windows are finite, at most 1,800 seconds, including the
unchanged 600-second shutdown reserve. The selected account's verified positive
SU/hour rate is required. The sum of remaining granted seconds times those rates
must fit the declared compute budget. Rate and allocation facts must be refreshed
before the operational campaign; the stored rate alone is not a live bill.

Activation refuses expired campaigns, excessive windows, exhausted/uncertain
accounts or outstanding obligations. Restart retains the same allowance and
expiry. Expiry and clock ambiguity trigger cleanup; they never erase consumption
or discharge an obligation. Cleanup continues until a fresh identity-validated
`SHELVED_OFFLOADED` observation. Provider delays can exceed estimates and must
remain visible. Exact replay returns historical application only and does not
claim current readiness.

Journal application holds the existing experiment and maintenance locks, refuses
an active experiment owner, checks unchanged account/binding state, and commits
the new receipt atomically. Stale verification, concurrent state changes and
failed local checks roll back. New registrations and journal resets are not an
allowance-extension mechanism.

Prepare the exact UUIDs, protocol, compute/storage cost, conservative allocation
cutoff, cleanup reserve and rollback in the private deployment packet. Apply only
the approved campaign; subsequent experiment starts still use fresh readiness and
their separately identified workload/source/environment/deadline records.
