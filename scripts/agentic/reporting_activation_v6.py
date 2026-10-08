"""Finite V6 authority and durable accounting; provider dispatch remains disabled.

The final exact catalog/check adapter remains a deliberately closed dependency.
No caller-supplied success or context can activate this module. Synthetic tests replace those seams only in disposable
repositories; they provide no native qualification. Historical APIs are unchanged.
"""

from __future__ import annotations

import copy
import os
import re
import time

from claude_reporting_execution import exclusive
from reporting_activation_v2 import clock, read
from reporting_activation_v4 import validate_policy
from tasks import digest, plain_path, private_directory
from workflow import WorkflowError

CONTRACT = {
    "issue": 31,
    "plan_comment": 6035844223,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "494abcda1fa95d346ae5f8a84b638202701e4756c3ceb24fc4b4f0a9f9d114e5",
}
CONTRACT_DIGEST = "d124f792051c909f1cf77c8717393888141ff64c2c7cf5a38411f48cce7c5467"
# Exact prospective S binding; the exported g12 constants remain historical APIs.
NEXT_CONTRACT = {
    "issue": 31,
    "plan_comment": 6045434332,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "05e7a6d54163067d7b0a02f1e3d61e995482e617233b0d973a41940add09d85f",
}
NEXT_CONTRACT_DIGEST = "02e78ae136d06616db297cf656a3e17f6d6ee466a29970d7f6869adfe928d691"
NEXT_APPROVAL_DIGEST = "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"
NEXT_HISTORY_DIGEST = "1e687cfea245e9926302b77b51733c4abf1d36c4e968c9a98b5bcc930bdb44b2"
NEXT_HISTORY_ROWS = (
    (5900844013, "5eba73542750e24757e47f0cd7bd6143ca0f430c0439a8b0c9becb6b50ba7f98"),
    (5966428269, "c1006c694d96ef8487ee6742d54e427d8394ae10365b8a52275e6b0acc66ac35"),
    (6001819615, "6c9709afe5e931fd88340bc30b75558a1c51cc1467831691bdb82e4169b7f67e"),
    (6008093895, "8cb40e9af7f48c2f9522c4e146c7efd4e6dad24d2d71bcac667def8a8764f566"),
    (6009076812, "3143727dc6cf38b8f54cb4c3f017fbde7557006a9a0ac569d61f531149f872d7"),
    (6009865197, "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"),
    (6010775261, "004ea3142aed88ca9fa955d5ef808ce1285486dff8f73de5c84e48d9ed25c1e6"),
    (6011162252, "177ba4f77027e22b0f2510fd918ad66ebf1c6fdf2fb5e0abaf67a81e9fb1aabc"),
    (6012492318, "061e2149206692cdff0e8c013872a30c3dd205c678c8bbb6db81b7d05b795970"),
    (6013795098, "a3d1f17e7f3b3884d40859b9622a1bd172e0d05978e5515552171ed7be214e97"),
    (6014789492, "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"),
    (6035844223, "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"),
)
G14_CONTRACT = {
    "issue": 31,
    "plan_comment": 6061320190,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "22712ec0c4661047adc4b00960ca9f2dd8d162cbc243b658f7ce23a8d63fac21",
}
G14_CONTRACT_DIGEST = "dbe1725734df4a5392ebcdbe7ef6f898c722240c939ebef88578078ce25e40a8"
G14_APPROVAL_DIGEST = "4e6725fa686b93c38afcc3eec46c5ff5583cc28f319105b9eefb77051dea40bc"
G14_HISTORY_DIGEST = "9ddf1af97e909ed3817e3ab69c2010b66fecf7224e33cb063e5f0789ecad2791"
G14_HISTORY_ROWS = (
    (5900844013, "5eba73542750e24757e47f0cd7bd6143ca0f430c0439a8b0c9becb6b50ba7f98"),
    (5966428269, "c1006c694d96ef8487ee6742d54e427d8394ae10365b8a52275e6b0acc66ac35"),
    (6001819615, "6c9709afe5e931fd88340bc30b75558a1c51cc1467831691bdb82e4169b7f67e"),
    (6008093895, "8cb40e9af7f48c2f9522c4e146c7efd4e6dad24d2d71bcac667def8a8764f566"),
    (6009076812, "3143727dc6cf38b8f54cb4c3f017fbde7557006a9a0ac569d61f531149f872d7"),
    (6009865197, "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"),
    (6010775261, "004ea3142aed88ca9fa955d5ef808ce1285486dff8f73de5c84e48d9ed25c1e6"),
    (6011162252, "177ba4f77027e22b0f2510fd918ad66ebf1c6fdf2fb5e0abaf67a81e9fb1aabc"),
    (6012492318, "061e2149206692cdff0e8c013872a30c3dd205c678c8bbb6db81b7d05b795970"),
    (6013795098, "a3d1f17e7f3b3884d40859b9622a1bd172e0d05978e5515552171ed7be214e97"),
    (6014789492, "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"),
    (6035844223, "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"),
    (6045434332, "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"),
)
G15_CONTRACT = {
    "issue": 31,
    "plan_comment": 6062530466,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "915e46d413f2d19b0ee2b69ce6ef903fa787d777ebad442d4236781bb2120436",
}
G15_CONTRACT_DIGEST = "729bca73b3fcc64ad044e7dd876657436c8cb614df871876fc855fbff2e93105"
G15_APPROVAL_DIGEST = "248965ba97be950afe274bb9f76780f2811837030420b1cd9968370d9b1d36f5"
G15_HISTORY_DIGEST = "33b77694522896b294ccc364d6194c4f43c80bc708ca64da9758ecfd948df4fb"
G15_HISTORY_ROWS = (
    (5900844013, "5eba73542750e24757e47f0cd7bd6143ca0f430c0439a8b0c9becb6b50ba7f98"),
    (5966428269, "c1006c694d96ef8487ee6742d54e427d8394ae10365b8a52275e6b0acc66ac35"),
    (6001819615, "6c9709afe5e931fd88340bc30b75558a1c51cc1467831691bdb82e4169b7f67e"),
    (6008093895, "8cb40e9af7f48c2f9522c4e146c7efd4e6dad24d2d71bcac667def8a8764f566"),
    (6009076812, "3143727dc6cf38b8f54cb4c3f017fbde7557006a9a0ac569d61f531149f872d7"),
    (6009865197, "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"),
    (6010775261, "004ea3142aed88ca9fa955d5ef808ce1285486dff8f73de5c84e48d9ed25c1e6"),
    (6011162252, "177ba4f77027e22b0f2510fd918ad66ebf1c6fdf2fb5e0abaf67a81e9fb1aabc"),
    (6012492318, "061e2149206692cdff0e8c013872a30c3dd205c678c8bbb6db81b7d05b795970"),
    (6013795098, "a3d1f17e7f3b3884d40859b9622a1bd172e0d05978e5515552171ed7be214e97"),
    (6014789492, "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"),
    (6035844223, "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"),
    (6045434332, "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"),
    (6061320190, "4e6725fa686b93c38afcc3eec46c5ff5583cc28f319105b9eefb77051dea40bc"),
)
G16_CONTRACT = {
    "issue": 31,
    "plan_comment": 6064513854,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "63e7370374cab273b55c0188f92ec84d41b37ea6c0e43bf00da710a544e28731",
}
G16_CONTRACT_DIGEST = "bfbdf3dc6142487812e945c1fc91338e8c634a1eda9d5e7b4f50108499391af9"
G16_APPROVAL_DIGEST = "8aad3237f9f908ce48cef1f9d84ac82fdd669d0a33e26d7bf723a28deebefe79"
G16_HISTORY_DIGEST = "933026902cdc02f9bee28407960c0588776b94eac3041b794505a0aa3e2116b4"
G16_HISTORY_ROWS = (
    (5900844013, "5eba73542750e24757e47f0cd7bd6143ca0f430c0439a8b0c9becb6b50ba7f98"),
    (5966428269, "c1006c694d96ef8487ee6742d54e427d8394ae10365b8a52275e6b0acc66ac35"),
    (6001819615, "6c9709afe5e931fd88340bc30b75558a1c51cc1467831691bdb82e4169b7f67e"),
    (6008093895, "8cb40e9af7f48c2f9522c4e146c7efd4e6dad24d2d71bcac667def8a8764f566"),
    (6009076812, "3143727dc6cf38b8f54cb4c3f017fbde7557006a9a0ac569d61f531149f872d7"),
    (6009865197, "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"),
    (6010775261, "004ea3142aed88ca9fa955d5ef808ce1285486dff8f73de5c84e48d9ed25c1e6"),
    (6011162252, "177ba4f77027e22b0f2510fd918ad66ebf1c6fdf2fb5e0abaf67a81e9fb1aabc"),
    (6012492318, "061e2149206692cdff0e8c013872a30c3dd205c678c8bbb6db81b7d05b795970"),
    (6013795098, "a3d1f17e7f3b3884d40859b9622a1bd172e0d05978e5515552171ed7be214e97"),
    (6014789492, "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"),
    (6035844223, "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"),
    (6045434332, "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"),
    (6061320190, "4e6725fa686b93c38afcc3eec46c5ff5583cc28f319105b9eefb77051dea40bc"),
    (6062530466, "248965ba97be950afe274bb9f76780f2811837030420b1cd9968370d9b1d36f5"),
)

PURPOSE = "issue-31-reporting-recovery-v6"
SEQUENCE = {
    20: "isolation-refusal",
    21: "native-tools-and-source",
    22: "capacity-largest-component",
    23: "capacity-integration48-projected",
}
LIMITS = {
    "processes": 4,
    "seconds": 2400,
    "reference_usd": 24,
    "paid_extra_usd": 0,
    "api_usd": 0,
    "setup_seconds": 900,
    "local_seconds_per_slot": 840,
    "replay_seconds_per_slot": 180,
    "wall_seconds": 7380,
    "expiry_seconds": 10800,
    "credential_margin_seconds": 360,
}
MAX_RECORD_BYTES = 2_000_000


G18_CONTRACT = {
    "issue": 31,
    "plan_comment": 6068705967,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "a9d3c8e4275db2f356d81889b5ba6079519e8670faeca5b5da6a032bbafa3c9f",
}
G18_CONTRACT_DIGEST = "e708e430432efd51f5ddc4fc9143f1f6b1d2ed4956d794cb8f5038d1a71b8390"
G18_APPROVAL_DIGEST = "75344fc856315303b3cdb632f6e91a4ada36f3960bf37b6d0824c8a8ed9e85ba"
G18_HISTORY_DIGEST = "b060a680a45ef43afc885bcf95ff2f1ced25f24cadc8c6e369d5d9984fe1862f"
G18_HISTORY_ROWS = (
    (5900844013, "5eba73542750e24757e47f0cd7bd6143ca0f430c0439a8b0c9becb6b50ba7f98"),
    (5966428269, "c1006c694d96ef8487ee6742d54e427d8394ae10365b8a52275e6b0acc66ac35"),
    (6001819615, "6c9709afe5e931fd88340bc30b75558a1c51cc1467831691bdb82e4169b7f67e"),
    (6008093895, "8cb40e9af7f48c2f9522c4e146c7efd4e6dad24d2d71bcac667def8a8764f566"),
    (6009076812, "3143727dc6cf38b8f54cb4c3f017fbde7557006a9a0ac569d61f531149f872d7"),
    (6009865197, "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"),
    (6010775261, "004ea3142aed88ca9fa955d5ef808ce1285486dff8f73de5c84e48d9ed25c1e6"),
    (6011162252, "177ba4f77027e22b0f2510fd918ad66ebf1c6fdf2fb5e0abaf67a81e9fb1aabc"),
    (6012492318, "061e2149206692cdff0e8c013872a30c3dd205c678c8bbb6db81b7d05b795970"),
    (6013795098, "a3d1f17e7f3b3884d40859b9622a1bd172e0d05978e5515552171ed7be214e97"),
    (6014789492, "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"),
    (6035844223, "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"),
    (6045434332, "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"),
    (6061320190, "4e6725fa686b93c38afcc3eec46c5ff5583cc28f319105b9eefb77051dea40bc"),
    (6062530466, "248965ba97be950afe274bb9f76780f2811837030420b1cd9968370d9b1d36f5"),
    (6064513854, "8aad3237f9f908ce48cef1f9d84ac82fdd669d0a33e26d7bf723a28deebefe79"),
    (6068144159, "0e2155b4bb9fc33653859cf350d6d9b9e250b7b705a92c9d00d232340fb8a35c"),
)

G17_CONTRACT = {
    "issue": 31,
    "plan_comment": 6068144159,
    "issue_digest": "1875464d1340e35cd90fae86ad70e87be1b13108587ef138ce8130cc9263c03f",
    "plan_digest": "d53015047613e856093221f416a0811abac71feb40554b80deb38e5f4576aca4",
}
G17_CONTRACT_DIGEST = "721f492ae03b8ac6356766996c5b87167f914c76a7acd902e8fabf35ee630d33"
G17_APPROVAL_DIGEST = "0e2155b4bb9fc33653859cf350d6d9b9e250b7b705a92c9d00d232340fb8a35c"
G17_HISTORY_DIGEST = "34ce5ba5ced0ce394e30c99ac5a44082af57813055b937a89aec98a0b9138333"
G17_HISTORY_ROWS = (
    (5900844013, "5eba73542750e24757e47f0cd7bd6143ca0f430c0439a8b0c9becb6b50ba7f98"),
    (5966428269, "c1006c694d96ef8487ee6742d54e427d8394ae10365b8a52275e6b0acc66ac35"),
    (6001819615, "6c9709afe5e931fd88340bc30b75558a1c51cc1467831691bdb82e4169b7f67e"),
    (6008093895, "8cb40e9af7f48c2f9522c4e146c7efd4e6dad24d2d71bcac667def8a8764f566"),
    (6009076812, "3143727dc6cf38b8f54cb4c3f017fbde7557006a9a0ac569d61f531149f872d7"),
    (6009865197, "65ea7d798151f777a1e6682d7cda05f14b6722bd0c36f455febaabe8d7f2023d"),
    (6010775261, "004ea3142aed88ca9fa955d5ef808ce1285486dff8f73de5c84e48d9ed25c1e6"),
    (6011162252, "177ba4f77027e22b0f2510fd918ad66ebf1c6fdf2fb5e0abaf67a81e9fb1aabc"),
    (6012492318, "061e2149206692cdff0e8c013872a30c3dd205c678c8bbb6db81b7d05b795970"),
    (6013795098, "a3d1f17e7f3b3884d40859b9622a1bd172e0d05978e5515552171ed7be214e97"),
    (6014789492, "111069f7b7474052c5055b2b9f69cf73b2df8b5aba2f5c719550fb20bad37a71"),
    (6035844223, "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"),
    (6045434332, "0cf9de4afdbd048248941023eaf66b4d4f7be70108d11026c69b30a085bd7605"),
    (6061320190, "4e6725fa686b93c38afcc3eec46c5ff5583cc28f319105b9eefb77051dea40bc"),
    (6062530466, "248965ba97be950afe274bb9f76780f2811837030420b1cd9968370d9b1d36f5"),
    (6064513854, "8aad3237f9f908ce48cef1f9d84ac82fdd669d0a33e26d7bf723a28deebefe79"),
)


def root(repo):
    return plain_path(repo.main / ".agentic-local/claude-reporting-activation-v6")


def _hex(value, length=64):
    return type(value) is str and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None


def slot(number):
    if type(number) is not int or number not in SEQUENCE:
        raise WorkflowError("Only V6 slots 20 through 23 exist; no retry or slot24")
    return {
        "number": number,
        "purpose": SEQUENCE[number],
        "native_seconds": 300 if number < 22 else 900,
        "reference_usd": 2 if number < 22 else 10,
    }


def remaining(number):
    slot(number)
    return sum(slot(n)["native_seconds"] + 840 + 180 for n in SEQUENCE if n >= number)


def _next_history(state):
    rows = state.get("approval_history")
    if (
        type(rows) is not list
        or len(rows) != 12
        or digest(rows) != NEXT_HISTORY_DIGEST
        or any(
            type(row) is not dict
            or type(row.get("plan_comment")) is not int
            or row["plan_comment"] != comment
            or digest(row) != expected
            for row, (comment, expected) in zip(rows, NEXT_HISTORY_ROWS, strict=True)
        )
    ):
        raise WorkflowError("V6 next ordered approval history differs")


def _g14_history(state):
    rows = state.get("approval_history")
    if (
        type(rows) is not list
        or len(rows) != 13
        or digest(rows) != G14_HISTORY_DIGEST
        or any(
            type(row) is not dict
            or type(row.get("plan_comment")) is not int
            or row["plan_comment"] != comment
            or digest(row) != expected
            for row, (comment, expected) in zip(rows, G14_HISTORY_ROWS, strict=True)
        )
    ):
        raise WorkflowError("V6 generation14 ordered approval history differs")


def _g15_history(state):
    rows = state.get("approval_history")
    if (
        type(rows) is not list
        or len(rows) != 14
        or digest(rows) != G15_HISTORY_DIGEST
        or any(
            type(row) is not dict
            or type(row.get("plan_comment")) is not int
            or row["plan_comment"] != comment
            or digest(row) != expected
            for row, (comment, expected) in zip(rows, G15_HISTORY_ROWS, strict=True)
        )
    ):
        raise WorkflowError("V6 generation15 ordered approval history differs")


def _g16_history(state):
    rows = state.get("approval_history")
    if (
        type(rows) is not list
        or len(rows) != 15
        or digest(rows) != G16_HISTORY_DIGEST
        or any(
            type(row) is not dict
            or type(row.get("plan_comment")) is not int
            or row["plan_comment"] != comment
            or digest(row) != expected
            for row, (comment, expected) in zip(rows, G16_HISTORY_ROWS, strict=True)
        )
    ):
        raise WorkflowError("V6 generation16 ordered approval history differs")


def _g17_history(state):
    rows = state.get("approval_history")
    if (
        type(rows) is not list
        or len(rows) != 16
        or digest(rows) != G17_HISTORY_DIGEST
        or any(
            type(row) is not dict
            or type(row.get("plan_comment")) is not int
            or row["plan_comment"] != comment
            or digest(row) != expected
            for row, (comment, expected) in zip(rows, G17_HISTORY_ROWS, strict=True)
        )
    ):
        raise WorkflowError("V6 generation17 ordered approval history differs")


def _g18_history(state):
    rows = state.get("approval_history")
    if (
        type(rows) is not list
        or len(rows) != 17
        or digest(rows) != G18_HISTORY_DIGEST
        or any(
            type(row) is not dict
            or type(row.get("plan_comment")) is not int
            or row["plan_comment"] != comment
            or digest(row) != expected
            for row, (comment, expected) in zip(rows, G18_HISTORY_ROWS, strict=True)
        )
    ):
        raise WorkflowError("V6 generation18 ordered approval history differs")


def selected_contract(repo):
    """Select only an internally verified, literal current contract."""
    authority = authorization(repo)
    if authority["contract_digest"] == G18_CONTRACT_DIGEST:
        return copy.deepcopy(G18_CONTRACT)
    if authority["contract_digest"] == G17_CONTRACT_DIGEST:
        return copy.deepcopy(G17_CONTRACT)
    if authority["contract_digest"] == G16_CONTRACT_DIGEST:
        return copy.deepcopy(G16_CONTRACT)
    if authority["contract_digest"] == G15_CONTRACT_DIGEST:
        return copy.deepcopy(G15_CONTRACT)
    if authority["contract_digest"] == G14_CONTRACT_DIGEST:
        return copy.deepcopy(G14_CONTRACT)
    contract = NEXT_CONTRACT if authority["contract_digest"] == NEXT_CONTRACT_DIGEST else CONTRACT
    return copy.deepcopy(contract)


def authorization(repo):
    state = read(repo.main / ".agentic-local/tasks/issue-31.json")
    approval = state.get("approval")
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 18:
        if (
            repo.name != "Zi-Deng/FLOW-DC"
            or state.get("repository") != repo.name
            or state.get("key") != "issue-31"
            or digest(G18_CONTRACT) != G18_CONTRACT_DIGEST
            or type(approval) is not dict
            or type(approval.get("issue")) is not int
            or approval["issue"] != 31
            or type(approval.get("plan_comment")) is not int
            or approval["plan_comment"] != 6068705967
            or digest(approval.get("contract")) != G18_CONTRACT_DIGEST
            or digest(approval) != G18_APPROVAL_DIGEST
        ):
            raise WorkflowError("V6 requires the exact generation18 approval")
        _g18_history(state)
        return {"contract_digest": G18_CONTRACT_DIGEST, "approval_digest": G18_APPROVAL_DIGEST}
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 17:
        if (
            repo.name != "Zi-Deng/FLOW-DC"
            or state.get("repository") != repo.name
            or state.get("key") != "issue-31"
            or digest(G17_CONTRACT) != G17_CONTRACT_DIGEST
            or type(approval) is not dict
            or type(approval.get("issue")) is not int
            or approval["issue"] != 31
            or type(approval.get("plan_comment")) is not int
            or approval["plan_comment"] != 6068144159
            or digest(approval.get("contract")) != G17_CONTRACT_DIGEST
            or digest(approval) != G17_APPROVAL_DIGEST
        ):
            raise WorkflowError("V6 requires the exact generation17 approval")
        _g17_history(state)
        return {"contract_digest": G17_CONTRACT_DIGEST, "approval_digest": G17_APPROVAL_DIGEST}
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 16:
        if (
            repo.name != "Zi-Deng/FLOW-DC"
            or state.get("repository") != repo.name
            or state.get("key") != "issue-31"
            or digest(G16_CONTRACT) != G16_CONTRACT_DIGEST
            or type(approval) is not dict
            or type(approval.get("issue")) is not int
            or approval["issue"] != 31
            or type(approval.get("plan_comment")) is not int
            or approval["plan_comment"] != 6064513854
            or digest(approval.get("contract")) != G16_CONTRACT_DIGEST
            or digest(approval) != G16_APPROVAL_DIGEST
        ):
            raise WorkflowError("V6 requires the exact generation16 approval")
        _g16_history(state)
        return {"contract_digest": G16_CONTRACT_DIGEST, "approval_digest": G16_APPROVAL_DIGEST}
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 15:
        if (
            repo.name != "Zi-Deng/FLOW-DC"
            or state.get("repository") != repo.name
            or state.get("key") != "issue-31"
            or digest(G15_CONTRACT) != G15_CONTRACT_DIGEST
            or type(approval) is not dict
            or type(approval.get("issue")) is not int
            or approval["issue"] != 31
            or type(approval.get("plan_comment")) is not int
            or approval["plan_comment"] != 6062530466
            or digest(approval.get("contract")) != G15_CONTRACT_DIGEST
            or digest(approval) != G15_APPROVAL_DIGEST
        ):
            raise WorkflowError("V6 requires the exact generation15 approval")
        _g15_history(state)
        return {"contract_digest": G15_CONTRACT_DIGEST, "approval_digest": G15_APPROVAL_DIGEST}
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 14:
        if (
            repo.name != "Zi-Deng/FLOW-DC"
            or state.get("repository") != repo.name
            or state.get("key") != "issue-31"
            or digest(G14_CONTRACT) != G14_CONTRACT_DIGEST
            or type(approval) is not dict
            or type(approval.get("issue")) is not int
            or approval["issue"] != 31
            or type(approval.get("plan_comment")) is not int
            or approval["plan_comment"] != 6061320190
            or digest(approval.get("contract")) != G14_CONTRACT_DIGEST
            or digest(approval) != G14_APPROVAL_DIGEST
        ):
            raise WorkflowError("V6 requires the exact generation14 approval")
        _g14_history(state)
        return {"contract_digest": G14_CONTRACT_DIGEST, "approval_digest": G14_APPROVAL_DIGEST}
    if type(state.get("contract_generation")) is int and state["contract_generation"] == 13:
        if (
            repo.name != "Zi-Deng/FLOW-DC"
            or state.get("repository") != repo.name
            or state.get("key") != "issue-31"
            or digest(NEXT_CONTRACT) != NEXT_CONTRACT_DIGEST
            or type(approval) is not dict
            or type(approval.get("issue")) is not int
            or approval["issue"] != 31
            or type(approval.get("plan_comment")) is not int
            or approval["plan_comment"] != 6045434332
            or digest(approval.get("contract")) != NEXT_CONTRACT_DIGEST
            or digest(approval) != NEXT_APPROVAL_DIGEST
        ):
            raise WorkflowError("V6 requires the exact generation13 approval")
        _next_history(state)
        return {"contract_digest": NEXT_CONTRACT_DIGEST, "approval_digest": NEXT_APPROVAL_DIGEST}
    if (
        repo.name != "Zi-Deng/FLOW-DC"
        or state.get("repository") != repo.name
        or state.get("key") != "issue-31"
        or type(state.get("contract_generation")) is not int
        or state["contract_generation"] != 12
        or digest(CONTRACT) != CONTRACT_DIGEST
        or type(approval) is not dict
        or digest(approval.get("contract")) != CONTRACT_DIGEST
        or type(approval.get("issue")) is not int
        or approval["issue"] != 31
        or type(approval.get("plan_comment")) is not int
        or approval["plan_comment"] != 6035844223
        or type(approval.get("source")) is not str
        or not approval["source"].strip()
        or type(approval.get("recorded_at")) is not str
        or not approval["recorded_at"].strip()
    ):
        raise WorkflowError("V6 requires the current exact issue-31 approval")
    return {"contract_digest": CONTRACT_DIGEST, "approval_digest": digest(approval)}


def historical(repo):
    from reporting_recovery_history_v6 import stopped

    return stopped(repo)


def context(repo, policy, *, remaining_seconds=7380, owned_auth=None):
    """Closed until final-source fixtures and owned full-window checks are wired.

    The later implementation must recheck exact source/history/authority/policy,
    all four fixture descriptors and unchanged owned authentication generation;
    credential AND paid-receipt lifetime must exceed remaining_seconds + 360.
    The default catalog refusal precedes all credential/history access. Once that
    adapter exists, only an already-owned snapshot may verify the full window;
    there is no unowned fallback or caller-supplied qualification.
    """
    from claude_owned_auth import require
    from reporting_activation_v2 import harness
    from reporting_diagnostic_v6 import catalog, fixture_binding, packets
    from reporting_recovery_history import semantics

    current = authorization(repo)
    validate_policy(policy)
    if type(remaining_seconds) not in {int, float} or not 0 < remaining_seconds <= 7380:
        raise WorkflowError("Invalid V6 full remaining window")
    # The actual final catalog/check adapter is deliberately not implemented yet.
    source = catalog(repo)
    expected_approval = (
        NEXT_APPROVAL_DIGEST
        if current["contract_digest"] == NEXT_CONTRACT_DIGEST
        else "ae8e4b2ff106d44908e471d1f36be74b5d9f632f1d0b92b32e5261193a245cd8"
    )
    if current["contract_digest"] == G18_CONTRACT_DIGEST:
        expected_approval = G18_APPROVAL_DIGEST
    elif current["contract_digest"] == G17_CONTRACT_DIGEST:
        expected_approval = G17_APPROVAL_DIGEST
    if current["contract_digest"] == G14_CONTRACT_DIGEST:
        expected_approval = G14_APPROVAL_DIGEST
    if current["contract_digest"] == G16_CONTRACT_DIGEST:
        expected_approval = G16_APPROVAL_DIGEST
    elif current["contract_digest"] == G15_CONTRACT_DIGEST:
        expected_approval = G15_APPROVAL_DIGEST
    if current["approval_digest"] != expected_approval:
        raise WorkflowError("V6 exact standing approval differs")
    actual_harness = harness()
    fixtures = {n: fixture_binding(files) for n, files in packets(source).items()}
    history = historical(repo)
    if semantics(policy) != history["stopped_v4"]["policy_semantics"]:
        raise WorkflowError("V6 frozen policy semantics differ")
    # This existing owned verifier checks BOTH lifetimes, adds300+60 margins,
    # and holds the original lock. No unowned fallback or lineage renewal.
    actual_auth = require(owned_auth).current_binding(900, remaining_seconds)
    if actual_auth != policy["authentication"]:
        raise WorkflowError("V6 authentication generation changed")
    result = {
        "authorization": current,
        "history": history,
        "harness": actual_harness,
        "policy": copy.deepcopy(policy),
        "fixtures": fixtures,
    }
    _binding(result)
    return result


def _binding(value):
    if type(value) is not dict or set(value) != {"authorization", "history", "harness", "policy", "fixtures"}:
        raise WorkflowError("Invalid V6 context shape")
    auth = value["authorization"]
    if (
        type(auth) is not dict
        or set(auth) != {"contract_digest", "approval_digest"}
        or auth["contract_digest"]
        not in (
            CONTRACT_DIGEST,
            NEXT_CONTRACT_DIGEST,
            G14_CONTRACT_DIGEST,
            G15_CONTRACT_DIGEST,
            G16_CONTRACT_DIGEST,
            G18_CONTRACT_DIGEST,
            G17_CONTRACT_DIGEST,
        )
        or (
            auth["contract_digest"] == NEXT_CONTRACT_DIGEST
            and auth["approval_digest"] != NEXT_APPROVAL_DIGEST
        )
        or (auth["contract_digest"] == G14_CONTRACT_DIGEST and auth["approval_digest"] != G14_APPROVAL_DIGEST)
        or (auth["contract_digest"] == G18_CONTRACT_DIGEST and auth["approval_digest"] != G18_APPROVAL_DIGEST)
        or (auth["contract_digest"] == G17_CONTRACT_DIGEST and auth["approval_digest"] != G17_APPROVAL_DIGEST)
        or (auth["contract_digest"] == G16_CONTRACT_DIGEST and auth["approval_digest"] != G16_APPROVAL_DIGEST)
        or (auth["contract_digest"] == G15_CONTRACT_DIGEST and auth["approval_digest"] != G15_APPROVAL_DIGEST)
        or not _hex(auth["approval_digest"])
    ):
        raise WorkflowError("Invalid V6 authority binding")
    harness = value["harness"]
    if (
        type(harness) is not dict
        or set(harness) != {"head", "files"}
        or not _hex(harness["head"], 40)
        or type(harness["files"]) is not dict
        or not harness["files"]
        or not all(type(k) is str and k and _hex(v) for k, v in harness["files"].items())
    ):
        raise WorkflowError("Invalid V6 source binding")
    if type(value["history"]) is not dict or not value["history"]:
        raise WorkflowError("Missing V6 inherited history")
    validate_policy(value["policy"])
    fixtures = value["fixtures"]
    if type(fixtures) is not dict or set(fixtures) != {str(n) for n in SEQUENCE}:
        raise WorkflowError("V6 requires the whole fixed four-fixture inventory")
    for item in fixtures.values():
        if (
            type(item) is not dict
            or set(item) != {"fixture_sha256", "descriptor_sha256"}
            or not all(_hex(v) for v in item.values())
        ):
            raise WorkflowError("Invalid V6 fixture binding")


def preview(repo, policy, *, name, tested_head, now=None, owned_auth=None):
    if root(repo).exists():
        raise WorkflowError("Prior V6 application cannot be repeated")
    start = clock(time.time() if now is None else now)
    binding = context(repo, policy, owned_auth=owned_auth)
    if tested_head != binding["harness"]["head"]:
        raise WorkflowError("V6 tested source differs")
    grant = {
        "schema_version": 6,
        "kind": "reporting-recovery-v6",
        "purpose": PURPOSE,
        "name": name,
        "not_before": start,
        "expires_at": start + 10800,
        "binding": copy.deepcopy(binding),
        "slots": [slot(n) for n in SEQUENCE],
        "limits": LIMITS.copy(),
        "stop_on_failure": True,
    }
    validate_grant(grant)
    return {"status": "preview", "grant": grant, "preview_digest": digest(grant)}


def validate_grant(grant):
    if type(grant) is not dict or set(grant) != {
        "schema_version",
        "kind",
        "purpose",
        "name",
        "not_before",
        "expires_at",
        "binding",
        "slots",
        "limits",
        "stop_on_failure",
    }:
        raise WorkflowError("Invalid V6 grant shape")
    _binding(grant["binding"])
    start, end = clock(grant["not_before"]), clock(grant["expires_at"])
    expected = {
        **grant,
        "schema_version": 6,
        "kind": "reporting-recovery-v6",
        "purpose": PURPOSE,
        "slots": [slot(n) for n in SEQUENCE],
        "limits": LIMITS.copy(),
        "stop_on_failure": True,
    }
    if (
        digest(expected) != digest(grant)
        or end != start + 10800
        or type(grant["name"]) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", grant["name"]) is None
    ):
        raise WorkflowError("V6 bounds or identity changed")


def apply(repo, proposal, *, preview_digest, now=None, owned_auth=None):
    started = clock(time.time() if now is None else now)
    # Even an empty or torn namespace is not silently adopted or reset.
    if root(repo).exists():
        raise WorkflowError("Prior V6 application cannot be repeated")
    if type(proposal) is not dict or set(proposal) != {"status", "grant", "preview_digest"}:
        raise WorkflowError("Invalid V6 preview")
    grant = proposal["grant"]
    validate_grant(grant)
    if (
        proposal["status"] != "preview"
        or proposal["preview_digest"] != preview_digest
        or digest(grant) != preview_digest
        or not grant["not_before"] <= started <= grant["expires_at"] - 7380
        or digest(context(repo, grant["binding"]["policy"], owned_auth=owned_auth))
        != digest(grant["binding"])
    ):
        raise WorkflowError("Stale V6 preview or context")
    checked = started if now is not None else clock(time.time())
    if not started <= checked <= min(grant["expires_at"] - 7380, started + 900):
        raise WorkflowError("V6 application checks exhausted the window")
    # Exclusive directory claim closes concurrent applications and torn writes.
    private_directory(root(repo).parent)
    try:
        root(repo).mkdir(mode=0o700)
    except FileExistsError:
        raise WorkflowError("Prior V6 application cannot be repeated") from None
    parent = os.open(root(repo).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    exclusive(
        root(repo) / "application.json",
        {
            "schema_version": 6,
            "grant_digest": digest(grant),
            "applied_at": started,
            "deadline": min(grant["expires_at"], started + 7380),
        },
    )
    exclusive(root(repo) / "grant.json", grant, limit=MAX_RECORD_BYTES)
    return {"status": "applied", "grant_digest": digest(grant)}


def load(repo):
    allowed = {"grant.json", "application.json"}
    allowed.update(f"{kind}-{n}.json" for n in SEQUENCE for kind in ("attempt", "outcome"))
    allowed.update(f"evidence-{n}" for n in SEQUENCE)
    if any(path.name not in allowed or path.is_symlink() for path in root(repo).iterdir()):
        raise WorkflowError("Unexpected V6 namespace material")
    grant, application = read(root(repo) / "grant.json"), read(root(repo) / "application.json")
    validate_grant(grant)
    applied = clock(application.get("applied_at"))
    expected = {
        "schema_version": 6,
        "grant_digest": digest(grant),
        "applied_at": applied,
        "deadline": min(grant["expires_at"], applied + 7380),
    }
    if (
        digest(expected) != digest(application)
        or not grant["not_before"] <= applied <= grant["expires_at"] - 7380
    ):
        raise WorkflowError("V6 application is torn or changed")
    return grant, application


def _reservation(grant, number, input_digest, started):
    allocation = slot(number)
    if not _hex(input_digest):
        raise WorkflowError("V6 requires an exact prepared input digest")
    return {
        "schema_version": 6,
        "number": number,
        "purpose": allocation["purpose"],
        "grant_digest": digest(grant),
        "input_digest": input_digest,
        "fixture": copy.deepcopy(grant["binding"]["fixtures"][str(number)]),
        "started": started,
        "deadline": started + allocation["native_seconds"] + 840,
        "replay_deadline": started + allocation["native_seconds"] + 840 + 180,
        "reserved_seconds": allocation["native_seconds"],
        "reserved_reference_usd": allocation["reference_usd"],
        "wrapper_processes": 1,
    }


def reserve(repo, *, number, input_digest, now=None, owned_auth=None):
    from reporting_recovery_history_v6 import known_usage

    slot(number)
    if not _hex(input_digest):
        raise WorkflowError("V6 requires an exact prepared input digest")
    started = clock(time.time() if now is None else now)
    grant, application = load(repo)
    if any(
        (root(repo) / f"{kind}-{n}.json").exists()
        for n in SEQUENCE
        if n >= number
        for kind in ("attempt", "outcome")
    ):
        raise WorkflowError("V6 replay or out-of-order reservation")
    previous_finished = application["applied_at"]
    for n in SEQUENCE:
        if n < number:
            previous = outcome(repo, n)  # Independently replay; a saved bool is insufficient.
            allocation = slot(n)
            if previous.get("qualified") is not True or not known_usage(
                previous.get("usage"), allocation["native_seconds"], allocation["reference_usd"]
            ):
                raise WorkflowError("V6 predecessor is incomplete or usage unknown")
            previous_finished = clock(previous["finished"])
    if not previous_finished <= started <= application["deadline"] - remaining(number):
        raise WorkflowError("V6 remaining allocation does not fit or clock rolled back")
    if digest(
        context(
            repo,
            grant["binding"]["policy"],
            remaining_seconds=application["deadline"] - started,
            owned_auth=owned_auth,
        )
    ) != digest(grant["binding"]):
        raise WorkflowError("V6 source, authority, history, fixtures or generation changed")
    checked = started if now is not None else clock(time.time())
    if not started <= checked <= application["deadline"] - remaining(number):
        raise WorkflowError("V6 reservation checks exhausted the window")
    # Fresh prerequisite checks consume this slot's local allocation too.
    record = _reservation(grant, number, input_digest, started)
    exclusive(root(repo) / f"attempt-{number}.json", record)
    return record


def evaluate(repo, grant, reservation, finished):
    """Recompute exact V6 diagnostic/capacity evidence within immutable bounds."""
    validate_grant(grant)
    number = reservation.get("number")
    slot(number)
    started, finished = clock(reservation.get("started")), clock(finished)
    loaded, application = load(repo)
    if (
        digest(loaded) != digest(grant)
        or digest(reservation)
        != digest(_reservation(grant, number, reservation.get("input_digest"), started))
        or not application["applied_at"]
        <= started
        <= finished
        <= min(application["deadline"], reservation["replay_deadline"])
    ):
        raise WorkflowError("V6 reservation, completion window or grant changed")
    from reporting_diagnostic_v6 import replay

    return replay(repo, grant, reservation, finished)


def outcome(repo, number):
    slot(number)
    grant, _ = load(repo)
    saved = read(root(repo) / f"outcome-{number}.json")
    expected = evaluate(repo, grant, read(root(repo) / f"attempt-{number}.json"), saved.get("finished"))
    if digest(expected) != digest(saved):
        raise WorkflowError("V6 saved outcome differs from independent replay")
    return expected


def complete(repo, *, number, now=None):
    slot(number)
    grant, _ = load(repo)
    record = evaluate(
        repo, grant, read(root(repo) / f"attempt-{number}.json"), clock(time.time() if now is None else now)
    )
    exclusive(root(repo) / f"outcome-{number}.json", record)
    return record
