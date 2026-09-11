"""WecomCliSmartTableAdapter 的 CLI 契约测试。"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence

import pytest

from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapterConfigurationError,
    SmartTablePermissionError,
)
from app.smart_table.models import SmartTableFieldType
from app.smart_table.wecom_cli import WecomCliSmartTableAdapter


class FakeCli:
    """按调用顺序提供固定 CLI JSON 响应，并保存请求参数。"""

    def __init__(self, responses: list[Mapping[str, object] | BaseException]) -> None:
        """初始化响应队列和空调用记录。

        参数：responses 为每次 CLI 调用的成功响应或待抛出的异常。
        异常：无。
        副作用：保存可被测试断言的 CLI 参数序列。
        """
        self._responses = responses
        self.calls: list[Sequence[str]] = []

    def __call__(self, arguments: Sequence[str]) -> Mapping[str, object]:
        """记录一次 CLI 调用并返回下一项预设响应。

        参数：arguments 为无 shell 的 CLI 参数序列。
        返回：预置 CLI JSON 响应。
        异常：队列耗尽或预置异常时抛出对应异常。
        副作用：追加调用参数并消费一个队列项。
        """
        self.calls.append(arguments)
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _adapter(fake_cli: FakeCli, *, retry_count: int = 1) -> WecomCliSmartTableAdapter:
    """构造使用假 CLI 的真实适配器实例。

    参数：fake_cli 为注入的命令执行器；retry_count 控制重试次数。
    返回：绑定稳定测试表标识的适配器。
    异常：无。
    副作用：无。
    """
    return WecomCliSmartTableAdapter(
        doc_id="test-doc",
        sheet_id="test-sheet",
        sales_can_create_records=False,
        sales_can_delete_records=False,
        retry_count=retry_count,
        runner=fake_cli,
    )


def _payload(arguments: Sequence[str]) -> dict[str, object]:
    """从 CLI 参数中取得经 JSON 编码的请求体。

    参数：arguments 为一次已记录的 CLI 参数序列。
    返回：可供精确断言的请求对象。
    异常：缺少 --json 或 JSON 不是对象时由测试失败。
    副作用：无。
    """
    value = json.loads(arguments[arguments.index("--json") + 1])
    assert isinstance(value, dict)
    return value


def _field_response() -> Mapping[str, object]:
    """构造包含管理员必填前缀和 AI待确认选项的真实字段结构。

    返回值：模拟 fields list 的成功响应。
    异常：无。
    副作用：无。
    """
    return {
        "errcode": 0,
        "fields": [
            {
                "field_id": "business-line",
                "field_title": "*业务线",
                "field_type": "single_select",
            },
            {"field_id": "contact", "field_title": "*联系人", "field_type": "text"},
            {"field_id": "phone", "field_title": "*手机", "field_type": "phone_number"},
            {"field_id": "lead-name", "field_title": "*线索名称", "field_type": "text"},
            {"field_id": "industry", "field_title": "客户行业", "field_type": "single_select"},
            {"field_id": "process", "field_title": "工艺", "field_type": "single_select"},
            {
                "field_id": "pending",
                "field_title": "AI待确认",
                "field_type": "select",
                "property_select": {
                    "options": [
                        {"id": "pending-business-line", "text": "*业务线"},
                        {"id": "pending-contact", "text": "*联系人"},
                    ]
                },
            },
            {"field_id": "creator", "field_title": "创建人", "field_type": "user"},
            {"field_id": "owner", "field_title": "负责人", "field_type": "user"},
        ],
    }


def test_canonical_fields_and_pending_options_are_mapped_to_real_schema_names() -> None:
    """验证业务层字段和 AI待确认选项写入时均转换为管理员实际显示名。

    参数：无。
    返回值：无。
    异常：断言失败时由 pytest 报告。
    副作用：消费 FakeCli 响应并检查发送给 CLI 的字段补丁。
    """
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "records": [
                    {
                        "record_id": "record-1",
                        "values": {
                            "*业务线": "协作机器人",
                            "*联系人": "张三",
                            "*手机": "13800138000",
                            "*线索名称": "长广溪智造",
                            "客户行业": "机械加工",
                            "AI待确认": [
                                {"id": "pending-business-line", "text": "*业务线"}
                            ],
                            "创建人": [{"userId": "sales-1", "userName": "销售一"}],
                            "负责人": [{"userId": "sales-1", "userName": "销售一"}],
                        },
                    }
                ],
            },
        ]
    )

    record = _adapter(fake_cli).create_record(
        {
            "业务线": "协作机器人",
            "联系人": "张三",
            "手机": "13800138000",
            "线索名称": "长广溪智造",
            "客户行业": "机械加工",
            "AI待确认": ["业务线"],
            "创建人": "sales-1",
            "负责人": "sales-1",
        },
        actor=SmartTableActor.ROBOT,
    )

    create_payload = _payload(fake_cli.calls[1])
    assert create_payload["records"] == [
        {
            "values": {
                "*业务线": "协作机器人",
                "*联系人": "张三",
                "*手机": "13800138000",
                "*线索名称": "长广溪智造",
                "客户行业": "机械加工",
                "AI待确认": [{"id": "pending-business-line", "text": "*业务线"}],
                "创建人": [{"userId": "sales-1"}],
                "负责人": [{"userId": "sales-1"}],
            }
        }
    ]
    assert record.fields == {
        "业务线": "协作机器人",
        "联系人": "张三",
        "手机": "13800138000",
        "线索名称": "长广溪智造",
        "客户行业": "机械加工",
        "AI待确认": ["业务线"],
        "创建人": "sales-1",
        "负责人": "sales-1",
    }


def test_get_record_restores_canonical_names_before_t09_compares_ai_values() -> None:
    """验证带星显示名回读为规范名，供 T09 与字段来源安全比较。

    参数：无。
    返回值：无。
    异常：断言失败时由 pytest 报告。
    副作用：消费 FakeCli 的字段和记录读取响应。
    """
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "records": [
                    {
                        "record_id": "record-1",
                        "values": {"*线索名称": "长广溪智造", "客户行业": "机械加工"},
                    }
                ],
            },
        ]
    )

    record = _adapter(fake_cli).get_record("record-1")

    assert record is not None
    assert record.fields["线索名称"] == "长广溪智造"
    assert record.fields["客户行业"] == "机械加工"
    assert "*线索名称" not in record.fields


def test_schema_reads_pages_and_preserves_real_option_identifiers() -> None:
    """验证字段分页读取保留字段类型和服务端 option ID。

    参数：无。返回：无。异常：断言失败时由 pytest 报告。副作用：消费 FakeCli 响应并记录调用。
    """
    fake_cli = FakeCli(
        [
            {
                "errcode": 0,
                "fields": [
                    {
                        "field_id": "field-1",
                        "field_title": "业务线",
                        "field_type": "single_select",
                        "property_single_select": {
                            "options": [{"id": "option-robot", "text": "协作机器人"}]
                        },
                    }
                ],
                "next_cursor": "cursor-2",
            },
            {
                "errcode": 0,
                "fields": [
                    {"field_id": "field-2", "field_title": "负责人", "field_type": "user"}
                ],
            },
        ]
    )

    schema = _adapter(fake_cli).get_schema()

    assert schema.fields[0].field_type is SmartTableFieldType.SINGLE_SELECT
    assert schema.fields[0].options[0].option_id == "option-robot"
    assert schema.fields[1].field_type is SmartTableFieldType.MEMBER
    assert _payload(fake_cli.calls[1])["cursor"] == "cursor-2"


def test_records_are_paginated_and_robot_writes_only_given_field_patch() -> None:
    """验证 records list 分页，以及 create/update 只发送经过映射的字段补丁。

    参数：无。返回：无。异常：断言失败时由 pytest 报告。副作用：消费 FakeCli 响应并记录调用。
    """
    member = [{"userId": "sales-user", "userName": "测试销售"}]
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "records": [{"record_id": "record-1", "values": {"负责人": member}}],
                "next_cursor": "record-page-2",
            },
            {"errcode": 0, "records": [{"record_id": "record-2", "values": {"工艺": "装配"}}]},
            {"errcode": 0, "records": [{"record_id": "record-3", "values": {"负责人": member}}]},
            {"errcode": 0, "records": [{"record_id": "record-3", "values": {"负责人": member}}]},
            {"errcode": 0, "records": [{"record_id": "record-3", "values": {"工艺": "装配"}}]},
        ]
    )
    adapter = _adapter(fake_cli)

    records = adapter.get_records()
    created = adapter.create_record(
        {"创建人": "sales-user", "负责人": "sales-user", "线索名称": "受控测试"},
        actor=SmartTableActor.ROBOT,
    )
    updated = adapter.update_record(created.record_id, {"工艺": "装配"})

    assert [record.record_id for record in records] == ["record-1", "record-2"]
    assert records[0].fields["负责人"] == "sales-user"
    create_payload = _payload(fake_cli.calls[3])
    assert create_payload["records"] == [
        {
            "values": {
                "创建人": [{"userId": "sales-user"}],
                "负责人": [{"userId": "sales-user"}],
                "*线索名称": "受控测试",
            }
        }
    ]
    update_payload = _payload(fake_cli.calls[5])
    assert update_payload["records"] == [{"record_id": "record-3", "values": {"工艺": "装配"}}]
    assert updated.fields == {"工艺": "装配"}


def test_robot_requires_owner_and_sales_cannot_be_impersonated() -> None:
    """验证真实 CLI 适配器保留机器人新增和销售权限语义。

    参数：无。返回：无。异常：预期权限异常由 pytest 捕获。副作用：无外部调用。
    """
    adapter = _adapter(FakeCli([]))

    with pytest.raises(ValueError, match="负责人"):
        adapter.create_record({"线索名称": "受控测试"}, actor=SmartTableActor.ROBOT)
    with pytest.raises(SmartTablePermissionError, match="不能模拟销售"):
        adapter.create_record({"负责人": [{"userName": "测试销售"}]}, actor=SmartTableActor.SALES)


def test_permissions_require_administrator_verified_snapshot() -> None:
    """验证 CLI 无权限读取接口时不会把历史 PoC 伪装成实时结果。

    参数：无。返回：无。异常：预期配置异常由 pytest 捕获。副作用：无外部调用。
    """
    adapter = WecomCliSmartTableAdapter(
        doc_id="test-doc",
        sheet_id="test-sheet",
        runner=FakeCli([]),
    )

    with pytest.raises(SmartTableAdapterConfigurationError, match="权限读取接口"):
        adapter.get_permissions()


def test_transient_network_error_retries_once_without_replaying_business_error() -> None:
    """验证仅 NetworkError 触发有限重试，成功响应随后正常返回。

    参数：无。返回：无。异常：断言失败时由 pytest 报告。副作用：消费 FakeCli 响应并记录调用。
    """
    fake_cli = FakeCli(
        [
            {"error": {"type": "NetworkError"}},
            {"errcode": 0, "fields": []},
        ]
    )

    assert _adapter(fake_cli).get_schema().fields == ()
    assert len(fake_cli.calls) == 2


def test_subprocess_timeout_is_retried_once() -> None:
    """验证 subprocess 超时遵循配置的有限重试次数。

    参数：无。返回：无。异常：断言失败时由 pytest 报告。副作用：消费 FakeCli 响应并记录调用。
    """
    fake_cli = FakeCli(
        [
            subprocess.TimeoutExpired(cmd="wecom-cli", timeout=1),
            {"errcode": 0, "fields": []},
        ]
    )

    assert _adapter(fake_cli).get_schema().fields == ()
    assert len(fake_cli.calls) == 2
