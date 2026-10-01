"""WecomCliSmartTableAdapter 的 CLI 契约测试。"""

from __future__ import annotations

import json
import logging
import subprocess
from collections.abc import Mapping, Sequence

import pytest

from app.core.logging import JsonFormatter
from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapterConfigurationError,
    SmartTablePermissionError,
)
from app.smart_table.models import (
    SmartTableField,
    SmartTableFieldType,
    SmartTableOption,
    SmartTableSchema,
)
from app.smart_table.wecom_cli import (
    WecomCliProcessError,
    WecomCliSmartTableAdapter,
)


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


def _adapter(
    fake_cli: FakeCli, *, retry_count: int = 1, sheet_title: str | None = None
) -> WecomCliSmartTableAdapter:
    """构造使用假 CLI 的真实适配器实例。

    参数：fake_cli 为注入的命令执行器；retry_count 控制重试次数。
    返回：绑定稳定测试表标识的适配器。
    异常：无。
    副作用：无。
    """
    return WecomCliSmartTableAdapter(
        doc_id="test-doc",
        sheet_id="test-sheet",
        sheet_title=sheet_title,
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


def _wire_value(value: object) -> str:
    """按 wecom-cli 1.3.4 values map 契约序列化单元格值。"""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


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
            {"field_id": "remarks", "field_title": "备注", "field_type": "text"},
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
                "*业务线": _wire_value("协作机器人"),
                "*联系人": _wire_value("张三"),
                "*手机": _wire_value("13800138000"),
                "*线索名称": _wire_value("长广溪智造"),
                "客户行业": _wire_value("机械加工"),
                "AI待确认": _wire_value([{"id": "pending-business-line", "text": "*业务线"}]),
                "创建人": _wire_value([{"userId": "sales-1"}]),
                "负责人": _wire_value([{"userId": "sales-1"}]),
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


def test_record_add_normalizes_phone_and_skips_unrepresentable_location() -> None:
    """验证恢复记录时不会因格式化座机或纯文本位置阻断整行创建。

    参数：无。
    返回值：无。
    异常：字段转换错误或把不合法位置值发送给 CLI 时由 pytest 报告。
    副作用：仅消费 Fake CLI 响应并检查待发送 payload，不访问企业微信。
    """
    fake_cli = FakeCli(
        [
            {
                "errcode": 0,
                "fields": [
                    {"field_id": "phone", "field_title": "电话", "field_type": "phone_number"},
                    {
                        "field_id": "location",
                        "field_title": "地区定位",
                        "field_type": "location",
                    },
                    {"field_id": "owner", "field_title": "负责人", "field_type": "user"},
                ],
            },
            {"errcode": 0, "records": [{"record_id": "record-1", "values": {}}]},
        ]
    )

    _adapter(fake_cli).create_record(
        {
            "电话": "0510-83480979-917",
            "地区定位": "江苏省无锡市滨湖区",
            "负责人": "sales-1",
        },
        actor=SmartTableActor.ROBOT,
    )

    payload = _payload(fake_cli.calls[1])
    assert payload["records"] == [
        {
            "values": {
                "电话": _wire_value("051083480979917"),
                "负责人": _wire_value([{"userId": "sales-1"}]),
            }
        }
    ]


def test_get_record_falls_back_to_recent_create_when_list_is_eventually_consistent() -> None:
    """验证写入成功但列表暂未反映新行时，读取可使用本进程的创建快照。

    参数：无。
    返回值：无。
    异常：新建记录被误判为不存在时由 pytest 报告断言失败。
    副作用：模拟 records add 已成功而紧随其后的 records list 暂时为空。
    """
    fake_cli = FakeCli(
        [
            _field_response(),
            {"errcode": 0, "records": [{"record_id": "record-new", "values": {}}]},
            {"errcode": 0, "records": []},
        ]
    )
    adapter = _adapter(fake_cli)

    created = adapter.create_record({"负责人": "sales-1"}, actor=SmartTableActor.ROBOT)
    reread = adapter.get_record(created.record_id)

    assert reread is not None
    assert reread.record_id == created.record_id
    assert reread.fields == {"负责人": "sales-1"}


def test_recent_create_fallback_keeps_only_fields_sent_for_the_new_record() -> None:
    """验证新增响应夹带旧行字段时，不会污染列表暂未可见的新行快照。

    参数：无。
    返回值：无。
    异常：旧行联系人被缓存为新行字段时由 pytest 报告断言失败。
    副作用：模拟 records add 返回正确新行标识但 values 含旧行字段的外部异常响应。
    """
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "records": [
                    {
                        "record_id": "record-new",
                        "values": {"*联系人": "线索A联系人"},
                    }
                ],
            },
            {"errcode": 0, "records": []},
        ]
    )
    adapter = _adapter(fake_cli)

    created = adapter.create_record({"负责人": "sales-1"}, actor=SmartTableActor.ROBOT)
    reread = adapter.get_record(created.record_id)

    assert reread is not None
    assert reread.fields == {"负责人": "sales-1"}


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


def test_get_record_converts_cli_cell_values_to_canonical_domain_values() -> None:
    """验证文本、单选和多选 CellValue 均不泄露给领域层。

    参数：无。返回值：无。异常：断言失败时由 pytest 报告。副作用：消费 FakeCli 字段和记录读取响应。
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
                            "*联系人": [{"text": "张三"}],
                            "*业务线": [{"text": "协作机器人"}],
                            "AI待确认": [
                                {"id": "pending-business-line", "text": "*业务线"},
                                {"id": "pending-contact", "text": "*联系人"},
                            ],
                        },
                    }
                ],
            },
        ]
    )

    record = _adapter(fake_cli).get_record("record-1")

    assert record is not None
    assert record.fields == {
        "联系人": "张三",
        "业务线": "协作机器人",
        "AI待确认": ["业务线", "联系人"],
    }


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("*联系人", []),
        ("*业务线", []),
    ],
)
def test_get_record_normalizes_empty_text_and_single_select_cells_to_none(
    field_name: str, value: object
) -> None:
    """验证文本和单选的空 CLI CellValue 数组回读为领域空值。

    参数：field_name 为真实字段标题；value 为 CLI 返回的空数组。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：消费 FakeCli 的字段和记录读取响应。
    """
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "records": [{"record_id": "record-1", "values": {field_name: value}}],
            },
        ]
    )

    record = _adapter(fake_cli).get_record("record-1")

    assert record is not None
    assert record.fields[field_name.removeprefix("*")] is None


def test_get_record_joins_multiple_text_cells_in_server_order() -> None:
    """验证文本字段的多个 CLI CellValue 按服务端顺序还原为一段文本。

    参数：无。
    返回：无。
    异常：WecomCliSmartTableAdapterError 为受控外部协议失败。
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
                        "values": {"*联系人": [{"text": "A"}, {"text": "B"}]},
                    }
                ],
            },
        ]
    )

    record = _adapter(fake_cli).get_record("record-1")
    assert record is not None
    assert record.fields == {"联系人": "AB"}


def test_get_record_ignores_historical_multi_cell_text_when_target_is_valid() -> None:
    """验证历史多片段文本不会阻断目标记录的单条回读。

    参数：无。
    返回：无。
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
                        "record_id": "historical-record",
                        "values": {
                            "备注": [
                                {"text": "片段一", "type": "text"},
                                {"text": "片段二", "type": "text"},
                                {"text": "片段三", "type": "text"},
                                {"text": "片段四", "type": "text"},
                            ]
                        },
                    },
                    {"record_id": "target-record", "values": {"备注": [{"text": "正常备注"}]}},
                ],
            },
        ]
    )

    record = _adapter(fake_cli).get_record("target-record")

    assert record is not None
    assert record.record_id == "target-record"
    assert record.fields == {"备注": "正常备注"}


def test_get_records_joins_historical_multi_cell_text() -> None:
    """验证全量回读可以保留历史多片段文本，不阻断其他记录。

    参数：无。
    返回：无。
    异常：WecomCliSmartTableAdapterError 为受控外部协议失败。
    副作用：消费 FakeCli 的字段和记录读取响应。
    """
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "records": [
                    {
                        "record_id": "historical-record",
                        "values": {
                            "备注": [
                                {"text": "片段一", "type": "text"},
                                {"text": "片段二", "type": "text"},
                                {"text": "片段三", "type": "text"},
                                {"text": "片段四", "type": "text"},
                            ]
                        },
                    },
                    {"record_id": "target-record", "values": {"备注": [{"text": "正常备注"}]}},
                ],
            },
        ]
    )

    records = _adapter(fake_cli).get_records()
    assert records[0].fields == {"备注": "片段一片段二片段三片段四"}
    assert records[1].fields == {"备注": "正常备注"}


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
    assert records[0].member_names == {"负责人": "测试销售"}
    create_payload = _payload(fake_cli.calls[3])
    assert create_payload["records"] == [
        {
            "values": {
                "创建人": _wire_value([{"userId": "sales-user"}]),
                "负责人": _wire_value([{"userId": "sales-user"}]),
                "*线索名称": _wire_value("受控测试"),
            }
        }
    ]
    update_payload = _payload(fake_cli.calls[5])
    assert update_payload["records"] == [
        {"record_id": "record-3", "values": {"工艺": _wire_value("装配")}}
    ]
    assert updated.fields == {"工艺": "装配"}


def test_write_cells_match_wecom_cli_134_json_string_values_contract() -> None:
    """验证 1.3.4 的 values map 每个字段值都是 JSON 序列化字符串。"""
    schema = SmartTableSchema(
        fields=(
            SmartTableField("name", "线索名称", SmartTableFieldType.TEXT),
            SmartTableField("remark", "备注", SmartTableFieldType.LONG_TEXT),
            SmartTableField("phone", "手机", SmartTableFieldType.PHONE_NUMBER),
            SmartTableField("email", "邮箱", SmartTableFieldType.EMAIL),
            SmartTableField("date", "下次联系时间", SmartTableFieldType.DATE),
            SmartTableField(
                "line",
                "业务线",
                SmartTableFieldType.SINGLE_SELECT,
                (SmartTableOption("line-id", "协作机器人"),),
            ),
            SmartTableField(
                "process",
                "工艺",
                SmartTableFieldType.MULTI_SELECT,
                (SmartTableOption("process-id", "装配"),),
            ),
            SmartTableField("owner", "负责人", SmartTableFieldType.MEMBER),
            SmartTableField(
                "pending",
                "AI待确认",
                SmartTableFieldType.MULTI_SELECT,
                (SmartTableOption("pending-id", "线索名称"),),
            ),
        )
    )
    fields = {
        "线索名称": "公司样例",
        "备注": "备注样例",
        "手机": "138 0013 8000",
        "邮箱": "sales@example.com",
        "下次联系时间": "2026-10-02T01:00:00Z",
        "业务线": "协作机器人",
        "工艺": ["装配"],
        "负责人": "sales-user",
        "AI待确认": ["线索名称"],
    }

    encoded = _adapter(FakeCli([]))._to_cli_fields(fields, schema)

    assert all(isinstance(value, str) for value in encoded.values())
    assert {name: json.loads(value) for name, value in encoded.items()} == {
        "线索名称": "公司样例",
        "备注": "备注样例",
        "手机": "13800138000",
        "邮箱": "sales@example.com",
        "下次联系时间": "2026-10-02 09:00:00",
        "业务线": [{"id": "line-id", "text": "协作机器人"}],
        "工艺": [{"id": "process-id", "text": "装配"}],
        "负责人": [{"userId": "sales-user"}],
        "AI待确认": [{"id": "pending-id", "text": "线索名称"}],
    }
    date_only = _adapter(FakeCli([]))._to_cli_fields(
        {"下次联系时间": "2026-10-03"}, schema
    )
    assert json.loads(date_only["下次联系时间"]) == "2026-10-03 00:00:00"
    # 无法无歧义解析的自然语言日期只跳过该字段，不阻断同一增量补丁。
    invalid_date = _adapter(FakeCli([]))._to_cli_fields(
        {"线索名称": "公司样例", "下次联系时间": "下周联系"}, schema
    )
    assert json.loads(invalid_date["线索名称"]) == "公司样例"
    assert "下次联系时间" not in invalid_date


def test_configured_sheet_title_uses_full_query_and_parses_member_rows() -> None:
    """验证配置子表名称后读取完整查询结果，不受 records list 可见范围影响。"""
    fake_cli = FakeCli(
        [
            _field_response(),
            {
                "errcode": 0,
                "values": [
                    {
                        "rows": [
                            {
                                "RECORD_ID": "record-full",
                                "*线索名称": "完整查询线索",
                                "AI待确认": None,
                                "创建人": [{"id": "sales-user", "name": "测试销售"}],
                                "负责人": [{"id": "sales-user", "name": "测试销售"}],
                            }
                        ]
                    }
                ],
            },
        ]
    )
    records = _adapter(fake_cli, sheet_title="CRM线索").get_records()

    assert records[0].record_id == "record-full"
    assert records[0].fields["线索名称"] == "完整查询线索"
    assert records[0].fields["负责人"] == "sales-user"
    assert records[0].member_names == {"创建人": "测试销售", "负责人": "测试销售"}
    assert records[0].fields["AI待确认"] == []
    assert fake_cli.calls[1][0:5] == ("wecom-cli", "smartsheet", "records", "query", "--docid")


def test_empty_multi_select_string_is_normalized_to_empty_list() -> None:
    """验证真实 CLI 对空多选返回空字符串时不会被误判为协议错误。"""
    field = SmartTableField(
        field_id="pending",
        name="AI待确认",
        field_type=SmartTableFieldType.MULTI_SELECT,
    )

    value = WecomCliSmartTableAdapter._from_cli_value("AI待确认", field, "")

    assert value == []


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


def test_idempotent_cli_network_process_error_is_retried() -> None:
    """验证明确网络错误的幂等读取会有限重试。"""
    fake_cli = FakeCli(
        [
            WecomCliProcessError(
                "wecom-cli 进程调用失败：network_error",
                error_code="network_error",
                external_error_code=893101,
                external_error_type="NetworkError",
            ),
            {"errcode": 0, "fields": []},
        ]
    )

    assert _adapter(fake_cli).get_schema().fields == ()
    assert len(fake_cli.calls) == 2


def test_permanent_cli_process_error_is_not_retried() -> None:
    """验证 CLI 明确报告权限或参数错误时不重复提交同一更新。"""
    fake_cli = FakeCli(
        [
            WecomCliProcessError("wecom-cli 退出失败", error_code="permission_denied"),
            {"errcode": 0, "fields": []},
        ]
    )

    with pytest.raises(WecomCliProcessError):
        _adapter(fake_cli).get_schema()
    assert len(fake_cli.calls) == 1


def test_subprocess_failure_keeps_only_controlled_error_code(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """验证 stdout 结构化权限错误只保留白名单元数据。"""

    def failed_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        """返回包含敏感片段的模拟 CLI 失败结果。"""
        return subprocess.CompletedProcess(
            args=["wecom-cli"],
            returncode=1,
            stdout=(
                '{"errcode":851003,"errmsg":"private response text",'
                '"body":"private body","docid":"private-doc"}'
            ),
            stderr="private stderr token=secret-value",
        )

    monkeypatch.setattr(subprocess, "run", failed_run)
    caplog.set_level(logging.ERROR)

    with pytest.raises(WecomCliProcessError) as error:
        _adapter(FakeCli([]))._run_subprocess(("wecom-cli", "--json", "{}"))

    assert error.value.error_code == "permission_denied"
    assert error.value.external_error_code == 851003
    assert error.value.external_error_type is None
    assert "private response text" not in str(error.value)
    assert "private body" not in caplog.text
    assert "private-doc" not in caplog.text
    assert "secret-value" not in caplog.text
    assert caplog.records[-1].external_error_code == 851003
    formatted_log = JsonFormatter().format(caplog.records[-1])
    assert '"external_error_code": 851003' in formatted_log
    assert "private response text" not in formatted_log


def test_subprocess_network_error_parses_only_safe_stdout_fields(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """验证嵌套网络错误映射为暂态类别且不泄露远端正文。"""

    def failed_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        """返回带有敏感正文的结构化模拟网络失败。"""
        return subprocess.CompletedProcess(
            args=["wecom-cli"],
            returncode=1,
            stdout=(
                '{"error":{"type":"NetworkError","code":893101,'
                '"message":"private response","body":"private body"}}'
            ),
            stderr="ignored private stderr",
        )

    monkeypatch.setattr(subprocess, "run", failed_run)
    caplog.set_level(logging.ERROR)

    with pytest.raises(WecomCliProcessError) as error:
        _adapter(FakeCli([]))._run_subprocess(("wecom-cli", "--json", "{}"))

    assert error.value.error_code == "network_error"
    assert error.value.external_error_code == 893101
    assert error.value.external_error_type == "NetworkError"
    assert "private response" not in str(error.value)
    assert "private body" not in caplog.text
    assert "ignored private stderr" not in caplog.text


def test_non_json_stdout_falls_back_to_limited_stderr_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 stdout 非 JSON 时仍使用 stderr 的有限权限分类。"""

    def failed_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        """返回非 JSON stdout 和可识别的受控权限提示。"""
        return subprocess.CompletedProcess(
            args=["wecom-cli"],
            returncode=1,
            stdout="not-json private payload",
            stderr="permission denied token=private",
        )

    monkeypatch.setattr(subprocess, "run", failed_run)

    with pytest.raises(WecomCliProcessError) as error:
        _adapter(FakeCli([]))._run_subprocess(("wecom-cli", "--json", "{}"))

    assert error.value.error_code == "permission_denied"
    assert error.value.external_error_code is None
    assert "private" not in str(error.value)


def test_unclassified_nonzero_exit_is_permanent_and_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证未知 stdout/stderr 只归为 process_exit，不被误判为暂态重试。"""

    subprocess_calls = 0

    def failed_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        """返回没有已知结构或 stderr 分类的进程错误。"""
        nonlocal subprocess_calls
        subprocess_calls += 1
        return subprocess.CompletedProcess(
            args=["wecom-cli"], returncode=1, stdout="{\"other\":true}", stderr="unclassified"
        )

    monkeypatch.setattr(subprocess, "run", failed_run)
    adapter = WecomCliSmartTableAdapter(
        doc_id="test-doc", sheet_id="test-sheet", retry_count=1
    )

    with pytest.raises(WecomCliProcessError) as error:
        adapter.get_schema()

    assert error.value.error_code == "process_exit"
    assert subprocess_calls == 1


def test_http_status_controls_process_error_retry() -> None:
    """验证只有明确可重试 HTTP 状态允许幂等 CLI 调用重放。"""
    retryable = FakeCli(
        [
            WecomCliProcessError("HTTP error", error_code="http_error", http_status=503),
            {"errcode": 0, "fields": []},
        ]
    )
    permanent = FakeCli(
        [WecomCliProcessError("HTTP error", error_code="http_error", http_status=403)]
    )

    assert _adapter(retryable).get_schema().fields == ()
    assert len(retryable.calls) == 2
    with pytest.raises(WecomCliProcessError):
        _adapter(permanent).get_schema()
    assert len(permanent.calls) == 1


def test_update_declares_field_title_key_type() -> None:
    """验证更新请求显式声明使用字段标题，避免 CLI 默认键类型漂移。"""
    fake_cli = FakeCli(
        [
            _field_response(),
            {"errcode": 0, "records": [{"record_id": "record-1", "values": {}}]},
            {"errcode": 0, "records": [{"record_id": "record-1", "values": {}}]},
            {"errcode": 0, "records": [{"record_id": "record-1", "values": {}}]},
        ]
    )

    _adapter(fake_cli).update_record("record-1", {"备注": "更新后的备注"})

    payload = _payload(fake_cli.calls[-1])
    assert payload["key_type"] == "CELL_VALUE_KEY_TYPE_FIELD_TITLE"


def test_no_authority_cli_failure_is_classified_as_permission_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证企业规模权限错误进入受控权限分类，不被当作暂态进程错误。"""

    def failed_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        """返回企业微信明确的权限失败片段。"""
        return subprocess.CompletedProcess(
            args=["wecom-cli"],
            returncode=1,
            stdout="",
            stderr="errcode=851003 errmsg=no authority",
        )

    monkeypatch.setattr(subprocess, "run", failed_run)

    with pytest.raises(WecomCliProcessError) as error:
        _adapter(FakeCli([]))._run_subprocess(("wecom-cli", "--json", "{}"))

    assert error.value.error_code == "permission_denied"


def test_cli_process_exit_during_add_is_not_retried() -> None:
    """验证新增记录遇到未知进程退出时不重放，避免服务端已成功而本地重复建行。"""
    fake_cli = FakeCli(
        [
            _field_response(),
            WecomCliProcessError("wecom-cli 退出失败，退出码：1"),
        ]
    )

    with pytest.raises(WecomCliProcessError):
        _adapter(fake_cli).create_record(
            {"负责人": "sales-1"},
            actor=SmartTableActor.ROBOT,
        )
    assert len(fake_cli.calls) == 2
