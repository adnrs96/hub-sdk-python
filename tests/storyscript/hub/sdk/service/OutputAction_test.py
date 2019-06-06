import json

from storyscript.hub.sdk.service.Argument import Argument
from storyscript.hub.sdk.service.OutputAction import OutputAction
from storyscript.hub.sdk.service.HttpOptions import HttpOptions

output_action_fixture = {
    "name": "write",
    "output_action": {
        "http": {
            "path": "/digest",
            "port": 8080,
            "method": "post",
            "contentType": "application/json",
            "use_event_conn": True,
            "subscribe": {
                "path": "/stream/subscribe",
                "method": "post",
                "contentType": "application/json"
            },
            "unsubscribe": {
                "path": "/stream/unsubscribe",
                "method": "post"
            }
        },
        "arguments": {
            "flush": {
                "in": "responseBody",
                "type": "boolean",
                "required": False
            }
        }
    }
}

output_action_fixture_json = json.dumps(output_action_fixture)


def test_deserialization(mocker):
    mocker.patch.object(json, 'loads', return_value=output_action_fixture)

    mocker.patch.object(HttpOptions, 'from_dict')
    mocker.patch.object(Argument, 'from_dict')

    assert OutputAction.from_json(jsonstr=output_action_fixture_json) is not None

    json.loads.assert_called_with(output_action_fixture_json)

    HttpOptions.from_dict.assert_called_with(data={
        "http_options": output_action_fixture["output_action"]["http"]
    })

    Argument.from_dict.assert_called_with(data={
        "name": "flush",
        "argument": output_action_fixture["output_action"]["arguments"]["flush"]
    })


def test_serialization(mocker):
    mocker.patch.object(json, 'dumps', return_value=output_action_fixture_json)

    service_event = OutputAction.from_dict(data=output_action_fixture)

    assert service_event.as_json(compact=True) is not None
    json.dumps.assert_called_with(output_action_fixture, sort_keys=True)

    assert service_event.as_json() is not None
    json.dumps.assert_called_with(output_action_fixture, indent=4, sort_keys=True)
