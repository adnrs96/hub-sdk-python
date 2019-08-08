from storyhub.sdk.service.Argument import Argument
from storyhub.sdk.service.Event import Event
from storyhub.sdk.service.HttpOptions import HttpOptions
from storyhub.sdk.service.Output import Output
from storyhub.sdk.service.ServiceObject import ServiceObject


class Action(ServiceObject):
    """
    A service action that exposes events.
    """

    def __init__(self, name, help_, args, events, http_options, output, data):
        super().__init__(data=data)

        self._name = name
        self._help = help_
        self._args = args
        self._events = events
        self._http_options = http_options
        self._output = output

    @classmethod
    def from_dict(cls, data):
        name = data["name"]
        action = data["action"]

        if isinstance(action, str):
            return cls(
                name=name,
                help_=action,
                args={},
                events={},
                http_options=None,
                output=None,
                data=data
            )

        args = {}
        if 'arguments' in action:
            for arg_name, arg in action['arguments'].items():
                args[arg_name] = Argument.from_dict(data={
                    "name": arg_name,
                    "argument": arg
                })

        events = {}
        if 'events' in action:
            for event_name, event in action['events'].items():
                events[event_name] = Event.from_dict(data={
                    "name": event_name,
                    "event": event
                })

        http_options = action.get(
            'http', None
        )

        if http_options is not None:
            http_options = HttpOptions.from_dict(data={
                "http_options": http_options
            })

        output = None
        if 'output' in action:
            output = Output.from_dict(data={
                "output": action["output"]
            })

        return cls(
            name=name,
            help_=action.get(
                'help_', 'No help_ available'
            ),
            args=args,
            events=events,
            http_options=http_options,
            output=output,
            data=data
        )

    def name(self):
        return self._name

    def help(self):
        return self._help

    def http(self):
        return self._http_options

    def args(self):
        return list(self._args.values())

    def arg(self, name):
        return self._args.get(name, None)

    def events(self):
        return list(self._events.values())

    def event(self, name):
        return self._events.get(name, None)

    def output(self):
        return self._output
