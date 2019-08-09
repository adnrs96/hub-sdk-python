from storyhub.sdk.service.Argument import Argument
from storyhub.sdk.service.HttpOptions import HttpOptions
from storyhub.sdk.service.ServiceObject import ServiceObject


class OutputAction(ServiceObject):
    """
    A service output action
    """

    def __init__(self, name, help_, args, http_options, data):
        super().__init__(data=data)

        self._name = name
        self._help_ = help_
        self._args = args
        self._http_options = http_options

    @classmethod
    def from_dict(cls, data):
        name = data["name"]
        output_action = data["output_action"]

        args = {}
        if 'arguments' in output_action:
            for arg_name, arg in output_action['arguments'].items():
                args[arg_name] = Argument.from_dict(data={
                    "name": arg_name,
                    "argument": arg
                })

        http_options = output_action.get(
            'http', None
        )

        if http_options is not None:
            http_options = HttpOptions.from_dict(data={
                "http_options": http_options
            })

        return cls(
            name=name,
            help_=output_action.get(
                'help', 'No help_ available'
            ),
            args=args,
            http_options=http_options,
            data=data
        )

    def name(self):
        return self._name

    def help(self):
        return self._help_

    def args(self):
        return list(self._args.values())

    def arg(self, name):
        return self._args.get(name, None)

    def http(self):
        return self._http_options

    def required_args(self):
        return list(filter(lambda arg: arg.required(), self._args.values()))
