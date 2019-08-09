from storyhub.sdk.service.Argument import Argument
from storyhub.sdk.service.ServiceObject import ServiceObject


class Command(ServiceObject):
    """
    A service command that accepts arguments.
    """

    def __init__(self, name, help_, args, data):
        super().__init__(data=data)

        self._name = name
        self._help_ = help_
        self._args = args

    @classmethod
    def from_dict(cls, data):
        name = data["name"]
        command = data["command"]
        args = {}
        if 'arguments' in command:
            for arg_name, arg in command['arguments'].items():
                args[arg_name] = Argument.from_dict(data={
                    "name": arg_name,
                    "argument": arg
                })

        help_ = command.get(
            'help', 'No help_ available'
        )
        return cls(
            name=name,
            help_=help_,
            args=args,
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

    def required_args(self):
        return list(filter(lambda arg: arg.required(), self._args.values()))
