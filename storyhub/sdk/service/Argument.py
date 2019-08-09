from storyhub.sdk.service.ServiceObject import ServiceObject


class Argument(ServiceObject):
    """
    Represents an argument, for an event or other service object.
    """

    def __init__(self, name, help_, type_, required_, data):
        super().__init__(data=data)

        self._name = name
        self._help_ = help_
        self._type = type_
        self._required = required_

    @classmethod
    def from_dict(cls, data):
        name = data["name"]
        argument = data["argument"]

        return cls(
            name=name,
            help_=argument.get(
                'help', '.not.available'
            ),
            type_=argument['type'],
            required_=argument.get('required', False),
            data=data
        )

    def name(self):
        return self._name

    def help(self):
        return self._help_

    def type(self):
        return self._type

    def required(self):
        return self._required
