from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChangeLogLevelRequest(_message.Message):
    __slots__ = ["level"]
    class Level(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    DEBUG: ChangeLogLevelRequest.Level
    ERROR: ChangeLogLevelRequest.Level
    INFO: ChangeLogLevelRequest.Level
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    TRACE: ChangeLogLevelRequest.Level
    WARN: ChangeLogLevelRequest.Level
    level: ChangeLogLevelRequest.Level
    def __init__(self, level: _Optional[_Union[ChangeLogLevelRequest.Level, str]] = ...) -> None: ...

class ChangeLogLevelResponse(_message.Message):
    __slots__ = ["remark"]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    remark: str
    def __init__(self, remark: _Optional[str] = ...) -> None: ...
