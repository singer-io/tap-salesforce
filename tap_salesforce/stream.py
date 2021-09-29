import sys
import json
import base64
from datetime import datetime
from typing import TextIO, Dict, Optional

from tap_salesforce.state import State


class _DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        DATETIME_ERR = "Object of type datetime is not JSON serializable"
        DATE_ERR = "Object of type date is not JSON serializable"
        BYTES_ERR = "Object of type bytes is not JSON serializable"
        DECIMAL_ERR = "Object of type Decimal is not JSON serializable"
        try:
            super().default(obj)
        except TypeError as err:
            err_str = str(err)

            if err_str == DATETIME_ERR:
                return obj.isoformat() + "Z"
            elif err_str == DATE_ERR:
                return str(obj)
            elif err_str == BYTES_ERR:
                try:
                    return obj.decode("utf-8")
                except UnicodeDecodeError:
                    pass

                # failing to utf-8 encode,
                # fallback to base64 and nest within
                # base64 object
                return {"base64": base64.b64encode(obj)}
            elif err_str == DECIMAL_ERR:
                return str(obj)
            else:
                raise


class Stream:
    _state: State

    def __init__(self, state: Optional[Dict] = None):
        if state:
            self._state = State(**state)
        else:
            self._state = State()

    def set_stream_state(self, stream_id: str, replication_key: str, value: datetime):
        self._state.set_stream_state(stream_id, replication_key, value)

    def get_stream_state(self, stream_id: str, replication_key) -> Optional[datetime]:
        return self._state.get_stream_state(stream_id, replication_key)

    def write_state(self, file: TextIO = sys.stdout):
        state_message = dict(type="STATE", value=self._state.dict())
        self.write_message(state_message, file=file)

    def write_record(self, record: Dict, stream_id: str, file: TextIO = sys.stdout):
        self.write_message(
            dict(
                type="RECORD",
                stream=stream_id,
                time_extracted=datetime.utcnow().isoformat() + "Z",
                record=record,
            ),
            file=file,
        )

    def write_message(self, message: Dict, file: TextIO):
        line = json.dumps(message, cls=_DatetimeEncoder)
        file.write(line + "\n")
        file.flush()
