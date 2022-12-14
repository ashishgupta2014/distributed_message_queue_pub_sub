class Message:
    """Message"""

    def __init__(self, msg: str):
        self._msg: str = msg

    def get_message(self):
        return self._msg
