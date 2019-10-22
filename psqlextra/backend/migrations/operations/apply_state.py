from django.db.migrations.operations.base import Operation


class ApplyState(Operation):
    """Takes an abritrary operation and migrates the project state but does not
    apply the operation to the database.

    This is very similar to the :see:RunSQL `state_operations`
    parameter. This is useful if you want to tell Django that an
    operation was applied without actually applying it.
    """

    reduces_to_sql = False

    def __init__(self, state_operation: Operation) -> None:
        self.state_operation = state_operation

    def deconstruct(self):
        kwargs = {"state_operation": self.state_operation}
        return (self.__class__.__qualname__, [], kwargs)

    @property
    def reversible(self):
        return True

    def state_forwards(self, app_label, state):
        self.state_operation.state_forwards(app_label, state)

    def state_backwards(self, app_label, state):
        self.state_operation.state_backwards(app_label, state)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        pass

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        pass

    def describe(self):
        return "Apply state: " + self.state_operation.describe()
