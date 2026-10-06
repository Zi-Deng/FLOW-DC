"""Explicit diagnostic storage dispatch; unknown versions never fall back."""

from workflow import WorkflowError


def diagnostic(meta):
    import reporting_diagnostic as v1
    import reporting_diagnostic_v2 as v2
    import reporting_diagnostic_v3 as v3
    import reporting_diagnostic_v4 as v4

    for module in (v1, v2, v3, v4):
        if meta.get("purpose") == module.PURPOSE:
            return module
    raise WorkflowError("Unsupported reporting diagnostic version")


def sequence(meta):
    # Historical storage-only v1 fixtures lacked a diagnostic purpose. Preserve
    # their prompt interpretation; they still cannot establish actual admission.
    if meta.get("purpose") is None:
        import reporting_activation

        return reporting_activation.SEQUENCE
    return diagnostic(meta).activation.SEQUENCE
