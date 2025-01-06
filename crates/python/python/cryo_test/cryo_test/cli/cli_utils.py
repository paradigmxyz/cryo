from __future__ import annotations

import typing


def open_interactive_session(variables: dict[str, typing.Any]) -> None:
    header = 'data sorted in these variables:'
    for key, value in variables.items():
        header += (
            '\n- \033[1m\033[97m' + key + '\033[0m: ' + type(value).__name__
        )
    try:
        from IPython.terminal.embed import InteractiveShellEmbed

        ipshell = InteractiveShellEmbed(colors='Linux')  # type: ignore
        ipshell(header=header, local_ns=variables)
    except ImportError:
        import code
        import sys

        class ExitInteract:
            def __call__(self) -> None:
                raise SystemExit

            def __repr__(self) -> str:
                raise SystemExit

        try:
            sys.ps1 = '>>> '
            code.interact(
                banner='\n' + header + '\n',
                local=dict(variables, exit=ExitInteract()),
            )
        except SystemExit:
            pass


def _enter_debugger() -> None:
    """open debugger to most recent exception

    - adapted from https://stackoverflow.com/a/242514
    """
    import sys
    import traceback

    # print stacktrace
    extype, value, tb = sys.exc_info()
    print('[ENTERING DEBUGGER]')
    traceback.print_exc()
    print()

    try:
        import ipdb  # type: ignore
        import types

        tb = typing.cast(types.TracebackType, tb)
        ipdb.post_mortem(tb)

    except ImportError:
        import pdb

        pdb.post_mortem(tb)
