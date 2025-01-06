from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import argparse


def summarize_args(
    args: argparse.Namespace,
    comparison_dir: str,
    batches: dict[str, dict[str, str]],
) -> None:
    print_text_box('cryo test')
    print()
    print_header('parameters')
    print_bullet(key='re-running previous job:', value=args.rerun)
    print_bullet(key='directory', value=comparison_dir)
    print_bullet(key='steps', value=', '.join(args.steps))

    # argument: rpc
    if args.rpc is not None:
        if len(args.rpc) == 1:
            print_bullet(key='rpc', value=args.rpc[0])
        else:
            print_bullet(key='rpc', value=None)  # type: ignore
            for rpc in args.rpc:
                print_bullet(key=rpc, value=None, indent=4)  # type: ignore
    else:
        print_bullet(key='rpc', value='\[default]')  # noqa: W605

    # argument: executable
    if args.executable is not None:
        if len(args.executable) == 1:
            print_bullet(key='executable', value=args.executable[0])
        else:
            print_bullet(key='executable', value=None)  # type: ignore
            for executable in args.executable:
                print_bullet(key=None, value=executable, indent=4)  # type: ignore
    else:
        print_bullet(key='executable', value='\[default]')  # noqa: W605

    # argument: python
    if args.cli_vs_python or args.python:
        if args.cli_vs_python:
            print_bullet(key='interface', value='cli, python')
        else:
            print_bullet(key='interface', value='python')
    else:
        print_bullet(key='interactive', value='cli')

    # argument: interactive
    if args.interactive or args.scan_interactive:
        if args.scan_interactive:
            print_bullet(key='interactive', value="yes, scanning LazyFrame's")
        else:
            print_bullet(key='interactive', value="yes, loading DataFrame's")
    else:
        print_bullet(key='interactive', value='no')

    print()
    print_header('comparing ' + str(len(batches)) + ' cryo contexts')
    for batch_name in batches.keys():
        print_bullet(key=None, value=batch_name)  # type: ignore
    if len(batches) == 0:
        print('[none]')
    print()
    print()


def print_text_box(text: str) -> None:
    import toolstr

    header_kwargs = {'style': 'rgb(0,255,0)', 'text_style': 'white bold'}
    toolstr.print_text_box(text, **header_kwargs)  # type: ignore


def print_header(text: str) -> None:
    import toolstr

    header_kwargs = {'style': 'rgb(0,255,0)', 'text_style': 'white bold'}
    toolstr.print_header(text, **header_kwargs)  # type: ignore


def print_bullet(key: str, value: str, indent: int | None = None) -> None:
    import toolstr

    bullet_kwargs = {
        'colon_style': 'rgb(0,255,0)',
        'key_style': 'white bold',
    }
    toolstr.print_bullet(key=key, value=value, indent=indent, **bullet_kwargs)
