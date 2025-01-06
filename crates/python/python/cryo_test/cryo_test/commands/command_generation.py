from __future__ import annotations

import typing

from .. import defaults
from .. import files
from . import command_types

if typing.TYPE_CHECKING:
    from typing_extensions import Unpack


python_command_template = """cryo.freeze(
    {datatype},
    {time_dimension},
    output_dir={output_dir},
    rpc={rpc},
    subdirs='datatype',
    **{extra_args}
)"""


def generate_commands(
    command_arg_list: list[command_types.PartialCommandArgs] | None = None,
    command_arg_combos: dict[str, list[typing.Any]] | None = None,
    common_command_args: command_types.PartialCommandArgs | None = None,
) -> dict[str, str]:
    """generate cryo commands"""
    import itertools

    # parse default inputs
    if command_arg_list is None:
        command_arg_list = [{}]
    if common_command_args is None:
        common_command_args = {}

    missing_keys = {
        key
        for key in defaults.default_combos.keys()
        if key not in common_command_args
        and not all(key in item for item in command_arg_list)
        and (command_arg_combos is not None and key not in command_arg_combos)
    }
    if len(missing_keys) > 0:
        if command_arg_combos is None:
            command_arg_combos = {}
        else:
            command_arg_combos = dict(command_arg_combos)
        for key in missing_keys:
            command_arg_combos[key] = defaults.default_combos[key]

    # render all_command_args
    if command_arg_combos is not None:
        all_command_args: list[command_types.PartialCommandArgs] = []
        for command_arg_set in command_arg_list:
            keys = list(command_arg_combos.keys())
            for values in itertools.product(*command_arg_combos.values()):
                command_args: command_types.PartialCommandArgs = dict(  # type: ignore
                    command_arg_set, **dict(zip(keys, values))
                )
                all_command_args.append(command_args)
    else:
        all_command_args = command_arg_list

    # add common kwargs
    for args in all_command_args:
        for key, value in common_command_args.items():
            if key not in args:
                args[key] = value  # type: ignore

    # eliminate invalid combinations
    all_command_args = [
        command_args
        for command_args in all_command_args
        if are_command_args_valid(**command_args)
    ]

    # generate commands
    commands = {}
    for command_args in all_command_args:
        name = create_command_name(**command_args)
        if name in commands:
            raise Exception('name collision: ' + str(name))
        commands[name] = generate_command(**command_args)

    return commands


def generate_command(**kwargs: Unpack[command_types.CommandArgs]) -> str:
    if isinstance(kwargs.get('datatype'), list):
        kwargs['datatype'] = tuple(kwargs['datatype'])
    interface = kwargs.get('interface', 'cli')
    if interface == 'cli':
        return generate_cli_command(**kwargs)
    elif interface == 'python':
        return generate_python_command(**kwargs)
    else:
        raise Exception('invalid interface')


def generate_cli_command(
    datatype: str | tuple[str, ...],
    time_dimension: str,
    data_root: str,
    executable: str,
    extra_args: list[str] | None = None,
    name: str | None = None,
    interface: typing.Literal['cli', 'python'] = 'cli',
    rpc: str | None = None,
) -> str:
    datatype = _datatype_to_tuple(datatype)
    if interface != 'cli':
        raise Exception('interface must be cli')
    if name is None:
        name = create_command_name(datatype, time_dimension)
    if extra_args is None:
        extra_args = []
    pieces = [
        executable,
        *datatype,
        defaults.time_dimensions[time_dimension],
        '--subdirs datatype',
        '--output-dir '
        + files.get_command_data_dir(
            batch_data_dir=data_root, command_name=name
        ),
        ' '.join(extra_args),
        defaults.datatype_parameters.get(datatype, ''),
    ]
    if rpc is not None:
        pieces.extend(['--rpc', rpc])
    return ' '.join(pieces).strip(' ').replace('  ', ' ')


def generate_python_command(
    datatype: str | tuple[str, ...],
    time_dimension: str,
    data_root: str,
    executable: str,
    extra_args: list[str],
    name: str | None = None,
    interface: typing.Literal['cli', 'python'] = 'python',
    rpc: str | None = None,
) -> str:
    datatype = _datatype_to_tuple(datatype)
    if interface != 'python':
        raise Exception('interface must be python')
    if name is None:
        name = create_command_name(datatype, time_dimension)
    if time_dimension == 'blocks':
        value = defaults.time_dimensions['blocks'].split(' ', maxsplit=1)[1:]
        time_dimension = 'blocks=' + ' '.join(value)
    elif time_dimension == 'transactions':
        value = defaults.time_dimensions['transactions'].split(' ', maxsplit=1)[
            1:
        ]
        time_dimension = 'transactions=' + ' '.join(value)
    else:
        raise Exception('invalid time dimension')
    return python_command_template.format(
        datatype=datatype,
        time_dimension=time_dimension,
        rpc=rpc,
        output_dir=files.get_command_data_dir(
            batch_data_dir=data_root, command_name=name
        ),
        extra_args=extra_args,
    )


def create_command_name(
    datatype: str | tuple[str, ...],
    time_dimension: str,
    name: str | None = None,
    **kwargs: typing.Any,
) -> str:
    datatype = _datatype_to_tuple(datatype)
    if name is not None:
        return name
    else:
        return '_and_'.join(datatype) + '_by_' + time_dimension


def are_command_args_valid(
    datatype: str | tuple[str, ...], time_dimension: str, **kwargs: typing.Any
) -> bool:
    datatype = _datatype_to_tuple(datatype)

    if time_dimension == 'transactions' and any(
        dt in defaults.not_by_transaction for dt in datatype
    ):
        return False
    return True


def _datatype_to_tuple(datatype: str | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(datatype, str):
        datatype = (datatype,)
    return tuple([d for dt in datatype for d in dt.split(' ')])
