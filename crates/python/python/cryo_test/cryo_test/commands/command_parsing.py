from __future__ import annotations

import typing

from .. import defaults


def get_commands_interface(commands: dict[str, str]) -> str:
    interfaces = set(
        get_command_interface(command) for command in commands.values()
    )
    if len(interfaces) == 1:
        return list(interfaces)[0]
    else:
        raise Exception()


def get_command_interface(command: str) -> typing.Literal['cli', 'python']:
    if command.startswith('cryo.'):
        return 'python'
    else:
        return 'cli'


def get_command_output_dir(command: str) -> str:
    interface = get_command_interface(command)
    if interface == 'cli':
        pre_index = command.index('--output-dir ')
        start_index = pre_index + len('--output-dir')
        end_index = command.index(' \\', pre_index)
        return command[start_index:end_index]
    elif interface == 'python':
        pre_index = command.index('output_dir=')
        start_index = pre_index + len('output_dir=')
        end_index = command.index(',', pre_index)
        return command[start_index:end_index]
    else:
        raise Exception('invalid interface')


def parse_command_datatypes(command: str) -> list[str]:
    interface = get_command_interface(command)
    if interface == 'cli':
        datatypes = []
        pieces = command.split(' ')
        for piece in pieces[1:]:
            if piece.startswith('-'):
                break
            else:
                datatypes.append(piece)
        return [
            dt
            for datatype in datatypes
            for dt in defaults.multi_aliases.get(datatype, [datatype])
        ]
    else:
        raise Exception()
