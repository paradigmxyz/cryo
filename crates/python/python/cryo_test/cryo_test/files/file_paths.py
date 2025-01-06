from __future__ import annotations

from .. import commands as commands_module


def create_comparison_dir(
    comparison_dir: str | None = None, label: str | None = None
) -> str:
    import datetime
    import os

    if comparison_dir is not None:
        if label is not None:
            pieces = comparison_dir.split('__', maxsplit=1)
            if len(pieces) != 2 or pieces[-1] != '__' + label:
                raise Exception('comparison_dir does not match label')
        return comparison_dir
    else:
        dir_name = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        if label is not None:
            dir_name = dir_name + '__' + label
        return os.path.join(get_cryo_test_root(), dir_name)


def get_most_recent_comparison_dir() -> str:
    import os

    test_root = get_cryo_test_root()
    jobs = sorted(os.listdir(test_root))
    if len(jobs) == 0:
        raise Exception('no previous jobs')
    else:
        return os.path.join(test_root, jobs[-1])


def get_cryo_test_root() -> str:
    import os
    import tempfile

    tmp_root = '/tmp' if os.path.exists('/tmp') else tempfile.gettempdir()
    return os.path.join(tmp_root, 'cryo_test')


def get_commands_path(comparison_dir: str) -> str:
    import os

    return os.path.join(comparison_dir, 'commands.json')


def get_summary_path(comparison_dir: str) -> str:
    import datetime
    import os

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return os.path.join(comparison_dir, 'runs', timestamp + '.json')


def get_batch_data_dir(comparison_dir: str, batch_name: str) -> str:
    import os

    return os.path.join(comparison_dir, 'data', batch_name)


def get_command_data_dir(
    *,
    batch_data_dir: str | None = None,
    comparison_dir: str | None = None,
    batch_name: str | None = None,
    command_name: str,
) -> str:
    import os

    if batch_data_dir is None:
        if comparison_dir is None or batch_name is None:
            raise Exception('specify more arguments')
        batch_data_dir = get_batch_data_dir(
            comparison_dir=comparison_dir, batch_name=batch_name
        )

    return os.path.join(batch_data_dir, command_name)


def get_command_collected_datatypes(
    *,
    batch_data_dir: str | None = None,
    comparison_dir: str | None = None,
    batch_name: str | None = None,
    command_name: str,
) -> list[str]:
    import os

    data_dir = get_command_data_dir(
        batch_data_dir=batch_data_dir,
        comparison_dir=comparison_dir,
        batch_name=batch_name,
        command_name=command_name,
    )
    return [
        item
        for item in os.listdir(data_dir)
        if item != '.cryo'
        and (len(os.listdir(os.path.join(data_dir, item))) > 0)
    ]


def get_command_data_glob(
    *,
    batch_data_dir: str | None = None,
    comparison_dir: str | None = None,
    batch_name: str | None = None,
    command_name: str,
    datatype: str,
) -> str:
    import os

    command_dir = get_command_data_dir(
        batch_data_dir=batch_data_dir,
        comparison_dir=comparison_dir,
        batch_name=batch_name,
        command_name=command_name,
    )
    return os.path.join(command_dir, datatype, '*.parquet')


def get_scripts_dir(comparison_dir: str) -> str:
    import os

    return os.path.join(comparison_dir, 'scripts')


def get_scripts_path(
    comparison_dir: str, batch_name: str, batch_commands: dict[str, str]
) -> str:
    import os

    interface = commands_module.get_commands_interface(batch_commands)
    if interface == 'cli':
        extension = '.sh'
    elif interface == 'python':
        extension = '.py'
    else:
        raise Exception()

    filename = 'collect_' + batch_name + extension

    return os.path.join(get_scripts_dir(comparison_dir), filename)
