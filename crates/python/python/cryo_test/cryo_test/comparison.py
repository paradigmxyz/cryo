from __future__ import annotations

import typing

from . import cli
from . import commands
from . import files
from . import scripts
from . import polars_utils

if typing.TYPE_CHECKING:
    from typing import Literal, Sequence

    CompareStep = Literal['setup', 'collect', 'compare']


def perform_comparison(
    batches: dict[str, dict[str, str]],
    comparison_dir: str | None,
    steps: Sequence[CompareStep] = ['setup', 'collect', 'compare'],
) -> dict[str, typing.Any]:
    import json
    import subprocess
    import time

    # initialize variables
    timing: dict[str, float | None] = {
        't_' + side + '_' + step: None
        for side in ['start', 'end']
        for step in ['setup', 'collect', 'compare']
    }
    if comparison_dir is None and batches is None:
        raise Exception('must specify batches or existing comparison dir')
    if comparison_dir is None:
        comparison_dir = files.create_comparison_dir()
    elif batches is None:
        batches_path = files.get_batches_json_path(comparison_dir)
        with open(batches_path) as f:
            batches = json.load(f)
    script_paths = {}
    for batch_name, batch_commands in batches.items():
        script_paths[batch_name] = files.get_scripts_path(
            comparison_dir=comparison_dir,
            batch_name=batch_name,
            batch_commands=batch_commands,
        )

    # step: setup collection scripts
    if steps is None or 'setup' in steps:
        timing['t_start_setup'] = time.time()
        cli.print_text_box('\[step: setup]')  # noqa: W605
        print('building scripts in ' + files.get_scripts_dir(comparison_dir))
        for batch_name, batch_commands in batches.items():
            scripts.generate_collect_script(
                commands=batch_commands, script_path=script_paths[batch_name]
            )
        timing['t_end_setup'] = time.time()

    # step: collect output data
    if steps is None or 'collect' in steps:
        timing['t_start_collect'] = time.time()
        print('\n\n')
        for batch_name, batch_commands in batches.items():
            cli.print_text_box(
                '\[step: collect] collecting data in batch [rgb(50,220,50) bold]'
                + batch_name
                + '[/rgb(50,220,50) bold]'
            )  # noqa: W605
            subprocess.call(script_paths[batch_name])
        timing['t_end_collect'] = time.time()

    # step: compare outputs
    if steps is None or 'compare' in steps:
        timing['t_start_compare'] = time.time()
        cli.print_text_box('\[step: compare] comparing data')  # noqa: W605
        _compare_outputs(comparison_dir=comparison_dir, batches=batches)
        timing['t_end_compare'] = time.time()

    # save output
    output = {
        'comparison_dir': comparison_dir,
        'script_paths': script_paths,
        'timing': timing,
        'steps': steps,
    }
    _save_comparison_output(output)

    print()
    print('...complete')
    print()
    print('(see data in ' + comparison_dir + ')')

    return output


def _compare_outputs(
    comparison_dir: str, batches: dict[str, dict[str, str]]
) -> dict[str, dict[str, list[str]]]:
    import rich

    # gather command names
    command_names = []  # use list not set to preserve order
    for batch in batches.values():
        for command_name in batch.keys():
            if command_name not in command_names:
                command_names.append(command_name)
    if len(batches) == 1:
        context_str = 'context'
    else:
        context_str = 'contexts'
    print(len(command_names), 'commands across', len(batches), context_str)
    print()
    cli.print_header('Data summary')

    # for each command, compare outputs
    all_differences: dict[str, dict[str, list[str]]] = {}
    for command_name in command_names:
        # get datatypes
        datatypes = set()
        for batch_name, batch in batches.items():
            if command_name in batch:
                command = batches[batch_name][command_name]
                datatypes.update(commands.parse_command_datatypes(command))

        # compare results
        all_differences[command_name] = {}
        for datatype in datatypes:
            s = (
                '[rgb(0,255,0)]-[/rgb(0,255,0)] verifying [rgb(0,255,0) bold]'
                + command_name
                + '[/rgb(0,255,0) bold]'
            )
            if len(datatypes) > 1:
                s = s + ' (datatype = ' + datatype + ')'
            rich.print(s)

            all_differences[command_name][datatype] = _compare_specific(
                comparison_dir=comparison_dir,
                command_name=command_name,
                datatype=datatype,
                batches=batches,
            )

    total_differences = sum(
        len(datatype_diffs)
        for command_diffs in all_differences.values()
        for datatype_diffs in command_diffs.values()
    )
    print('')
    cli.print_header('Total differences: ' + str(total_differences))
    if total_differences == 0:
        print('[none]')
    else:
        i = 1
        for command_name in all_differences.keys():
            for datatype in all_differences[command_name].keys():
                for diff in all_differences[command_name][datatype]:
                    if diff.startswith('(omitting'):
                        continue
                    rich.print(
                        str(i) + '.',
                        '\[[rgb(0,255,0) bold]'
                        + command_name
                        + '[/rgb(0,255,0) bold]]',
                        '\[[white bold]' + datatype + '[/white bold]]',
                        diff,
                    )
                    i += 1

    return all_differences


def _compare_specific(
    comparison_dir: str,
    command_name: str,
    datatype: str,
    batches: dict[str, dict[str, str]],
) -> list[str]:
    import glob
    import rich

    diffs = []
    all_paths = {}
    for batch_name, batch in batches.items():
        # get paths
        if command_name not in batch:
            continue
        batch_glob = files.get_command_data_glob(
            comparison_dir=comparison_dir,
            batch_name=batch_name,
            command_name=command_name,
            datatype=datatype,
        )
        paths = glob.glob(batch_glob)
        _print_batch_data_summary(batch_name=batch_name, paths=paths)
        if len(paths) == 0:
            diffs.append('data missing for ' + batch_name)
        else:
            all_paths[batch_name] = paths

    # compute differences
    diffs += polars_utils.find_df_differences(all_paths)
    diffs = [diff[:300] for diff in diffs if not diff.startswith('(omitting')]
    if len(diffs) > 0:
        for diff in diffs:
            rich.print('    [red bold]diff[/red bold]: ' + diff)
    else:
        rich.print('    [green bold]equal[/green bold]')

    return diffs


def _print_batch_data_summary(batch_name: str, paths: list[str]) -> None:
    import os
    import toolstr
    import polars as pl
    import rich

    if len(paths) == 1:
        file_str = 'file'
    else:
        file_str = 'files'

    if len(paths) == 0:
        summary = '[red bold]DATA MISSING[/red bold])'
    else:
        n_bytes = sum(os.path.getsize(path) for path in paths)
        n_rows = pl.scan_parquet(paths).select(pl.len()).collect()['len'][0]
        summary = (
            toolstr.format(n_bytes, 'nbytes')
            + ', '
            + toolstr.format(n_rows)
            + ' rows)[/white]'
        )

    rich.print(
        '    batch [rgb(0,255,0)]=[/rgb(0,255,0)] [white bold]'
        + batch_name
        + '[/white bold] '
        + '[white]('
        + str(len(paths))
        + ' '
        + file_str
        + ', '
        + summary
    )


def _save_comparison_output(output: dict[str, typing.Any]) -> None:
    import json
    import os

    comparison_dir = output['comparison_dir']
    run_path = files.get_summary_path(comparison_dir=comparison_dir)
    os.makedirs(os.path.dirname(run_path), exist_ok=True)
    with open(run_path, 'w') as f:
        json.dump(output, f)
